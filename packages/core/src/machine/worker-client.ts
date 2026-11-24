export * as WorkerClient from "./worker-client.js"

import { Cause, Deferred, Effect, Queue, Result, Sink, Stream } from "effect"
import type { Environment } from "../environment/index.js"
import { Frames } from "./frames.js"
import { Machine } from "./index.js"
import { Link } from "./link.js"
import type { FromWorker, Hello, ToWorker } from "./wire.js"

const HELLO_DEADLINE = "10 seconds"
const HEADER_BYTES = 4

/** Encodes a single UTF-8 JSON payload with a 4-byte big-endian uint32 length prefix for `opencode-worker stdio`. */
export const encodeStdioFrame = (payload: Uint8Array): Result.Result<Uint8Array, Link.Refused> => {
  if (payload.byteLength === 0 || payload.byteLength > Frames.MAX_MESSAGE) return Result.fail(new Link.Refused())
  const frame = new Uint8Array(HEADER_BYTES + payload.byteLength)
  new DataView(frame.buffer, frame.byteOffset, HEADER_BYTES).setUint32(0, payload.byteLength, false)
  frame.set(payload, HEADER_BYTES)
  return Result.succeed(frame)
}

/**
 * O(N) incremental parser for 4-byte big-endian uint32 length-prefixed `opencode-worker stdio` frames.
 * Reads the header into a fixed 4-byte buffer, allocates each payload once, and slices chunks via `subarray`.
 */
export const createStdioFrameDecoder = () => {
  const header = new Uint8Array(HEADER_BYTES)
  const headerView = new DataView(header.buffer)
  let headerOffset = 0
  let payload: Uint8Array | undefined
  let payloadOffset = 0

  return {
    push: (chunk: Uint8Array): Result.Result<ReadonlyArray<Uint8Array>, Link.Refused> => {
      const frames: Uint8Array[] = []
      let offset = 0
      while (offset < chunk.byteLength) {
        if (!payload) {
          const take = Math.min(HEADER_BYTES - headerOffset, chunk.byteLength - offset)
          header.set(chunk.subarray(offset, offset + take), headerOffset)
          headerOffset += take
          offset += take
          if (headerOffset < HEADER_BYTES) break
          const length = headerView.getUint32(0, false)
          if (length === 0 || length > Frames.MAX_MESSAGE) return Result.fail(new Link.Refused())
          payload = new Uint8Array(length)
          payloadOffset = 0
          headerOffset = 0
          continue
        }
        const take = Math.min(payload.byteLength - payloadOffset, chunk.byteLength - offset)
        payload.set(chunk.subarray(offset, offset + take), payloadOffset)
        payloadOffset += take
        offset += take
        if (payloadOffset === payload.byteLength) {
          frames.push(payload)
          payload = undefined
          payloadOffset = 0
        }
      }
      return Result.succeed(frames)
    },
    hasPartial: () => headerOffset > 0 || payload !== undefined,
  }
}

/**
 * Adapts a live `Machine.Connection` into a scoped `Environment.Driver`, reusing the worker wire protocol's
 * 8 symlink-safe file operations and streaming process spawner for any pull-mode or pipe-mode target.
 *
 * Pass an existing `machine` to keep running processes tracked across reconnects (closing the scope ends only
 * this connection); omit it for a single-connection driver whose scope owns and closes the machine.
 */
export const fromConnection = Effect.fn("WorkerClient.fromConnection")(function* (
  id: Environment.ID,
  connection: Machine.Connection,
  existing?: Machine.Interface,
) {
  const machine = existing ?? Machine.make(id)
  if (!existing) {
    yield* Effect.addFinalizer(() => Effect.sync(() => machine.close(new Machine.NotSent({ machine: id }))))
  }
  yield* Effect.forkScoped(machine.connect(connection), { startImmediately: true })
  return {
    spawner: machine.environment.spawner,
    overrides: machine.environment.files,
    options: {
      path: machine.environment.path,
      shell: machine.environment.shell,
      home: machine.environment.home,
    },
  } satisfies Environment.Driver
})

/**
 * Pull mode: attaches to a listening `opencode-worker serve` (or a relay-bridged worker) over any binary
 * `Link.Carrier`, authenticates via Noise XX against `peerKey`, and returns a scoped `Environment.Driver`.
 */
export const dial = Effect.fn("WorkerClient.dial")(function* (
  id: Environment.ID,
  carrier: Link.Carrier,
  options: { readonly peerKey: Uint8Array; readonly fixed?: Link.Fixed; readonly machine?: Machine.Interface },
) {
  const { connection } = yield* Link.serveDirect(carrier, { machine: id, peerKey: options.peerKey }, options.fixed)
  return yield* fromConnection(id, connection, options.machine)
})

export interface StdioPipe {
  readonly stdin: Sink.Sink<void, Uint8Array, unknown>
  readonly stdout: Stream.Stream<Uint8Array, unknown>
}

/**
 * Pipe mode: speaks 4-byte big-endian length-prefixed `wire.ts` JSON frames over an already-authenticated
 * duplex byte stream (such as `ssh host opencode-worker stdio` or container `exec`) and returns a scoped `Environment.Driver`.
 */
export const stdio = Effect.fn("WorkerClient.stdio")(function* (
  id: Environment.ID,
  io: StdioPipe,
  existing?: Machine.Interface,
) {
  // Bounded to 1 frame so stdout backpressures against the reader instead of buffering unbounded worker output.
  const inbound = yield* Queue.bounded<Uint8Array, Cause.Done | Link.Refused>(1)
  const ended = yield* Deferred.make<void>()
  const decoder = createStdioFrameDecoder()

  yield* Effect.forkScoped(
    Effect.gen(function* () {
      yield* Stream.runForEach(io.stdout, (chunk) =>
        Effect.gen(function* () {
          const frames = yield* Effect.fromResult(decoder.push(chunk))
          for (const frame of frames) yield* Queue.offer(inbound, frame)
        }),
      )
      if (decoder.hasPartial()) return yield* new Link.Refused()
      Queue.endUnsafe(inbound)
    }).pipe(
      Effect.catchCause(() =>
        Effect.sync(() => {
          Queue.failCauseUnsafe(inbound, Cause.fail(new Link.Refused()))
        }),
      ),
    ),
    { startImmediately: true },
  )

  const hello = yield* Queue.take(inbound).pipe(
    Effect.mapError(() => new Link.Refused()),
    Effect.flatMap((bytes) => Link.decodeHello(id, bytes)),
    Effect.timeout(HELLO_DEADLINE),
    Effect.catchCause(() => Effect.fail(new Link.Refused())),
  )

  const outbox = yield* Queue.unbounded<ToWorker, Cause.Done>()
  const end = Effect.andThen(Deferred.succeed(ended, undefined), Queue.end(outbox))
  yield* Effect.addFinalizer(() => end)

  const outbound = Stream.fromQueue(outbox).pipe(
    Stream.mapEffect((message) =>
      Link.encodeToWorkerMessage(message).pipe(
        Effect.flatMap((bytes) => Effect.fromResult(encodeStdioFrame(bytes))),
        Effect.mapError(() => new Link.Refused()),
      ),
    ),
  )
  yield* Effect.forkScoped(Stream.run(outbound, io.stdin).pipe(Effect.ignore, Effect.ensuring(end)), {
    startImmediately: true,
  })

  const messages: Stream.Stream<Exclude<FromWorker, Hello>> = Stream.fromEffectRepeat(
    Effect.suspend(() =>
      Deferred.isDoneUnsafe(ended)
        ? Effect.fail("Ended" as const)
        : Queue.take(inbound).pipe(
            Effect.flatMap(Link.decodeFromWorkerMessage),
            Effect.mapError(() => "Ended" as const),
          ),
    ).pipe(Effect.onError(() => end)),
  ).pipe(Stream.interruptWhen(Deferred.await(ended)), Stream.ignore)

  return yield* fromConnection(
    id,
    {
      hello,
      send: (message) => {
        Queue.offerUnsafe(outbox, message)
      },
      messages,
    },
    existing,
  )
})
