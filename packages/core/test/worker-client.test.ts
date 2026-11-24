import { describe, expect } from "bun:test"
import { randomBytes } from "node:crypto"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Queue, Result, Schema, Scope, Sink, Stream } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Environment, EnvironmentDriver, makeMemoryDriver } from "@opencode/core/environment/index"
import { KV } from "@opencode/core/kv"
import { Frames } from "@opencode/core/machine/frames"
import { Identity } from "@opencode/core/machine/identity"
import { Link } from "@opencode/core/machine/link"
import { FromWorker, ToWorker } from "@opencode/core/machine/wire"
import { createStdioFrameDecoder, encodeStdioFrame, WorkerClient } from "@opencode/core/machine/worker-client"
import { Workspace } from "@opencode/core/workspace"
import { makeWorker } from "./fixture/machine-worker"
import { it } from "./lib/effect"

const encodeFromWorker = Schema.encodeEffect(Schema.fromJsonString(FromWorker))
const decodeToWorker = Schema.decodeUnknownEffect(Schema.fromJsonString(ToWorker))
const utf8Encode = new TextEncoder()
const utf8Decode = new TextDecoder("utf-8", { fatal: true })

/** In-memory bidirectional binary carrier pair for testing Noise XX links. */
const makeCarrierPair = Effect.gen(function* () {
  const aToB = yield* Queue.unbounded<Uint8Array, Cause.Done>()
  const bToA = yield* Queue.unbounded<Uint8Array, Cause.Done>()
  const close = () => {
    Queue.endUnsafe(aToB)
    Queue.endUnsafe(bToA)
  }
  const sideA: Link.Carrier = {
    send: (frame) => Effect.sync(() => void Queue.offerUnsafe(aToB, new Uint8Array(frame))),
    receive: Queue.take(bToA).pipe(Effect.mapError(() => new Link.Closed())),
  }
  const sideB: Link.Carrier = {
    send: (frame) => Effect.sync(() => void Queue.offerUnsafe(bToA, new Uint8Array(frame))),
    receive: Queue.take(aToB).pipe(Effect.mapError(() => new Link.Closed())),
  }
  return { sideA, sideB, close }
})

describe("EnvironmentDriver.lazy", () => {
  it.effect(
    "acquires per operation, releases each scope, maps errors, and never lets a remote driver claim local",
    () =>
      Effect.gen(function* () {
        const events: string[] = []
        const memory = makeMemoryDriver()
        const memoryFiles = Environment.makeFiles(memory)

        const driver = EnvironmentDriver.lazy(
          Effect.acquireRelease(
            Effect.sync(() => {
              events.push("acquire")
              return { files: memoryFiles, spawner: memory.spawner }
            }),
            () => Effect.sync(() => void events.push("release")),
          ),
          {
            module: "TestDriver",
            spawnDescription: "failed to connect",
            // Simulate a runtime cast attempting to spoof `local: true` from a remote driver.
            options: {
              home: Effect.succeed("/home/sandbox"),
              shell: "sh",
              local: true,
            } as unknown as Environment.Driver["options"],
          },
        )
        const env = Environment.workspace(Workspace.ID.make("wrk_lazy"), driver)
        expect(env.local).toBe(false)
        expect(env.shell).toBe("sh")
        expect(yield* env.home!).toBe("/home/sandbox")

        yield* env.files.write("/hello.txt", utf8Encode.encode("world"))
        expect(utf8Decode.decode((yield* env.files.read("/hello.txt")).bytes)).toBe("world")
        expect(events).toEqual(["acquire", "release", "acquire", "release"])

        const failing = EnvironmentDriver.lazy(Effect.fail(new Error("offline")), {
          module: "TestDriver",
          spawnDescription: "failed to connect",
        })
        const fileErr = yield* Environment.makeFiles(failing).stat("/x").pipe(Effect.flip)
        expect(fileErr._tag).toBe("Environment.Failed")
        const spawnErr = yield* failing.spawner.spawn(ChildProcess.make("echo", ["hi"])).pipe(Effect.flip)
        expect(spawnErr._tag).toBe("PlatformError")
        expect(spawnErr.message).toContain("failed to connect")
      }),
  )
})

describe("Identity.kvLayer & fromSeedLayer", () => {
  it.live(
    "generates a home key once in KV, persists across rebuilds, rejects corrupted base64url, and copies seeds eagerly",
    () =>
      Effect.gen(function* () {
        const mutableSeed = new Uint8Array(randomBytes(32))
        const expectedPub = Identity.fromSeed(mutableSeed).publicKey
        const seedLayer = Identity.fromSeedLayer(mutableSeed)
        mutableSeed.fill(0)
        expect(yield* Effect.flatMap(Identity.Service, (id) => id.publicKey()).pipe(Effect.provide(seedLayer))).toEqual(
          expectedPub,
        )

        const store = new Map<string, KV.Value>()
        const fakeKv = Layer.succeed(
          KV.Service,
          KV.Service.of({
            get: (key) => Effect.sync(() => store.get(key)),
            set: (key, value) => Effect.sync(() => void store.set(key, value)),
            remove: (key) => Effect.sync(() => void store.delete(key)),
            scan: () => Effect.succeed({ entries: [] }),
          }),
        )

        const firstKeys = yield* Effect.gen(function* () {
          const identity = yield* Identity.Service
          return yield* Effect.all([identity.publicKey(), identity.publicKey(), identity.seed()], {
            concurrency: "unbounded",
          })
        }).pipe(Effect.provide(Identity.kvLayer.pipe(Layer.provide(fakeKv))))
        expect(firstKeys[0]).toEqual(firstKeys[1])
        expect(firstKeys[2].length).toBe(32)
        expect(store.size).toBe(1)

        const restored = yield* Effect.gen(function* () {
          const identity = yield* Identity.Service
          const sig = yield* identity.sign(utf8Encode.encode("hello"))
          return { pub: yield* identity.publicKey(), sig }
        }).pipe(Effect.provide(Identity.kvLayer.pipe(Layer.provide(fakeKv))))
        expect(restored.pub).toEqual(firstKeys[0])
        expect(Identity.verify(restored.pub, utf8Encode.encode("hello"), restored.sig)).toBe(true)

        const validEncoded = String(store.get("machine.home-key"))
        for (const corrupted of ["not-32-bytes", `${validEncoded}!!`, `${validEncoded}=`]) {
          store.set("machine.home-key", corrupted)
          const bad = yield* Effect.flatMap(Identity.Service, (id) => id.publicKey()).pipe(
            Effect.provide(Identity.kvLayer.pipe(Layer.provide(fakeKv))),
            Effect.flip,
          )
          expect(bad._tag).toBe("Identity.InvalidKey")
        }
      }),
  )

  it.live("works end to end through Identity.kvNode over Database", () =>
    Effect.gen(function* () {
      const layer = AppNodeBuilder.build(Identity.kvNode, [
        Database.node.replace(Database.configured({ path: ":memory:" })),
      ])
      const pub = yield* Effect.flatMap(Identity.Service, (id) => id.publicKey()).pipe(Effect.provide(layer))
      expect(pub.length).toBe(32)
    }),
  )
})

describe("WorkerClient.dial", () => {
  it.live(
    "authenticates a pull-mode worker by pinned peerKey, runs files & commands, refuses bad handshakes, and stays interruptible",
    () =>
      Effect.gen(function* () {
        const homeSeed = new Uint8Array(randomBytes(32))
        const homeKey = Identity.fromSeed(homeSeed)
        const workerKey = Identity.fromSeed(new Uint8Array(randomBytes(32)))
        const id = Environment.ID.make("exedev-box")

        const { sideA: workerCarrier, sideB: brainCarrier, close } = yield* makeCarrierPair
        yield* Effect.addFinalizer(() => Effect.sync(close))

        const worker = yield* makeWorker({
          connect: (connection) =>
            Effect.scoped(
              Effect.gen(function* () {
                const linked = yield* Link.connect(workerCarrier, { home: homeKey.publicKey, identity: workerKey })
                linked.send(connection.hello)
                yield* Effect.raceFirst(
                  Stream.runForEach(linked.messages, (message) => Effect.sync(() => connection.send(message))),
                  Stream.runForEach(connection.messages, (message) => Effect.sync(() => linked.send(message))),
                )
              }),
            ),
        })
        yield* worker.restore

        const driverScope = yield* Scope.fork(yield* Effect.scope)
        const driver = yield* WorkerClient.dial(id, brainCarrier, { peerKey: workerKey.publicKey }).pipe(
          Effect.provide(Identity.fromSeedLayer(homeSeed)),
          Scope.provide(driverScope),
        )
        const env = Environment.make(id, driver)

        expect(yield* env.home!).toBe("/home/worker")
        expect(env.shell).toBe("sh")
        expect(env.local).toBe(false)
        yield* env.files.write("/project/app.ts", utf8Encode.encode("export const x = 1"))
        expect(utf8Decode.decode((yield* env.files.read("/project/app.ts")).bytes)).toBe("export const x = 1")

        const proc = yield* env.spawner.spawn(ChildProcess.make("/bin/echo", ["from pull"], { cwd: "/project" }))
        const run = worker.runs[0]
        expect([run.argv, run.cwd]).toEqual([["/bin/echo", "from pull"], "/project"])
        yield* run.print("from pull\n")
        yield* run.exit(0)
        expect(yield* Stream.mkString(Stream.decodeText(proc.all))).toBe("from pull\n")
        expect(yield* proc.exitCode).toBe(ChildProcessSpawner.ExitCode(0))

        yield* Scope.close(driverScope, Exit.void)
        const afterClose = yield* env.files.stat("/project/app.ts").pipe(Effect.flip)
        expect(afterClose._tag).toBe("Environment.Failed")

        const shortKeyErr = yield* WorkerClient.dial(id, brainCarrier, { peerKey: new Uint8Array(16) }).pipe(
          Effect.provide(Identity.fromSeedLayer(homeSeed)),
          Effect.flip,
        )
        expect(shortKeyErr).toEqual(new Link.Refused())

        const badPair = yield* makeCarrierPair
        const wrongKey = Identity.fromSeed(new Uint8Array(randomBytes(32)))
        const peerFiber = yield* Effect.forkChild(
          Link.dial(badPair.sideA, { home: homeKey.publicKey, identity: workerKey }).pipe(Effect.ignore),
          { startImmediately: true },
        )
        const refused = yield* WorkerClient.dial(id, badPair.sideB, { peerKey: wrongKey.publicKey }).pipe(
          Effect.provide(Identity.fromSeedLayer(homeSeed)),
          Effect.flip,
        )
        expect(refused).toEqual(new Link.Refused())
        badPair.close()
        yield* Effect.ignore(Fiber.await(peerFiber))

        const secretPair = yield* makeCarrierPair
        const secretPeer = yield* Effect.forkChild(
          Link.dial(secretPair.sideA, {
            home: homeKey.publicKey,
            identity: workerKey,
            secret: new Uint8Array(randomBytes(32)),
          }).pipe(Effect.ignore),
          { startImmediately: true },
        )
        const secretRefused = yield* WorkerClient.dial(id, secretPair.sideB, { peerKey: workerKey.publicKey }).pipe(
          Effect.provide(Identity.fromSeedLayer(homeSeed)),
          Effect.flip,
        )
        expect(secretRefused).toEqual(new Link.Refused())
        secretPair.close()
        yield* Effect.ignore(Fiber.await(secretPeer))

        const silentPair = yield* makeCarrierPair
        const waiting = yield* Effect.forkChild(
          Link.serveDirect(silentPair.sideB, { machine: id, peerKey: workerKey.publicKey }).pipe(
            Effect.provide(Identity.fromSeedLayer(homeSeed)),
          ),
          { startImmediately: true },
        )
        yield* Fiber.interrupt(waiting)
        const interrupted = yield* Fiber.await(waiting)
        expect(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause)).toBe(true)
        silentPair.close()
      }),
  )
})

describe("WorkerClient.stdio", () => {
  it.effect("decodes multiple frames in one chunk, split headers, and rejects zero or oversized frames", () =>
    Effect.sync(() => {
      const f1 = Result.getOrThrow(encodeStdioFrame(utf8Encode.encode("alpha")))
      const f2 = Result.getOrThrow(encodeStdioFrame(utf8Encode.encode("beta")))
      const combined = new Uint8Array(f1.byteLength + f2.byteLength)
      combined.set(f1, 0)
      combined.set(f2, f1.byteLength)

      const dec1 = createStdioFrameDecoder()
      expect(Result.getOrThrow(dec1.push(combined)).map((bytes) => utf8Decode.decode(bytes))).toEqual(["alpha", "beta"])
      expect(dec1.hasPartial()).toBe(false)

      const dec2 = createStdioFrameDecoder()
      const collected: string[] = []
      for (let i = 0; i < combined.byteLength; i++) {
        for (const frame of Result.getOrThrow(dec2.push(combined.subarray(i, i + 1)))) {
          collected.push(utf8Decode.decode(frame))
        }
      }
      expect(collected).toEqual(["alpha", "beta"])
      expect(dec2.hasPartial()).toBe(false)

      expect(Result.isFailure(encodeStdioFrame(new Uint8Array(0)))).toBe(true)
      const tooBig = new Uint8Array(4)
      new DataView(tooBig.buffer).setUint32(0, Frames.MAX_MESSAGE + 1, false)
      expect(Result.isFailure(createStdioFrameDecoder().push(tooBig))).toBe(true)
    }),
  )

  it.live("speaks 4-byte BE length-prefixed wire.ts frames across split chunks and rejects malformed frames", () =>
    Effect.gen(function* () {
      const id = Environment.ID.make("ssh-box")
      const toClient = yield* Queue.unbounded<Uint8Array, Cause.Done>()
      const fromClient = yield* Queue.unbounded<Uint8Array, Cause.Done>()
      const ready = yield* Deferred.make<void>()

      const worker = yield* makeWorker({
        connect: (connection) =>
          Effect.gen(function* () {
            const sendFramed = (message: FromWorker) =>
              Effect.flatMap(encodeFromWorker(message), (json) =>
                Effect.sync(() => {
                  const frame = Result.getOrThrow(encodeStdioFrame(utf8Encode.encode(json)))
                  Queue.offerUnsafe(toClient, frame.subarray(0, 2))
                  Queue.offerUnsafe(toClient, frame.subarray(2))
                }),
              )

            yield* sendFramed(connection.hello)
            yield* Deferred.succeed(ready, undefined)

            const peerDecoder = createStdioFrameDecoder()
            yield* Effect.raceFirst(
              Stream.runForEach(connection.messages, sendFramed),
              Stream.runForEach(Stream.fromQueue(fromClient), (chunk) =>
                Effect.gen(function* () {
                  for (const payload of yield* Effect.fromResult(peerDecoder.push(chunk))) {
                    connection.send(yield* decodeToWorker(utf8Decode.decode(payload)))
                  }
                }),
              ),
            )
          }),
      })
      yield* worker.restore
      yield* Deferred.await(ready)

      const driver = yield* WorkerClient.stdio(id, {
        stdin: Sink.forEach((chunk) => Effect.sync(() => void Queue.offerUnsafe(fromClient, chunk))),
        stdout: Stream.fromQueue(toClient),
      })
      const env = Environment.make(id, driver)

      expect(yield* env.home!).toBe("/home/worker")
      yield* env.files.write("/pipe.txt", utf8Encode.encode("over stdio"))
      expect(utf8Decode.decode((yield* env.files.read("/pipe.txt")).bytes)).toBe("over stdio")

      const proc = yield* env.spawner.spawn(ChildProcess.make("/bin/echo", ["pipe"], { cwd: "/" }))
      const run = worker.runs[0]
      yield* run.print("pipe\n")
      yield* run.exit(0)
      expect(yield* Stream.mkString(Stream.decodeText(proc.all))).toBe("pipe\n")
      expect(yield* proc.exitCode).toBe(ChildProcessSpawner.ExitCode(0))

      const bad = yield* WorkerClient.stdio(id, {
        stdin: Sink.drain,
        stdout: Stream.succeed(new Uint8Array([0, 0, 0, 0])),
      }).pipe(Effect.flip)
      expect(bad).toEqual(new Link.Refused())
    }),
  )
})
