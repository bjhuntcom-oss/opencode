import { Effect, PlatformError, Scope } from "effect"
import { ChildProcessSpawner } from "effect/process"
import type { Options } from "./environment.js"
import { Failed, type Files, type FilesImpl } from "./files.js"

export type DriverOptions = Omit<Options, "local">

export interface Driver {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"]
  readonly overrides?: Partial<FilesImpl>
  readonly options?: DriverOptions
}

export interface LazyOptions {
  readonly module: string
  readonly spawnDescription: string
  readonly options?: DriverOptions
}

/**
 * Turns a scoped acquire effect into a per-operation lazy `Driver`: every spawn and file call acquires
 * a live target only for the duration of that operation and releases its scope as soon as it completes.
 */
export const lazy = (
  acquire: Effect.Effect<
    { readonly files: Files; readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"] },
    unknown,
    Scope.Scope
  >,
  config: LazyOptions,
): Driver => {
  const spawner = ChildProcessSpawner.make((command) =>
    acquire.pipe(
      Effect.mapError((cause) =>
        PlatformError.systemError({
          _tag: "Unknown",
          module: config.module,
          method: "spawn",
          description: config.spawnDescription,
          cause,
        }),
      ),
      Effect.flatMap((target) => target.spawner.spawn(command)),
    ),
  )
  const withFiles = <A, E>(path: string, use: (files: Files) => Effect.Effect<A, E>): Effect.Effect<A, E | Failed> =>
    Effect.scoped(
      acquire.pipe(
        Effect.mapError((cause) => new Failed({ path, cause })),
        Effect.flatMap((target) => use(target.files)),
      ),
    )
  const overrides: FilesImpl = {
    read: (path, range) => withFiles(path, (files) => files.read(path, range)),
    write: (path, bytes) => withFiles(path, (files) => files.write(path, bytes)),
    stat: (path) => withFiles(path, (files) => files.stat(path)),
    list: (path) => withFiles(path, (files) => files.list(path)),
    remove: (path) => withFiles(path, (files) => files.remove(path)),
    move: (from, to) => withFiles(from, (files) => files.move(from, to)),
    mkdir: (path) => withFiles(path, (files) => files.mkdir(path)),
    append: (path, bytes) => withFiles(path, (files) => files.append(path, bytes)),
  }
  return { spawner, overrides, options: config.options }
}

export * as EnvironmentDriver from "./driver.js"
