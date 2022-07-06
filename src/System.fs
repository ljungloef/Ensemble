(*

   Copyright 2022 The Ensemble Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*)

namespace Ensemble

open System
open System.Threading

/// The actor system acts as a plane with shared resources and
/// functionality across groups.
type IActorSystem =
  inherit IDisposable

  /// The message scheduler used by the system
  abstract member Scheduler: IMessageScheduler

module ActorSystem =

  open LazyExtensions

  type ActorSystemImpl(scheduler: Lazy<IMessageScheduler>) =

    let mutable disposed = 0

    let dispose (disposing: bool) =
      if Interlocked.Exchange(&disposed, 1) = 0 then
        if disposing then
          if scheduler.IsValueCreated then
            scheduler.Value.Dispose()

    let throwIfDisposed () =
      if disposed > 0 then
        raise (ObjectDisposedException(nameof (ActorSystemImpl)))

    interface IActorSystem with

      override __.Scheduler =
        throwIfDisposed ()
        scheduler.Value

      override self.Dispose() =
        dispose true
        GC.SuppressFinalize(self)

    override __.Finalize() = dispose false

  let inline create scheduler =
    let scheduler = Lazy<_>.Create (scheduler)
    new ActorSystemImpl(scheduler) :> IActorSystem

  let inline withDefaults () =
    MessageScheduler.withDefaults () |> create
