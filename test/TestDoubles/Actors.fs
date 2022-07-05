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

namespace Ensemble.Tests.TestDoubles

open Ensemble
open Ensemble.Actors
open System
open System.Threading

module Actors =

  type TestState =
    { Messages: string list }
    static member Default() = { Messages = [] }

  type TestMsg =
    | PostMsg of string
    | PostNewMsg of TestMsg
    | SchedulePostNewMsg of TestMsg * TimeSpan
    | RaiseExn of exn
    | Mock of (TestState -> (ActorMessageContext<TestState, TestMsg> -> bool))
    | GetState of Request<TestState>

  module Mocks =

    let inline stopAfterSignal (waitHandle: ManualResetEventSlim) =
      Mock (fun _ ->
        waitHandle.Wait()
        stop ())

    let inline throwAfterSignal (waitHandle: ManualResetEventSlim) (ex : exn) =
      Mock (fun _ ->
        waitHandle.Wait()
        raise ex)

    let inline setThenWait (set: ManualResetEventSlim) (wait: ManualResetEventSlim) f =
      Mock (fun _ ->
        set.Set()
        wait.Wait()
        f ())

  module TestActor =

    let handler state =
      function
      | PostMsg msg -> set { state with Messages = msg :: state.Messages }
      | PostNewMsg msg -> post msg
      | SchedulePostNewMsg (msg, delay) -> postLater (DeliveryInstruction.OnceAfter(delay, msg))
      | RaiseExn e -> raise e
      | Mock f -> f state
      | GetState reply ->
        reply |> Request.respond state |> ignore
        success ()

    let create () = Actor.create handler
