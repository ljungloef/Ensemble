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

namespace Ensemble.Samples

open System
open System.Threading

open Ensemble
open Ensemble.Actors
open Ensemble.Topics

module MessageScheduling =

  type Msg =
    | Start of int
    | FrameUpdate

  module App =

    type State = { CurrentFrame: int; Max: int }

    let rec idle state =
      function
      | Start numberOfRounds ->
        printfn "Received start signal.."

        become running
        <&> set
              { state with
                  CurrentFrame = 0
                  Max = numberOfRounds }
        <&> postLater (DeliveryInstruction.Every(TimeSpan.FromSeconds(2), FrameUpdate))
      | _ -> success ()

    and running state =
      function
      | FrameUpdate ->
        let frame = state.CurrentFrame + 1

        printfn $"Processing frame %i{frame}"

        if frame >= state.Max then
          stop ()
        else
          set { state with CurrentFrame = frame }
      | _ -> success ()

    let create () = Actor.create idle

  let run (ct: CancellationToken) =
    task {
      printfn "Starting message scheduling sample.."

      use system = ActorSystem.withDefaults ()

      let actor = App.create ()

      use groupInstance =
        groupWith (Routers.topic ())
        |= add actor (Sub.topic ">") (State.init { CurrentFrame = 0; Max = 0 })
        |> build
        |> run system ct

      groupInstance <! Start 10

      let! result = groupInstance.Completion

      match result with
      | Ok finalState -> printfn $"Result is %A{finalState}"
      | Error e -> printfn $"Error when running sample: %A{e}"

      return ()
    }
