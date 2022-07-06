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

module ProducerConsumer =

  type Msg =
    | Generate of int
    | Increment of int
    | Decrement of int
    | GetResultQuery of Request<int * int>

  module Producer =

    let handler _ =
      function
      | Generate amount ->
        printfn "Generating %i commands" amount

        let rng = new Random()
        let inline inc () = rng.NextDouble() > 0.5

        postMany (seq { for i in 1..amount -> i |> if inc () then Increment else Decrement })

      | _ -> success ()

    let create () = Actor.create handler

  module Consumer =

    type State = { Total: int; NumberOfOps: int }

    let inline apply op amount state =
      printfn "a"
      set
        { state with
            Total = op state.Total amount
            NumberOfOps = state.NumberOfOps + 1 }

    let handler state =
      function
      | Increment amount -> state |> apply (+) amount
      | Decrement amount -> state |> apply (-) amount

      | GetResultQuery reply ->
        reply
        |> Request.respond (state.NumberOfOps, state.Total)
        |> ignore

        success ()

      | _ -> success ()

    let create () = Actor.create handler

  open type Consumer.State

  let run (ct: CancellationToken) =
    task {
      printfn "Starting producer consumer sample.."

      use system = ActorSystem.withDefaults ()
      let producer = Producer.create ()
      let consumer = Consumer.create ()

      use groupInstance =
        groupWith (Routers.topic ())
        |= add consumer (Sub.topic ">") (State.init { Total = 0; NumberOfOps = 0 })
        |. add producer (Sub.topic ">") State.none
        |> build
        |> run system ct

      groupInstance <! (Generate 1_000)
      groupInstance.Complete()

      let! result = groupInstance.Completion

      match result with
      | Ok finalState -> printfn $"Result is %A{finalState}"
      | Error e -> printfn $"Error when running sample: %A{e}"

      return ()
    }
