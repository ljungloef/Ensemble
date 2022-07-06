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

module Sample1 =

  type Msg =
    | Generate of int
    | Increment of int
    | Decrement of int
    | GetResultQuery of Request<int * int>

  module Producer =

    let handler _ =
      function
      | Generate amount ->

        let rng = new Random()
        let inline inc () = rng.NextDouble() > 0.5

        postMany (seq { for i in 1..amount -> i |> if inc () then Increment else Decrement })

      | _ -> success ()

    let create () = Actor.create handler

  module Consumer =

    type State = { Total: int; NumberOfOps: int }

    let inline apply op amount state =
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
      use system = ActorSystem.withDefaults ()
      let producer = Producer.create ()
      let consumer = Consumer.create ()

      use groupInstance =
        groupWith (Routers.topic ())
        |= add producer (Sub.topic ">") State.none
        |= add consumer (Sub.topic ">") (State.init { Total = 0; NumberOfOps = 0 })
        |> build
        |> run system ct

      groupInstance <! (Generate 1_000)
      groupInstance.Complete()

      return! groupInstance.Completion
    }

module Sample2 =

  [<Struct>]
  type Msg =
    | Connect
    | Ping
    | Pong
    | Disconnect
    | LastWill of string

  module Domain =

    type State =
      { ConnectTime: DateTimeOffset option
        LastPingSent: DateTimeOffset option
        LastPingTime: DateTimeOffset option
        DisconnectTime: DateTimeOffset option }

    let rec disconnected state =
      function
      | Connect ->
        become connected
        <&> set { state with ConnectTime = Some DateTimeOffset.UtcNow }
      | _ -> success ()

    and connected state =
      function
      | Ping ->
        post Pong
        <&> set { state with LastPingSent = Some DateTimeOffset.UtcNow }
      | Pong -> set { state with LastPingTime = Some DateTimeOffset.UtcNow }
      | Disconnect ->
        become disconnected
        <&> post (LastWill "bye")
        <&> set state
      | _ -> success ()


    let create () =
      let state () =
        { ConnectTime = None
          LastPingSent = None
          LastPingTime = None
          DisconnectTime = None }

      Actor.create disconnected
