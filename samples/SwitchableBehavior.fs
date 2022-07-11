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
open System.Threading.Tasks

open Ensemble
open Ensemble.Actors
open Ensemble.Topics

module SwitchableBehavior =

  type Msg =
    | Connect
    | Ping
    | Pong
    | Disconnect
    | LastWill of string

  type AppState =
    { ConnectTime: DateTimeOffset option
      LastPingSent: DateTimeOffset option
      LastPingTime: DateTimeOffset option
      DisconnectTime: DateTimeOffset option }
    static member Default() =
      { ConnectTime = None
        LastPingSent = None
        LastPingTime = None
        DisconnectTime = None }

  let rec disconnected state =
    function
    | Connect ->

      printfn "Received connect message"

      // Switch to another handler
      become connected

      // Update the with some additional info
      <&> set { state with ConnectTime = Some DateTimeOffset.UtcNow }

    | _ -> success ()

  and connected state =
    function
    | Ping ->

      printfn "Received ping message; responding with Pong"

      post Pong
      <&> set { state with LastPingSent = Some DateTimeOffset.UtcNow }

    | Pong -> set { state with LastPingTime = Some DateTimeOffset.UtcNow }

    | Disconnect ->

      printfn "Received disconnect message"

      become disconnected
      <&> post (LastWill "bye")
      <&> set { state with DisconnectTime = Some DateTimeOffset.UtcNow }

    | _ -> success ()


  let run (ct: CancellationToken) =
    task {
      printfn "Starting switchable behavior sample.."

      let app = Actor.create disconnected

      use system = ActorSystem.withDefaults ()

      use groupInstance =
        groupWith (Routers.topic ())
        |= add app (Sub.topic ">") AppState.Default
        |> build
        |> run system ct

      // Simulate connect
      groupInstance <! Connect

      do! Task.Delay(1_000)

      groupInstance <! Ping

      do! Task.Delay(1_000)

      groupInstance <! Disconnect

      let! result = groupInstance.Completion

      match result with
      | Ok finalState -> printfn $"Result is %A{finalState}"
      | Error e -> printfn $"Error when running sample: %A{e}"

      return ()
    }
