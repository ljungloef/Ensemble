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

module SwitchableBehavior =

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
