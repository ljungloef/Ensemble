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


/// Message that also carries route info
[<Struct>]
type RouteMessage<'Route, 'Msg> = { Route: 'Route; Message: 'Msg }

/// The `IRouteMatcher` is used to tell whether a given message is accepted or not.
type IRouteMatcher<'Route, 'Msg> =

  /// Matches againts the incoming message and returns `true` if is a match.
  abstract member MatchAgainst: RouteMessage<'Route, 'Msg> -> bool

type IMessageRouter<'Route, 'Msg> =

  /// Generate a route from the name route.
  abstract member GenerateRouteFromName: 'Route -> 'Route

  /// Generate a route for the given type.
  abstract member GenerateRouteFromType: Type -> 'Route

  /// Generate a route for the given msg.
  abstract member GenerateRoute: 'Msg -> 'Route

[<RequireQualifiedAccess>]
module Routers =

  /// Create a new message router that always will generate the singleton value `v` as the route.
  let inline singleton v =
    { new IMessageRouter<_, _> with
        override __.GenerateRouteFromName(_) = v
        override __.GenerateRouteFromType(_) = v
        override __.GenerateRoute(_) = v }

[<RequireQualifiedAccess>]
module Sub =

  /// Create a router matcher that will accept all messages.
  let all =
    fun _ ->
      { new IRouteMatcher<_, _> with
          override __.MatchAgainst(_) = true }
