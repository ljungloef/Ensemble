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

namespace Ensemble.Benchmarks

open BenchmarkDotNet.Running

open Ensemble.Benchmarks.Actors
open Ensemble.Benchmarks.Topics

module Program =

  let defaultSwitch () =
    BenchmarkSwitcher [| typeof<ActorBenchmark>
                         typeof<TopicRoutingBenchmark> |]

  [<EntryPoint>]
  let main args =
    defaultSwitch().Run(args) |> ignore
    0
