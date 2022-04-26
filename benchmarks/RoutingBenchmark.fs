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

namespace Ensemble.Benchmarks.Routing

open BenchmarkDotNet.Attributes
open Ensemble
open Ensemble.Topics

type RouteMsg =
  | Message1 of int64
  | Message2 of string


[<SimpleJob; MemoryDiagnoser>]
type RoutingBenchmark() =

  let mutable counter = 0

  let logResult ctx result =
    if counter = 0 then
      counter <- counter + 1
      printfn $"{ctx}='%i{result}'"


  [<Params(10, 1_000, 100_000)>]
  member val public count = 0 with get, set

  [<Benchmark>]
  member __.RunWrkVerified() = TopicMapper.mapObj (Message1 12)
