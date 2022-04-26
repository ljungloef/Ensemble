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

namespace Ensemble.Benchmarks.Topics

open BenchmarkDotNet.Attributes
open Ensemble
open Ensemble.Actors
open Ensemble.Topics
open System.Threading
open System.Threading.Tasks

module Group =

  let inline createActor (barrier: Barrier) compl =
    Actor.create (fun sum i ->
      let newSum = sum + i

      if newSum = compl then
        barrier.SignalAndWait()

      set newSum)

  let inline createGroup (barrier: Barrier) size compl =
    let options =
      GroupOpts<string, int64>(Routers.topic (), GroupMailboxSize = size, DefaultActorMailboxSize = size)

    let group = defineGroup options

    let actors =
      Seq.init barrier.ParticipantCount (fun _ -> createActor barrier compl)
      |> Seq.toList

    let builder =
      match actors with
      | [] -> failwithf "ParticipantCount cannot be 0"
      | [ actor ] -> group |. add actor (Sub.topic ">") (State.init 0L)
      | head :: tail ->
        let init = group |. add head (Sub.topic ">") (State.init 0L)

        tail
        |> Seq.fold (fun group actor -> (|.) group (add actor (Sub.topic ">") (State.init 0L))) init

    builder |> build


[<SimpleJob; MemoryDiagnoser>]
type TopicRoutingBenchmark() =

  let mutable tcs = Unchecked.defaultof<TaskCompletionSource<obj>>
  let mutable barrier = Unchecked.defaultof<Barrier>

  let mutable group =
    Unchecked.defaultof<ActorGroup<Result<int64, string>, string, int64>>

  let mutable dataset = Unchecked.defaultof<int * int64>

  let mutable counter = 0

  let logResult ctx result =
    if counter = 0 then
      counter <- counter + 1
      printfn $"{ctx}='%i{result}'"

  [<Params(100_000)>]
  member val public MessageCount = 0 with get, set

  [<Params(3)>]
  member val public ParticipantCount = 0 with get, set

  [<GlobalSetup>]
  member self.GlobalSetup() =
    let expectedSum =
      Seq.init self.MessageCount int64
      |> Seq.fold (fun sum i -> sum + i) 0L

    dataset <- (self.MessageCount, expectedSum)

  [<IterationSetup>]
  member self.IterationSetup() =
    tcs <- TaskCompletionSource<obj>()
    barrier <- new Barrier(self.ParticipantCount, (fun _ -> tcs.TrySetResult(obj ()) |> ignore))

    group <-
      dataset
      |> snd
      |> Group.createGroup barrier self.MessageCount

  [<IterationCleanup>]
  member __.IterationCleanup() = barrier.Dispose()

  [<Benchmark>]
  member self.RunWrkVerified() =
    task {
      use group = group |> run CancellationToken.None

      for i in 1 .. self.MessageCount do
        group <<! ("all", i)

      let! _ = tcs.Task

      return ()
    }
