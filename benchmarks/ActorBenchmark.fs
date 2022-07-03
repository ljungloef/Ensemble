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

namespace Ensemble.Benchmarks.Actors

open BenchmarkDotNet.Attributes
open System.Threading.Channels
open System.Threading
open Ensemble
open Ensemble.Actors

#nowarn "25"

type Msg =
  | Increment of int64
  | GetResult of AsyncReplyChannel<int64>
  | GetResultProm of Request<int64>

module ChannelActor =

  let readUntilEnd (reader: ChannelReader<_>) f (token: CancellationToken) =
    task {
      let mutable item = Unchecked.defaultof<_>
      let mutable cont = true
      let mutable state = Unchecked.defaultof<_>

      while not token.IsCancellationRequested && cont do

        while reader.TryRead(&item) do
          state <- f state item

        let! canReadMore = reader.WaitToReadAsync(token)
        cont <- canReadMore

      return state
    }

  let run n f token =
    let channel =
      Channel.CreateUnbounded(UnboundedChannelOptions(SingleWriter = true, SingleReader = true))

    let actorTask = readUntilEnd channel.Reader f token

    for i in 0L .. n do
      let couldWrite = channel.Writer.TryWrite(Increment i)
      assert (couldWrite)

    channel.Writer.Complete()
    actorTask

module MailboxProcessorActor =

  let createAgent f (token: CancellationToken) =

    MailboxProcessor.Start(
      (fun inbox ->
        async {
          let! token = Async.CancellationToken
          let mutable state = Unchecked.defaultof<_>

          while not token.IsCancellationRequested do
            let! msg = inbox.Receive()

            match msg with
            | Increment i -> state <- f state i
            | GetResult rc -> rc.Reply(state)
        }),
      token
    )

  let increment = fun state i -> state + i

  let run n f token =
    use agent = createAgent f token

    for i in 0L .. n do
      agent.Post(Increment i)

    agent.PostAndReply GetResult

module WrkActor =

  let handler f state =
    function
    | Increment i -> set (f state i)
    | GetResultProm prom ->
      prom |> Request.respond state |> ignore
      success ()
    | _ -> success ()

  let run n f token =
    let system = ActorSystem.withDefaults ()
    let mailbox =
      UnboundedChannelOptions(SingleWriter = true, SingleReader = true)
      |> ChannelMailbox.fromOpts

    let actorDef = Actor.create (handler f)
    let task = actorDef |> Actor.run system 0L mailbox mailbox token

    for i in 0L .. n do
      mailbox <! Increment i

    mailbox.Complete(None)
    task


[<SimpleJob; MemoryDiagnoser>]
type ActorBenchmark() =

  let incrementInt = fun state i -> state + i
  let increment = fun state (Increment i) -> state + i

  let mutable counter = 0

  let logResult ctx result =
    if counter = 0 then
      counter <- counter + 1
      printfn $"{ctx}='%i{result}'"


  [<Params(10, 1_000, 100_000)>]
  member val public count = 0 with get, set

  // [<Benchmark>]
  member self.RunChannelsVerified() =
    task {
      let! result = ChannelActor.run self.count increment CancellationToken.None
      logResult "channels" result
      return ()
    }

  // [<Benchmark>]
  member self.RunAgentVerified() =
    let result =
      MailboxProcessorActor.run self.count incrementInt CancellationToken.None

    logResult "agent" result
    ()

  [<Benchmark>]
  member self.RunWrkVerified() =
    task {
      let! result = WrkActor.run self.count incrementInt CancellationToken.None
      logResult "wrk" result.State
      return ()
    }
