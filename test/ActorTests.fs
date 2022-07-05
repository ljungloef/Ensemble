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

namespace Ensemble.Tests

open Xunit
open FsUnit.Xunit
open Ensemble
open Ensemble.Actors
open Ensemble.Tests.TestDoubles
open FsUnit.CustomMatchers
open System
open System.Threading

module ActorTests =

  type TestMsg = TestMsg of string

  let inline ct () =
    (new CancellationTokenSource(TimeSpan.FromSeconds(10)))
      .Token

  module CoreTests =

    type State = { Messages: string list }

    [<Fact>]
    let ``should complete when mailbox is marked completed`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        do! mailbox <!! TestMsg "A"
        do! mailbox <!! TestMsg "B"
        do! mailbox <!! TestMsg "C"

        mailbox.Complete(None)

        let! result =
          Actor.create (fun state (TestMsg msg) -> set { state with Messages = msg :: state.Messages })
          |> Actor.run system { Messages = [] } mailbox mailbox (ct ())

        result.State.Messages
        |> should matchList [ "A"; "B"; "C" ]
      }

    [<Fact>]
    let ``should stop and return throwned exception`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        do! mailbox <!! TestMsg "A"

        let! result =
          Actor.create (fun _ (TestMsg msg) -> raise (Exception(msg)))
          |> Actor.run system { Messages = [] } mailbox mailbox (ct ())

        result.StopReason |> should be (ofCase <@ Exn @>)
      }

    [<Fact>]
    let ``should stop when actor report back stop command`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create
        do! mailbox <!! TestMsg "A"

        let! result =
          Actor.create (fun _ (TestMsg _) -> stop ())
          |> Actor.run system { Messages = [] } mailbox mailbox (ct ())

        result.StopReason
        |> should be (ofCase <@ Stopped @>)
      }

    [<Fact>]
    let ``should stop and discard state changes when actor report back abort command`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        do! mailbox <!! TestMsg "A"
        do! mailbox <!! TestMsg "B"

        let! result =
          Actor.create (fun state (TestMsg msg) ->
            if state.Messages.Length = 0 then
              set { state with Messages = msg :: state.Messages }
            else
              abort ())
          |> Actor.run system { Messages = [] } mailbox mailbox (ct ())

        result.StopReason
        |> should be (ofCase <@ Aborted @>)

        result.State.Messages |> should matchList [ "A" ]
      }

    [<Fact>]
    let ``should stop and return cancelled reason when cancelled from outside`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let cts = new CancellationTokenSource()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        let task =
          Actor.create (fun _ _ -> success ())
          |> Actor.run system { Messages = [] } mailbox mailbox cts.Token

        cts.Cancel()

        let! result = task

        result.StopReason
        |> should be (ofCase <@ Cancelled @>)
      }

    [<Fact>]
    let ``should stop and return cancelled reason when cancelled exn thrown from handler`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        do! mailbox <!! TestMsg "A"

        let! result =
          Actor.create (fun _ _ ->
            let inner = new CancellationTokenSource()
            inner.Cancel()
            inner.Token.ThrowIfCancellationRequested()
            success ())
          |> Actor.run system { Messages = [] } mailbox mailbox (ct ())

        result.StopReason
        |> should be (ofCase <@ Cancelled @>)
      }

  module ScheduleTests =

    [<Fact>]
    let ``should post to outbox on expiry`` () =
      task {
        let msg = TestMsg "Delayed"
        let system = Schedulers.noDelayScheduler () |> ActorSystem.create
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        let actor =
          Actor.create (fun _ m ->
            match m with
            // Since we use the same channel as inbox and outbox, the message will be read again by the actor when posted to the outbox on expiry
            | TestMsg "Delayed" -> set 1 <&> stop ()
            | TestMsg "Trigger" -> postLater (DeliveryInstruction.Once(msg, TimeSpan.Zero))
            | TestMsg _ -> abort ())
          |> Actor.run system 0 mailbox mailbox (ct ())

        do! mailbox <!! TestMsg "Trigger"

        let! result = actor

        result.StopReason |> should equal Stopped
        result.State |> should equal 1
      }

  module BehaviorTests =

    type BehaviorTestState =
      { BehaviorA: int
        BehaviorB: int
        Messages: string list }

    let rec behaviorA state (TestMsg msg) =
      become behaviorB
      <&> set
            { state with
                BehaviorA = state.BehaviorA + 1
                Messages = msg :: state.Messages }

    and behaviorB state (TestMsg msg) =
      become behaviorA
      <&> set
            { state with
                BehaviorB = state.BehaviorB + 1
                Messages = msg :: state.Messages }

    [<Fact>]
    let ``should switch behaviour if actor returns become`` () =
      task {
        let system = ActorSystem.withDefaults ()
        let mailbox = Channel.unbounded () |> ChannelMailbox.create

        let actor =
          Actor.create behaviorA
          |> Actor.run
               system
               { BehaviorA = 0
                 BehaviorB = 0
                 Messages = [] }
               mailbox
               mailbox
               (ct ())

        do! mailbox <!! TestMsg "A"
        do! mailbox <!! TestMsg "B"
        do! mailbox <!! TestMsg "C"

        mailbox.Complete(None)

        let! result = actor

        let finalState = result.State

        finalState.BehaviorA |> should equal 2
        finalState.BehaviorB |> should equal 1

        finalState.Messages
        |> should matchList [ "A"; "B"; "C" ]
      }

  module HandlerTests =

    let init = 10
    let defaultHandler state msg = set (state + msg)

    let context () =
      ActorMessageContext(init, defaultHandler)

    let build ctx f = f ctx
    let runHandler msg (ctx: ActorMessageContext<_, _>) = ctx |> (ctx.Handler ctx.State msg)

    [<Fact>]
    let ``become should set the handler in the context`` () =
      let mutable wasCalledDuringRun = false

      let newHandler _ _ =
        wasCalledDuringRun <- true
        success ()

      let ctx = context ()

      let buildResult = become newHandler |> build ctx
      let runResult = ctx |> runHandler 0

      buildResult |> should equal true
      runResult |> should equal true
      wasCalledDuringRun |> should equal true

      ctx.Status
      |> int
      |> should equal (int ActorMessageStatus.BehaviorChange)

    [<Fact>]
    let ``post should set outputs in context`` () =
      let ctx = context ()
      let msg = 22

      let buildResult = post msg |> build ctx

      buildResult |> should equal true
      ctx.Output |> should matchList [ msg ]

      ctx.Status
      |> int
      |> should equal (int ActorMessageStatus.HasOutputs)

    [<Fact>]
    let ``schedulePost should set scheduled outputs in context`` () =
      let ctx = context ()
      let msg = 33
      let delay = TimeSpan.FromSeconds(3)

      let buildResult = postLater (DeliveryInstruction.Once(msg, delay)) |> build ctx

      buildResult |> should equal true
      ctx.ScheduledOutput.Length |> should equal 1

      let scheduled = ctx.ScheduledOutput.Head

      scheduled.Interval |> should equal delay
      scheduled.IsRecurring |> should equal false
      scheduled.Message |> should equal msg

      ctx.Status
      |> int
      |> should equal (int ActorMessageStatus.HasScheduledOutputs)

    [<Fact>]
    let ``set should set state in context`` () =
      let ctx = context ()
      let newState = 101

      let buildResult = set newState |> build ctx

      buildResult |> should equal true
      ctx.State |> should equal newState

      ctx.Status
      |> int
      |> should equal (int ActorMessageStatus.StateChange)

    [<Fact>]
    let ``abort should set abort in context`` () =
      let ctx = context ()

      let buildResult = abort () |> build ctx

      buildResult |> should equal true
      ctx.Stop |> should equal (ValueSome true)

      ctx.Status
      |> int
      |> should equal (int ActorMessageStatus.MarkedStopped)

    [<Fact>]
    let ``stop should set stop in context`` () =
      let ctx = context ()

      let buildResult = stop () |> build ctx

      buildResult |> should equal true
      ctx.Stop |> should equal (ValueSome false)

      ctx.Status
      |> int
      |> should equal (int ActorMessageStatus.MarkedStopped)

    [<Fact>]
    let ``combine should combine all parts`` () =
      let newHandler _ _ = success ()

      let ctx = context ()

      let buildResult =
        set 101 <&> post 14 <&> become newHandler
        |> build ctx

      buildResult |> should equal true
      ctx.State |> should equal 101
      ctx.Output |> should matchList [ 14 ]

    [<Fact>]
    let ``reset should reset deltas`` () =
      let newHandler _ _ = success ()

      let ctx = context ()

      let buildResult =
        set 101 <&> post 14 <&> become newHandler
        |> build ctx

      buildResult |> should equal true
      ctx.State |> should equal 101
      ctx.Output |> should matchList [ 14 ]

      ctx.Status
      |> should
           equal
           (ActorMessageStatus.StateChange
            ||| ActorMessageStatus.HasOutputs
            ||| ActorMessageStatus.BehaviorChange)

      ctx.Reset()

      ctx.State |> should equal 101
      ctx.Output |> should haveLength 0

      ctx.Status
      |> should equal ActorMessageStatus.NoChanges
