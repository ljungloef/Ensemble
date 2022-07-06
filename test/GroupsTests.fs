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
open Ensemble.Topics
open Ensemble.Tests.TestDoubles.Actors
open Ensemble.Tests.TestDoubles.Groups
open Ensemble.Tests.TestDoubles.Schedulers
open FsUnit.CustomMatchers
open System
open System.Threading
open System.Threading.Tasks

module GroupsTests =

  [<Literal>]
  let RestartOnFailureAndKeepOnlyMailbox =
    SupervisionDecision.RestartOnFailure
    ||| SupervisionDecision.KeepMailboxIntact

  [<Literal>]
  let RestartOnFailureAndKeepStateAndMailbox =
    SupervisionDecision.RestartOnFailure
    ||| SupervisionDecision.KeepMailboxIntact
    ||| SupervisionDecision.KeepState

  [<Literal>]
  let RestartUnlessStoppedAndKeepOnlyMailbox =
    SupervisionDecision.RestartUnlessStopped
    ||| SupervisionDecision.KeepMailboxIntact

  [<Literal>]
  let RestartUnlessStoppedAndKeepStateAndMailbox =
    SupervisionDecision.RestartUnlessStopped
    ||| SupervisionDecision.KeepMailboxIntact
    ||| SupervisionDecision.KeepState

  [<Literal>]
  let AlwaysRestartAndKeepOnlyMailbox =
    SupervisionDecision.AlwaysRestart
    ||| SupervisionDecision.KeepMailboxIntact

  [<Literal>]
  let AlwaysRestartAndKeepStateAndMailbox =
    SupervisionDecision.AlwaysRestart
    ||| SupervisionDecision.KeepMailboxIntact
    ||| SupervisionDecision.KeepState

  let inline ct () =
    (new CancellationTokenSource(TimeSpan.FromSeconds(10)))
      .Token

  let inline iterEvents f (events: IEvent<ActorGroupEvent<_, _>>) = events |> Event.add f
  let inline logEvents events = iterEvents (printfn "EVENT=%A") events

  let inline waitForEvent f ct (events: IEvent<ActorGroupEvent<_, _>>) =
    task {
      let tcs = TaskCompletionSource<_>()

      events
      |> Event.add (fun e ->
        if f e then
          tcs.TrySetResult(e) |> ignore)

      let! completed = Task.WhenAny(tcs.Task, Task.Delay(-1, ct))

      if completed = tcs.Task then
        return! tcs.Task
      else
        return raise (Exception(""))
    }

  let inline waitForDeadletters expectedCount ct (events: IEvent<ActorGroupEvent<_, _>>) =
    task {
      let deadLetters = ResizeArray()

      let! _ =
        events
        |> waitForEvent
             (fun e ->
               match e with
               | GroupEvent (DeadLetters (m, _)) ->
                 m
                 |> Seq.choose (fun m ->
                   match m with
                   | { Message = (PostMsg v) } -> Some v
                   | _ -> None)
                 |> deadLetters.AddRange
               | GroupEvent (DeadLetter ({ Message = (PostMsg m) }, _)) -> deadLetters.Add(m)
               | _ -> ()

               deadLetters.Count >= expectedCount)
             ct

      return deadLetters |> Seq.toList
    }

  [<Theory>]
  [<InlineData(AlwaysRestartAndKeepOnlyMailbox, "B;C")>]
  [<InlineData(AlwaysRestartAndKeepStateAndMailbox, "A;B;C")>]
  let ``should restart stopped actor for stop-positive decisions`` (strat: SupervisionDecision) (expected: string) =
    task {
      let expected = expected.Split(';') |> Array.toList

      use system = ActorSystem.withDefaults ()
      let options = GroupOpts(Routers.publishToAll (), Supervision.singleton strat)
      let actor = TestActor.create ()

      use groupInstance =
        defineGroup options
        |= add actor Sub.all TestState.Default
        |> build
        |> run system (ct ())

      use waitHandle = new ManualResetEventSlim(false)

      groupInstance <! PostMsg "A"
      groupInstance <! Mocks.stopAfterSignal waitHandle
      groupInstance <! PostMsg "B"
      groupInstance <! PostMsg "C"

      waitHandle.Set()

      let! result = GetState -!> groupInstance
      result.Messages |> should matchList expected
    }

  [<Theory>]
  [<InlineData(RestartUnlessStoppedAndKeepOnlyMailbox)>]
  [<InlineData(RestartUnlessStoppedAndKeepStateAndMailbox)>]
  let ``should not restart actor for stop-negative decisions`` (strat: SupervisionDecision) =
    task {
      let ct = ct ()
      use system = ActorSystem.withDefaults ()
      let options = GroupOpts(Routers.publishToAll (), Supervision.singleton strat)
      let actor = TestActor.create ()

      use groupInstance =
        defineGroup options
        |= add actor Sub.all TestState.Default
        |> build
        |> run system ct

      let waitForDeadletters = groupInstance.Events |> waitForDeadletters 2 ct

      use waitHandle = new ManualResetEventSlim(false)

      groupInstance <! PostMsg "A"
      groupInstance <! Mocks.stopAfterSignal waitHandle
      groupInstance <! PostMsg "B"
      groupInstance <! PostMsg "C"

      waitHandle.Set()

      let! deadLetters = waitForDeadletters
      deadLetters |> should matchList [ "B"; "C" ]
    }


  [<Theory>]
  [<InlineData(RestartOnFailureAndKeepOnlyMailbox, "B;C")>]
  [<InlineData(RestartOnFailureAndKeepStateAndMailbox, "A;B;C")>]
  [<InlineData(RestartUnlessStoppedAndKeepOnlyMailbox, "B;C")>]
  [<InlineData(RestartUnlessStoppedAndKeepStateAndMailbox, "A;B;C")>]
  [<InlineData(AlwaysRestartAndKeepOnlyMailbox, "B;C")>]
  [<InlineData(AlwaysRestartAndKeepStateAndMailbox, "A;B;C")>]
  let ``should restart failed actor`` (strat: SupervisionDecision) (expected: string) =
    task {
      let expected = expected.Split(';') |> Array.toList
      let ct = ct ()
      use system = ActorSystem.withDefaults ()
      let options = GroupOpts(Routers.publishToAll (), Supervision.singleton strat)
      let actor = TestActor.create ()

      use groupInstance =
        defineGroup options
        |= add actor Sub.all TestState.Default
        |> build
        |> run system ct

      use waitHandle = new ManualResetEventSlim(false)

      groupInstance <! PostMsg "A"

      // Wait until the other messages has arrived to the mailbox before throwing the exception,
      // to make sure that the other messages has been added to the inbox.
      groupInstance
      <! Mocks.throwAfterSignal waitHandle (Exception "Sorry")

      groupInstance <! PostMsg "B"
      groupInstance <! PostMsg "C"

      waitHandle.Set()

      let! result = GetState -!> groupInstance
      result.Messages |> should matchList expected
    }

  [<Fact>]
  let ``should publish removed messages as dead letters`` () =
    task {
      use system = ActorSystem.withDefaults ()
      let ct = ct ()

      let options =
        GroupOpts(Routers.publishToAll (), Supervision.singleton SupervisionDecision.AlwaysRestart)

      let actor = TestActor.create ()

      use groupInstance =
        defineGroup options
        |= add actor Sub.all TestState.Default
        |> build
        |> run system ct

      let waitForDeadletters = groupInstance.Events |> waitForDeadletters 2 ct

      use waitHandle = new ManualResetEventSlim(false)
      groupInstance <! PostMsg "A"

      // Wait until the other messages has arrived to the mailbox before throwing the exception,
      // to make sure that the other messages has been added to the inbox.
      groupInstance
      <! Mocks.throwAfterSignal waitHandle (Exception "Sorry")

      groupInstance <! PostMsg "B"
      groupInstance <! PostMsg "C"

      waitHandle.Set()

      let! deadLetters = waitForDeadletters
      deadLetters |> should matchList [ "B"; "C" ]
    }

  [<Fact>]
  let ``should publish all available messages as dead letters and complete when shutdown is used`` () =
    task {
      let ct = ct ()

      use system = ActorSystem.withDefaults ()

      let options =
        GroupOpts(Routers.publishToAll (), Supervision.singleton SupervisionDecision.AlwaysRestart)

      let actor = TestActor.create ()

      use groupInstance =
        defineGroup options
        |= add actor Sub.all TestState.Default
        |> build
        |> run system ct

      let waitForDeadletters = groupInstance.Events |> waitForDeadletters 2 ct

      let waitForTeardown =
        groupInstance.Events
        |> waitForEvent
             (fun e ->
               match e with
               | (GroupEvent TeardownStarted) -> true
               | _ -> false)
             ct

      use shutdownHandle = new ManualResetEventSlim(false)
      use mockStartedHandle = new ManualResetEventSlim(false)

      groupInstance <! PostMsg "A"

      // Wait until the other messages has arrived to the mailbox before throwing the exception,
      // to make sure that the other messages has been added to the inbox.
      groupInstance
      <! Mocks.setThenWait mockStartedHandle shutdownHandle success

      groupInstance <! PostMsg "B"
      groupInstance <! PostMsg "C"

      // Wait to sthudown until the mock message is passed to the handler.
      mockStartedHandle.Wait()

      let shutdownTask = groupInstance.Shutdown()

      let! _ = waitForTeardown
      shutdownHandle.Set()

      let! successfulShutdown = shutdownTask

      let! deadLetters = waitForDeadletters
      deadLetters |> should matchList [ "B"; "C" ]
      successfulShutdown |> should equal true
    }

  [<Fact>]
  let ``should publish message as dead letter when a generated message is not picked up by any actor`` () =
    task {
      let ct = ct ()
      use system = ActorSystem.withDefaults ()

      let options =
        GroupOpts(Routers.publishToAll (), RestartUnlessStoppedAndKeepMailboxIntactStrategy.Instance)

      let actor = TestActor.create ()

      let matcher =
        fun _ ->
          { new IRouteMatcher<_, _> with
              override __.MatchAgainst(msg) =
                match msg with
                | { Message = PostMsg _ } -> false
                | _ -> true }

      use groupInstance =
        defineGroup options
        |= add actor matcher TestState.Default
        |> build
        |> run system ct

      let waitForDeadletters = groupInstance.Events |> waitForDeadletters 1 ct

      groupInstance <! PostNewMsg(PostMsg "A")

      let! deadLetters = waitForDeadletters
      deadLetters |> should matchList [ "A" ]
    }

  [<Fact>]
  let ``should publish message as dead letter when a published message is not picked up by any actor`` () =
    task {
      let ct = ct ()
      use system = ActorSystem.withDefaults ()

      let options =
        GroupOpts(Routers.publishToAll (), RestartUnlessStoppedAndKeepMailboxIntactStrategy.Instance)

      let actor = TestActor.create ()

      let matcher =
        fun _ ->
          { new IRouteMatcher<_, _> with
              override __.MatchAgainst(msg) =
                match msg with
                | { Message = PostMsg _ } -> false
                | _ -> true }

      use groupInstance =
        defineGroup options
        |= add actor matcher TestState.Default
        |> build
        |> run system ct

      let waitForDeadletters = groupInstance.Events |> waitForDeadletters 1 ct

      groupInstance <! PostMsg "A"

      let! deadLetters = waitForDeadletters
      deadLetters |> should matchList [ "A" ]
    }

  [<Fact>]
  let ``should publish message as dead letter when an actors mailbox is full`` () =
    task {
      let ct = ct ()
      use system = ActorSystem.withDefaults ()

      let options =
        GroupOpts(Routers.publishToAll (), RestartUnlessStoppedAndKeepMailboxIntactStrategy.Instance)

      let actor = TestActor.create ()

      use groupInstance =
        defineGroup options
        |= add actor (GroupActorOpts(Sub.all, MailboxSize = Some 1)) TestState.Default
        |> build
        |> run system ct

      let waitForDeadletters = groupInstance.Events |> waitForDeadletters 1 ct

      use shutdownHandle = new ManualResetEventSlim(false)
      use mockStartedHandle = new ManualResetEventSlim(false)

      groupInstance
      <! Mocks.setThenWait mockStartedHandle shutdownHandle success

      mockStartedHandle.Wait()

      groupInstance <! PostMsg "A"
      groupInstance <! PostMsg "B"

      let! result = waitForDeadletters

      shutdownHandle.Set()

      result |> should matchList [ "B" ]
    }

  [<Fact>]
  let ``should combine results from actors when completed`` () =
    task {
      let ct = ct ()
      use system = ActorSystem.withDefaults ()

      let groupOptions =
        GroupOpts(
          Routers.publishToAll (),
          Supervision.singleton (
            SupervisionDecision.NeverRestart
            ||| SupervisionDecision.KeepMailboxIntact
          )
        )

      let actor = TestActor.create ()

      use groupInstance =
        defineGroup groupOptions
        |= add actor Sub.all TestState.Default
        |= add actor Sub.all TestState.Default
        |> build
        |> run system ct

      groupInstance <! PostMsg "A"
      groupInstance <! PostMsg "B"
      groupInstance <! PostMsg "C"

      groupInstance.Complete()

      let! result = groupInstance.Completion

      match result with
      | Error e -> failwith e
      | Ok (resultA, resultB) ->
        resultA.Messages
        |> should matchList [ "A"; "B"; "C" ]

        resultB.Messages
        |> should matchList [ "A"; "B"; "C" ]
    }

  [<Fact>]
  let ``should post scheduled deliveries when timer expires`` () =
    task {
      let ct = ct ()

      let keyword = "Fire"
      use msgHandle = new ManualResetEventSlim(false)

      use system =
        MessageScheduler.TimingWheel.create (TimeSpan.FromMilliseconds(5)) 8
        |> ActorSystem.create

      let groupOptions = GroupOpts(Routers.topic ())

      let triggerer = TestActor.create ()

      let listener =
        Actors.Actor.create (fun _ msg ->
          match msg with
          | PostMsg content when content = keyword ->
            msgHandle.Set()
            set 1
          | _ -> abort ())

      use groupInstance =
        defineGroup groupOptions
        |= add listener (Sub.messages <@ PostMsg @>) (State.init 0)
        |. add triggerer (Sub.messages <@ SchedulePostNewMsg @>) TestState.Default
        |> build
        |> run system ct

      groupInstance
      <! SchedulePostNewMsg(PostMsg keyword, TimeSpan.Zero)

      msgHandle.Wait()

      groupInstance.Complete()

      let! result = groupInstance.Completion

      match result with
      | Error e -> failwith e
      | Ok state -> state |> should equal 1
    }
