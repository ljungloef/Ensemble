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

open Actors
open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels

#nowarn "64"

/// A group actor is an actor that is enriched with some additional funcionality (supervision, routing/subscription mechanisms).
/// A group actor is intended to be used in an `ActorGroup`.
type GroupActor<'State, 'Route, 'Msg> = (GroupContext<'Route, 'Msg> -> Task<Result<'State, string>>)

/// An actor group is a set of actors that work in collaboration, within the same message boundaries.
and ActorGroup<'State, 'Route, 'Msg> = (IActorSystem -> CancellationToken -> IActorGroup<'State, 'Route, 'Msg>)

/// An IActorGroup is the product of running an ActorGroup - basically, an `ActorGroup` instance.
and IActorGroup<'State, 'Route, 'Msg> =
  inherit IDisposable

  /// Events produced by the actor group
  [<CLIEvent>]
  abstract Events: IEvent<ActorGroupEvent<'Route, 'Msg>>

  /// Read only snapshot of the options used when constructing the group.
  abstract member Options: GroupOpts<'Route, 'Msg>

  /// A task that completes when the group has run to completion. A successful run return
  /// the group state as a result.
  abstract member Completion: Task<'State>

  /// Post a new message to the group mailbox.
  abstract member PostMessage: 'Msg -> bool

  /// Post a new message to the group mailbox, using the given route info, instead of a generated route.
  abstract member PostMessage: 'Route * 'Msg -> bool

  /// Post a new message to the group mailbox. Wait and retry until the operation succeeds.
  abstract member PostMessageAsync: 'Msg -> ValueTask

  /// Post a new message using the given route info to the group mailbox. Wait and retry until the operation succeeds.
  abstract member PostMessageAsync: 'Route * 'Msg -> ValueTask

  /// Complete the group. This will complete the group mailbox and no more messages will be accepted.
  /// All group actors' mailboxes will also be completed.
  ///
  /// The group will wait for all actors run to completion, before completing. This can however be controlled using the
  /// `maxCompletionTime` parameter. The parameter will put a max wait time.
  abstract member Complete: ?maxCompletionTime: TimeSpan -> unit

  /// Stop immediately, without waiting for actors to complete their handlers.
  abstract member StopImmediately: unit -> unit

  /// Do a graceful shutdown. The graceful shutdown will try to complete the current handlers
  abstract member Shutdown: unit -> Task<bool>

/// A union for different types of events that can be raised from the group.
and ActorGroupEvent<'Route, 'Msg> =
  | GroupEvent of GroupEventInfo<'Route, 'Msg>
  | ActorGroupEvent of ActorId * ActorGroupEventInfo<'Msg>

/// A union with event info about specific group actors
and ActorGroupEventInfo<'Msg> =
  | ActorRestarted
  | ActorStopped of StopReason
  | ActorTerminated

/// A union with general group event info
and GroupEventInfo<'Route, 'Msg> =
  | TeardownStarted
  | DeadLetter of RouteMessage<'Route, 'Msg> * DeadLetterReason
  | DeadLetters of RouteMessage<'Route, 'Msg> list * DeadLetterReason

/// Reason to why messages was treated as deadletters.
and DeadLetterReason =
  | ActorMailboxFull of ActorId
  | NoReceivers

/// An interface that can be implemented to control
/// when and how group actors are restarted.
and ISupervisionStrategy =

  /// Make a decision what to do with the given actor.
  abstract member Decide: unit -> SupervisionDecision

and [<Flags>] SupervisionDecision =
  | NeverRestart = 0x0
  | RestartOnCompletion = 0x1
  | RestartOnFailure = 0x2
  | RestartUnlessStopped = 0x3
  | RestartWhenStopped = 0x4
  | RestartWhenAborted = 0x8
  | AlwaysRestart = 0xF

  /// ---
  | KeepState = 0x0100
  | KeepMailboxIntact = 0x0200

and RestartOnFailureAndKeepMailboxIntactStrategy() =
  interface ISupervisionStrategy with
    override __.Decide() =
      SupervisionDecision.RestartOnFailure
      ||| SupervisionDecision.KeepMailboxIntact
  with
    static member Instance = RestartOnFailureAndKeepMailboxIntactStrategy()

/// Options used to configure an actor group.
and GroupOpts<'Route, 'Msg>
  (
    router: IMessageRouter<'Route, 'Msg>,
    ?fallbackSupervisionStrat: ISupervisionStrategy,
    ?maxTeardownTime: TimeSpan,
    ?defaultActorMailboxSize: int,
    ?groupMailboxSize: int
  ) =

  /// The router that should be used for routing messages. Required.
  member val Router = router

  /// The supervision strategy that should be used. Optional, defaults to `RestartUnlessStoppedAndKeepMailboxIntactStrategy`
  member val FallbackSupervisionStrategy =
    defaultArg fallbackSupervisionStrat RestartOnFailureAndKeepMailboxIntactStrategy.Instance with get, set

  /// The upper time limit of group teardown. Optional, defaults to 10s
  member val MaxTeardownTime = defaultArg maxTeardownTime (TimeSpan.FromSeconds(10)) with get, set

  /// The upper size limit of actors' mailbox. This can also be overriden by actors in their
  /// specific actor options. Optional, defaults to 10 000 messages.
  member val DefaultActorMailboxSize = defaultArg defaultActorMailboxSize 10_000 with get, set

  /// The uppser size limit of the internal communcation channel in the group. Optional, defaults to 100 000 messages.
  member val GroupMailboxSize = defaultArg groupMailboxSize 100_000 with get, set

/// Options used to configure a specific group actor in a group.
and GroupActorOpts<'Route, 'Msg>
  (
    routeMatcherFactory: GroupOpts<'Route, 'Msg> -> IRouteMatcher<'Route, 'Msg>,
    ?supervisionStrat: ISupervisionStrategy,
    ?mailboxSize: int
  ) =

  /// The supervision strategy that should be used for this specific actor. (Optional)
  member val SupervisionStrategy = supervisionStrat with get, set

  /// The upper size limit of this specific actor's mailbox (Optional)
  member val MailboxSize = mailboxSize with get, set

  /// The route matcher that should be used to match messages. Required
  member val RouteMatcherFactory = routeMatcherFactory

/// The result of combining the results from the group and a specific actors options.
and GroupActorOptsMerged<'Route, 'Msg>(groupOpts: GroupOpts<'Route, 'Msg>, actorOpts: GroupActorOpts<'Route, 'Msg>) =

  static let merge (spec: option<'T>) (fallback: 'T) : 'T = spec |> Option.defaultValue fallback

  /// The route matcher that should be used to match messages. Required
  member __.RouteMatcher = actorOpts.RouteMatcherFactory(groupOpts)

  /// The supervision strategy that should be used for this specific actor.
  member __.SupervisionStrategy =
    merge actorOpts.SupervisionStrategy groupOpts.FallbackSupervisionStrategy

  /// The upper size limit of this specific actor's mailbox
  member __.MailboxSize = merge actorOpts.MailboxSize groupOpts.DefaultActorMailboxSize

  /// Reference to the original options.
  member __.Original = actorOpts

/// A container that keep track of a specific group actors values in a group.
and GroupActorInstance<'Route, 'Msg>(id: ActorId, mailbox: IMailbox<'Msg>, router: IRouteMatcher<'Route, 'Msg>) =

  member __.Id = id
  member __.Mailbox = mailbox
  member __.Router = router

/// The context constructed for a specific group.
and GroupContext<'Route, 'Msg>
  (
    killSwitch: CancellationTokenSource,
    options: GroupOpts<'Route, 'Msg>,
    router: IMessageRouter<'Route, 'Msg>,
    groupMailbox: IMailbox<RouteMessage<'Route, 'Msg>>,
    groupMailboxAdapter: IOutbox<'Msg>,
    systemMailbox: IMailbox<GroupSystemMessages<'Msg>>,
    events: Event<ActorGroupEvent<'Route, 'Msg>>,
    system: IActorSystem,
    actors: ResizeArray<GroupActorInstance<'Route, 'Msg>>
  ) =

  /// The cancellation token that be used to track whether the group should continue, or stop.
  member __.CancellationToken = killSwitch.Token

  /// An internal switch that can be used to shutdown the group from the inside.
  member __.KillSwitch = killSwitch

  /// The group's router
  member __.Router = router

  /// The group's internal mailbox.
  member __.GroupMailbox = groupMailbox

  /// The group's internal mailbox mapped to the underlying message type. This adapter
  /// is intended to be the actors' outbox, and the messages written to the outbox is mapped
  /// to a route message. The route is generated by convention by the `Router`.
  member __.GroupMailboxAdapter = groupMailboxAdapter

  /// The group's internal channel for sending commands.
  member __.SystemMailbox = systemMailbox

  /// The group's options.
  member __.Options = options

  /// The group's event handler
  member __.Events = events

  /// The actor system used by the group
  member __.System = system

  /// All actor instances
  member __.Actors = actors

  /// Helper method to more conveniently check the cancellation token's status
  member self.NotCancelled() =
    not self.CancellationToken.IsCancellationRequested

/// System messages that is used internally in the group
and GroupSystemMessages<'Msg> =
  | CompleteGroup of maxCompletionTime: TimeSpan
  | ShutdownGroup of Request<bool>

/// The actor group implementation. The entry point to interact with the group.
and ActorGroupImpl<'State, 'Route, 'Msg>(ctx: GroupContext<_, _>, completion: Task<'State>) =

  let mutable disposed = 0

  let dispose (disposing: bool) =
    if Interlocked.Exchange(&disposed, 1) = 0 then
      ctx.KillSwitch.Cancel()

      if disposing then
        ctx.KillSwitch.Dispose()

  let throwIfDisposed () =
    if disposed > 0 then
      raise (ObjectDisposedException(nameof (ActorGroupImpl)))

  interface IActorGroup<'State, 'Route, 'Msg> with

    [<CLIEvent>]
    override __.Events = ctx.Events.Publish

    override __.Options = ctx.Options
    override __.Completion = completion

    override __.PostMessage(msg) =
      throwIfDisposed ()

      let route = ctx.Router.GenerateRoute(msg)

      ctx.GroupMailbox
      <!? { Route = route; Message = msg }

    override __.PostMessageAsync(msg) =
      throwIfDisposed ()

      let route = ctx.Router.GenerateRoute(msg)

      ctx.GroupMailbox
      <!! { Route = route; Message = msg }

    override __.PostMessage(route, msg) =
      throwIfDisposed ()

      ctx.GroupMailbox
      <!? { Route = route; Message = msg }

    override __.PostMessageAsync(route, msg) =
      throwIfDisposed ()

      ctx.GroupMailbox
      <!! { Route = route; Message = msg }

    override __.Complete(maxCompletionTime) =
      throwIfDisposed ()

      let maxCompletionTime = defaultArg maxCompletionTime (TimeSpan.FromMinutes(1))

      ctx.SystemMailbox
      <! CompleteGroup maxCompletionTime

    override __.Shutdown() =
      throwIfDisposed ()

      task {
        let request = Request.withCancellation (CancellationToken.None)

        do! ctx.SystemMailbox <!! ShutdownGroup request

        return! request |> Request.asTask
      }

    override __.StopImmediately() =
      throwIfDisposed ()
      dispose true

    override self.Dispose() =
      dispose true
      GC.SuppressFinalize(self)

  override __.Finalize() = dispose false

module Supervision =

  /// Create a new supervision strategy that always result in the `decision`
  let inline singleton decision =
    { new ISupervisionStrategy with
        override __.Decide() = decision }

  /// Get a reference to the default strategy (`RestartOnFailureAndKeepMailboxIntactStrategy`)
  let inline defaultStrategy () =
    RestartOnFailureAndKeepMailboxIntactStrategy.Instance

module ActorGroup =

  open InboxHelpers

  /// Create a new group mailbox
  let inline newGroupMailbox singleWriter size =
    BoundedChannelOptions(
      size,
      SingleReader = true,
      SingleWriter = singleWriter,
      FullMode = BoundedChannelFullMode.Wait
    )
    |> ChannelMailbox.fromOpts

  /// Create a new group mailbox
  let inline newActorMailbox size =
    BoundedChannelOptions(size, SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait)
    |> ChannelMailbox.fromOpts

  /// Raise the event `e`
  let inline raiseEvent (events: Event<_>) e = e |> events.Trigger

  /// Raise the group event `e`
  let inline raiseGroupEvent e events = e |> GroupEvent |> raiseEvent events

  /// Raise the actor event `e` on behalf of `actorId`
  let inline raiseActorEvent e events actorId =
    ActorGroupEvent(actorId, e) |> raiseEvent events

  /// Take a snapshot of the current available messages and, (1) discard them from the mailbox and (2) publish the discarded events
  /// as dead letters.
  let inline forwardDeadLetters (ctx: GroupContext<_, _>) (mailbox: IMailbox<_>) id fallbackSize =
    let limit =
      if mailbox.CanCount then
        mailbox.Count
      else
        fallbackSize

    let deadLetters =
      mailbox
      |> Mailbox.fastReadLimited limit
      |> Seq.map (fun msg -> { Route = "Unknown.."; Message = msg })
      |> Seq.toList

    raiseGroupEvent (DeadLetters(deadLetters, (ActorMailboxFull id))) ctx.Events

  /// Active listener on the group mailbox. Incoming messages are routed to the
  /// actors that are interested in the message.
  ///
  /// If a message has no recipient, or an actors mailbox is full, the message is published as
  /// a deadletter.
  let inline forwardIncMessages (ctx: GroupContext<_, _>) =
    readInbox
      ctx.GroupMailbox
      (fun msg ->
        let matches =
          ctx.Actors
          |> Seq.filter (fun a -> a.Router.MatchAgainst(msg))

        let mutable receiveCount = 0

        for actor in matches do
          receiveCount <- receiveCount + 1

          if not (actor.Mailbox <!? msg.Message) then
            raiseGroupEvent (DeadLetter(msg, ActorMailboxFull actor.Id)) ctx.Events

        if receiveCount = 0 then
          raiseGroupEvent (DeadLetter(msg, NoReceivers)) ctx.Events

        InboxReadCmd.ContinueReading)
      ctx.CancellationToken

  /// Begin the process to stop all group processes.
  let inline callStop (ctx: GroupContext<_, _>) =
    if not ctx.KillSwitch.IsCancellationRequested then
      ctx.KillSwitch.Cancel()
      raiseGroupEvent TeardownStarted ctx.Events

  /// Wait for the teardown to complete, or timeout; whichever happens first.
  let inline teardown (completion: Task<_>) (maxTime: TimeSpan) ct =
    task {
      let cancel = Task.Delay(maxTime, ct)
      let! completed = Task.WhenAny(completion, cancel)

      return completed = completion
    }

  /// Begin the process of gracefully shutting down the group.
  let inline gracefulShutdown actorCompletion (ctx: GroupContext<_, _>) =
    task {
      callStop ctx
      return! teardown actorCompletion ctx.Options.MaxTeardownTime CancellationToken.None
    }

  /// Complete the group and wait for all actors to complete, or timeout; whichever happens first.
  let inline completeGroup actorCompletion (ctx: GroupContext<_, _>) (maxCompletionTime: TimeSpan) =
    task {
      ctx.SystemMailbox.Complete(None)

      for actor in ctx.Actors do
        actor.Mailbox.Complete(None)

      return! teardown actorCompletion maxCompletionTime ctx.CancellationToken
    }

  /// Active listener and delegator of incoming group commands.
  let inline handleSystemMessages actorCompletion (ctx: GroupContext<_, _>) =
    task {

      let! result =
        readInboxAsync
          ctx.SystemMailbox
          (fun message ->
            task {
              match message with
              | CompleteGroup maxCompletionTime ->
                let! _ = completeGroup actorCompletion ctx maxCompletionTime
                return InboxReadCmd.CompleteReading

              | ShutdownGroup reply ->
                let! result = gracefulShutdown actorCompletion ctx
                reply |> Request.respond result |> ignore
                return InboxReadCmd.StopReading

            })
          ctx.CancellationToken

      return result
    }

  /// The group's main process.
  let inline groupMainloop (actorGroup: GroupActor<'State, _, _>) (ctx: GroupContext<_, _>) =
    task {

      let mutable result = Unchecked.defaultof<Result<'State, string>>
      let mutable cont = true

      let actorCompletion = ctx |> actorGroup

      while ctx.NotCancelled() && cont do
        let forwarder = forwardIncMessages ctx
        let systemMessageTask = handleSystemMessages actorCompletion ctx

        let! completedTask = Task.WhenAny(actorCompletion, systemMessageTask, forwarder)

        if completedTask = actorCompletion then
          let! res = actorCompletion
          result <- res
          cont <- false

      return result
    }

  /// Lift the `actor` to a group actor.
  let inline lift (opts: GroupActorOpts<_, _>) stateInitializer (actor: Actor<_, _>) : (GroupActor<_, _, _>) =
    fun ctx ->
      task {
        let final = GroupActorOptsMerged(ctx.Options, opts)
        let actorMailbox = newActorMailbox final.MailboxSize
        let id = Guid.NewGuid().ToString("N") |> ActorId

        ctx.Actors.Add(GroupActorInstance(id, actorMailbox, final.RouteMatcher))

        let mutable cont = true
        let mutable state = stateInitializer ()
        let mutable finalResult = Error "Timeout"

        while ctx.NotCancelled() && cont do
          let! result = actor ctx.System state actorMailbox ctx.GroupMailboxAdapter ctx.CancellationToken

          raiseActorEvent (ActorStopped result.StopReason) ctx.Events id

          let decision = final.SupervisionStrategy.Decide()

          cont <-
            match result.StopReason with
            | Cancelled -> false
            | Exn _ -> decision.HasFlag(SupervisionDecision.RestartOnFailure)
            | Complete -> decision.HasFlag(SupervisionDecision.RestartOnCompletion)
            | Stopped -> decision.HasFlag(SupervisionDecision.RestartWhenStopped)
            | Aborted -> decision.HasFlag(SupervisionDecision.RestartWhenAborted)

          if cont then
            state <-
              if decision.HasFlag(SupervisionDecision.KeepState) then
                result.State
              else
                stateInitializer ()

            if not (decision.HasFlag(SupervisionDecision.KeepMailboxIntact)) then
              forwardDeadLetters ctx actorMailbox id final.MailboxSize
          else
            forwardDeadLetters ctx actorMailbox id final.MailboxSize

            finalResult <-
              match result.StopReason with
              | Stopped
              | Aborted -> Error "Stopped"
              | Cancelled -> Error "Cancelled"
              | Exn e -> Error $"Exception: %A{e}"
              | Complete -> Ok result.State

        return finalResult
      }

[<AutoOpen>]
module ActorGroups =

  /// Combine the result from `a` and `b` in a tuple.
  let inline when2 (a: Task<'a>) (b: Task<'b>) : Task<'a * 'b> =
    task {
      let! _ = Task.WhenAll(a, b)
      let! valueA = a
      let! valueB = b

      return (valueA, valueB)
    }

  /// Run the two group actors `a` and `b`. If both succeeds, map their results using `f`.
  let inline run2 (a: GroupActor<_, _, _>) (b: GroupActor<_, _, _>) ([<InlineIfLambda>] f) : GroupActor<_, _, _> =
    fun ctx ->
      task {
        let taskA = a ctx
        let taskB = b ctx

        let! result = when2 taskA taskB

        return
          match result with
          | Ok resultA, Ok resultB -> Ok(f resultA resultB)
          | Error e, Ok _ -> Error $"Actor 'a' failed with the error '%s{e}'"
          | Ok _, Error e -> Error $"Actor 'b' failed with the error '%s{e}'"
          | Error e1, Error e2 -> Error $"Actor 'a' and 'b' failed with the errors '%s{e1}' and %s{e2}"
      }

  /// Run the two group actors `a` and `b`. Keep the result from both actors, and combine them in a tuple.
  let inline (<.>) (a: GroupActor<_, _, _>) (b: GroupActor<_, _, _>) : GroupActor<_, _, _> =
    run2 a b (fun resA resB -> (resA, resB))

  /// Run the two group actors `a` and `b`. Keep the result from only `b`.
  let inline (.>) (a: GroupActor<_, _, _>) (b: GroupActor<_, _, _>) : GroupActor<_, _, _> =
    run2 a b (fun _ resB -> resB)

  /// Run the two group actors `a` and `b`. Keep the result from only `a`.
  let inline (<.) (a: GroupActor<_, _, _>) (b: GroupActor<_, _, _>) : GroupActor<_, _, _> =
    run2 a b (fun resA _ -> resA)

  /// Initial state when building a group.
  type GroupInitDefinition<'Route, 'Msg> = { Options: GroupOpts<'Route, 'Msg> }

  /// Current state when building a group.
  type GroupDefinition<'TGrpState, 'Route, 'Msg> =
    { Options: GroupOpts<'Route, 'Msg>
      Head: GroupActor<'TGrpState, 'Route, 'Msg> }

  module GroupActorLiftResolver =

    type Resolver = Resolve
      with
        static member inline LiftActor
          (
            _: Resolver,
            matcher: GroupOpts<_, _> -> IRouteMatcher<_, _>,
            stateInitializer: unit -> _,
            actor
          ) : GroupActor<_, _, _> =
          ActorGroup.lift (GroupActorOpts(matcher)) stateInitializer actor

        static member inline LiftActor
          (
            _: Resolver,
            opts: GroupActorOpts<_, _>,
            stateInitializer: unit -> _,
            actor
          ) : GroupActor<_, _, _> =
          ActorGroup.lift opts stateInitializer actor

  module GroupDefinitionResolver =

    type Resolver = Resolve
      with
        static member inline GetDefinition
          (
            _: Resolver,
            def: GroupInitDefinition<_, _>,
            actor,
            _
          ) : GroupDefinition<_, _, _> =
          { Options = def.Options; Head = actor }

        static member inline GetDefinition
          (
            _: Resolver,
            def: GroupDefinition<_, _, _>,
            actor,
            f
          ) : GroupDefinition<_, _, _> =
          { Options = def.Options
            Head = f def.Head actor }

  /// Helper function, and not intended to be call directly. Helps to resolve the
  /// correct function given the current builder state.
  let inline resolvedAdd (arg: ^x) actor f : GroupDefinition<_, _, _> =
    ((^b or ^x): (static member GetDefinition: ^b * ^x * _ * _ -> _) (GroupDefinitionResolver.Resolver.Resolve,
                                                                      arg,
                                                                      actor,
                                                                      f))

  /// Start building a new group definition with the supplied group options.
  let inline defineGroup opts = { Options = opts }

  /// Start building a new group definition with default options, and the given `router`. A more narrow variant of `defineGroup`
  let inline groupWith router = { Options = GroupOpts<_, _>(router) }

  /// Takes an actor and lifts it to a group actor. Shorthand for ActorGroup.lift but named as `add`
  /// to work better together with (|=) and (|.). Also provides some additional parametric polymorphism for options and state.
  ///
  /// E.g. use `GroupActorOpts()`, Sub.topic ">" or any other match resolver for `optsArg`
  let inline add (actor: Actor<_, _>) (optsArg: ^x) (stateArg: ^y) : GroupActor<_, _, _> =
    ((^b or ^x or ^y): (static member LiftActor: ^b * ^x * ^y * _ -> _) (GroupActorLiftResolver.Resolve,
                                                                         optsArg,
                                                                         stateArg,
                                                                         actor))

  /// Add the group actor proved as `actor` to the group, and keep the result of the actor when it completes.
  let inline (|=) (a: ^x) (actor: GroupActor<_, _, _>) = resolvedAdd a actor (<.>)

  /// Add the group actor proved as `actor` to the group, but do not keep the result of the actor after it completes.
  let inline (|.) (a: ^x) (actor: GroupActor<_, _, _>) = resolvedAdd a actor (<.)

  /// Build the group. The result from this is an `ActorGroup` that can be started via the `run` function.
  let inline build (definition: GroupDefinition<_, _, _>) : (ActorGroup<_, _, _>) =
    fun system ct ->
      let killSwitch = CancellationTokenSource.CreateLinkedTokenSource(ct)
      let router = definition.Options.Router

      let groupEvents = Event<_>()

      let groupMessages =
        ActorGroup.newGroupMailbox false definition.Options.GroupMailboxSize

      let systemMessages =
        ActorGroup.newGroupMailbox true definition.Options.GroupMailboxSize

      let groupMessagesAdapter =
        groupMessages
        |> Mailbox.mapOutbox (fun msg ->
          { Route = router.GenerateRoute(msg)
            Message = msg })

      let systemCtx =
        GroupContext(
          killSwitch,
          definition.Options,
          router,
          groupMessages,
          groupMessagesAdapter,
          systemMessages,
          groupEvents,
          system,
          ResizeArray()
        )

      let completion =
        Task.Run<Result<_, string>>(
          Func<Task<Result<_, string>>>(fun () -> ActorGroup.groupMainloop definition.Head systemCtx)
        )

      new ActorGroupImpl<_, _, _>(systemCtx, completion)

  /// Start a new actor group run
  let inline run system (ct: CancellationToken) (group: ActorGroup<_, _, _>) = ct |> group system
