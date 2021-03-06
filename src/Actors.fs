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
open System.Threading
open System.Threading.Tasks

/// The defintion of an actor.
type Actor<'State, 'Msg> =
  (IActorSystem -> 'State -> IInbox<'Msg> -> IOutbox<'Msg> -> CancellationToken -> Task<ActorCompletionResult<'State>>)

/// Data container used by an actor to tell the result of a run. The `StopReason` prop reveals the reason to why
/// the actor stopped processing.
and ActorCompletionResult<'State> =
  { State: 'State
    StopReason: StopReason }

/// The different reasons to why an actor can stop.
and [<Struct>] StopReason =

  /// The actor got cancelled.
  | Cancelled

  /// The actor was stopped due to an unhandled exception
  | Exn of Exception

  /// The actor stopped because the inbox has been marked as completed.
  | Complete

  /// The actor was purposely stopped after processing an item.
  | Stopped

  /// The processing was aborted. This is basically the same as `Stopped`, with the semantic difference that
  /// abort indicates that the processing was stopped abruptly.
  | Aborted

/// An actors identity.
type ActorId = ActorId of string

/// An actor message func is a function handler that is used to
/// handle the processing of one message.
///
/// The return evaluates to `ActorMessageContext{} -> bool` and is not ment to be setup manually. Instead
/// use the existing helpers available in the namespace (e.g. success, set, stop, post)
type ActorMessageFunc<'State, 'Msg> = 'State -> 'Msg -> (ActorMessageContext<'State, 'Msg> -> bool)

/// A context used internally to keep track of the actions reported by a message handler.
and ActorMessageContext<'State, 'Msg>(state: 'State, handler: ActorMessageFunc<'State, 'Msg>) =

  let mutable output: 'Msg list = []
  let mutable scheduledOutput: DeliveryInstruction<'Msg> list = []
  let mutable stop: bool voption = ValueNone
  let mutable state = state
  let mutable handler = handler

  let mutable status = ActorMessageStatus.NoChanges

  let update (field: byref<_>) value statusLog =
    field <- value
    status <- status ||| statusLog

  /// Get the current status
  member __.Status = status

  /// Add messages generated by the handler. These messages
  /// will be posted to the outbox once the handler completes.
  member __.Output
    with get () = output
    and set (v) = update &output v ActorMessageStatus.HasOutputs

  /// Add messages scheduled by the handler. These messages
  /// will be registered in the actor system's scheduler once the handler completes.
  /// The scheduled messages will be delivered according to the registered schedule.
  member __.ScheduledOutput
    with get () = scheduledOutput
    and set (v) = update &scheduledOutput v ActorMessageStatus.HasScheduledOutputs

  /// Mark that the actor should be stopped after the handler completes.
  /// Set to `Some true` if the handler was aborted unexpectedly, `None` or `false` otherwise.
  member __.Stop
    with get () = stop
    and set (v) = update &stop v ActorMessageStatus.MarkedStopped

  /// The new state (that later will be used as input when processing the next message)
  member __.State
    with get () = state
    and set (v) = update &state v ActorMessageStatus.StateChange

  /// Set the handler that should be used when processing the next message. Can be used to switch behavior
  /// between processings.
  member __.Handler
    with get () = handler
    and set (v) =
      handler <- v
      status <- status ||| ActorMessageStatus.BehaviorChange

  /// The context is reused between handlers. This method is used
  /// to reset the state of the context.
  member __.Reset() =
    output <- []
    scheduledOutput <- []
    stop <- ValueNone
    status <- ActorMessageStatus.NoChanges

and [<Flags>] ActorMessageStatus =
  | NoChanges = 0x0
  | MarkedStopped = 0x1
  | HasOutputs = 0x2
  | BehaviorChange = 0x4
  | StateChange = 0x8
  | HasScheduledOutputs = 0x16

type NoneState = NoneState of unit

module State =

  /// Create a new state initializer that always will resolve to a `NoneState` instance.
  let inline none () = NoneState()

  /// Create a new state initializer that always will use the supplied value v as the initial object. A struct is advised here.
  let inline init v = fun _ -> v

[<AutoOpen>]
module ActorMessageFuncs =

  /// Apply `a` and then `b` if `a` succeeded
  let inline (<&>) a b =
    fun (ctx: ActorMessageContext<_, _>) -> a ctx && b ctx

  /// Can be used to update the context
  let inline updateCtx (ctx: ActorMessageContext<_, _>) ([<InlineIfLambda>] f) =
    f ctx
    true

  /// Swap out the current handler for the supplied handler. The next message will be procesed using the new handler.
  let inline become (handler: ActorMessageFunc<_, _>) =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.Handler <- handler)

  /// Mark the handler as successful. Should mainly only be used as a function to initialize the return value when no other message funcs are used.
  let inline success () =
    fun (_: ActorMessageContext<_, _>) -> true

  /// Stage the message to be posted to the outbox after the handler completes.
  let inline post msg =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.Output <- [ msg ])

  /// Stage all the messages to be posted to the outbox after the handler completes.
  let inline postMany (msg: seq<_>) =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.Output <- msg |> Seq.toList)

  /// Schedule a message to be posted to the outbox as instructed by the provided `delivery` instruction.
  let inline postLater delivery =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.ScheduledOutput <- [ delivery ])

  /// Update the state to the new `state`
  let inline set state =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.State <- state)

  /// Stop the actor, and indicate that this was an abrupt stop.
  let inline abort () =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.Stop <- ValueSome true)

  /// Stop the actor.
  let inline stop () =
    fun (ctx: ActorMessageContext<_, _>) -> updateCtx ctx (fun ctx -> ctx.Stop <- ValueSome false)

module Actors =

  module InboxHelpers =

    type InboxReadCmd =
      | ContinueReading = 0x0
      | StopReading = 0x1
      | AbortReading = 0x2
      | CompleteReading = 0x3

    /// Read messsages from the inbox until either (1) the inbox is marked as completed, or (2) the `handler` reports back anything but `ContinueReading`
    let inline readInbox (inbox: IInbox<_>) ([<InlineIfLambda>] handler) (ct: CancellationToken) =
      task {
        try
          let mutable result = InboxReadCmd.ContinueReading

          while result = InboxReadCmd.ContinueReading
                && not ct.IsCancellationRequested do
            let read, item = inbox.TryRead()

            if read then
              result <- handler item
            else
              let! canReadMore = inbox.WaitToReadAsync(ct)

              result <-
                if canReadMore then
                  InboxReadCmd.ContinueReading
                else
                  InboxReadCmd.CompleteReading

          return
            match result with
            | _ when ct.IsCancellationRequested -> Cancelled
            | InboxReadCmd.StopReading -> Stopped
            | InboxReadCmd.AbortReading -> Aborted
            | InboxReadCmd.CompleteReading -> Complete
            | _ -> Stopped
        with
        | :? TaskCanceledException -> return Cancelled
        | :? OperationCanceledException -> return Cancelled
        | e -> return Exn e
      }

    /// Read messsages from the inbox until either (1) the inbox is marked as completed, or (2) the `handler` reports back anything but `ContinueReading`
    let inline readInboxAsync (inbox: IInbox<_>) ([<InlineIfLambda>] handler) (ct: CancellationToken) =
      task {
        try
          let mutable result = InboxReadCmd.ContinueReading

          while result = InboxReadCmd.ContinueReading
                && not ct.IsCancellationRequested do
            let read, item = inbox.TryRead()

            if read then
              let! handlerResult = handler item
              result <- handlerResult
            else
              let! canReadMore = inbox.WaitToReadAsync(ct)

              result <-
                if canReadMore then
                  InboxReadCmd.ContinueReading
                else
                  InboxReadCmd.CompleteReading

          return
            match result with
            | _ when ct.IsCancellationRequested -> Cancelled
            | InboxReadCmd.StopReading -> Stopped
            | InboxReadCmd.AbortReading -> Aborted
            | InboxReadCmd.CompleteReading -> Complete
            | _ -> Stopped
        with
        | :? TaskCanceledException -> return Cancelled
        | :? OperationCanceledException -> return Cancelled
        | e -> return Exn e
      }

  open InboxHelpers

  module Actor =

    module Context =

      let inline markedWith flag (context: ActorMessageContext<_, _>) = context.Status.HasFlag(flag)

      let inline is flag (context: ActorMessageContext<_, _>) = markedWith flag context

      let inline whenMarked flag ([<InlineIfLambdaAttribute>] f) (context: ActorMessageContext<_, _>) =
        if context |> markedWith flag then
          f context

    module Helpers =

      let inline postOutputs outbox ct (ctx: ActorMessageContext<_, _>) = Mailbox.postAll ctx.Output ct outbox

      let inline scheduleDeliveries (system: IActorSystem) outbox (ctx: ActorMessageContext<_, _>) =
        system.Scheduler.ScheduleDeliveries(ctx.ScheduledOutput, outbox)

    open type ActorMessageStatus
    open Helpers

    /// Create a new actor using the given `handler` as the message handler.
    let inline create handler : Actor<_, _> =
      fun system state inbox outbox ct ->
        task {
          let mutable stagedCtx = ActorMessageContext(state, handler)
          let mutable currentCtx = stagedCtx

          let postOutputs = postOutputs outbox ct
          let scheduleDeliveries = scheduleDeliveries system outbox

          let! stopReason =
            readInbox
              inbox
              (fun msg ->
                let pipe = currentCtx.Handler currentCtx.State msg
                let _ = pipe stagedCtx

                if Context.is MarkedStopped stagedCtx then
                  match stagedCtx.Stop with
                  | ValueSome true -> InboxReadCmd.AbortReading
                  | _ -> InboxReadCmd.StopReading
                else
                  Context.whenMarked HasOutputs postOutputs stagedCtx
                  Context.whenMarked HasScheduledOutputs scheduleDeliveries stagedCtx

                  currentCtx <- stagedCtx
                  stagedCtx.Reset()

                  InboxReadCmd.ContinueReading)
              ct

          return
            { State = currentCtx.State
              StopReason = stopReason }
        }

    /// Run the actor using the given `state`, `inbox`, `outbox` and `ct` as parameters.
    let inline run system state inbox outbox ct (actor: Actor<_, _>) = actor system state inbox outbox ct
