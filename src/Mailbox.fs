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

open System.Threading
open System.Threading.Tasks

/// Container for inbound messages that can be read
type IInbox<'Msg> =

  /// Not all implementation can count and `CanCount` can be used to track the count capabilities of the implementation
  abstract member CanCount: bool

  /// Number of items available to be read
  abstract member Count: int

  /// Try to read the next available item. Returns `true` if the operation was successful
  abstract member TryRead: outref<'Msg> -> bool

  /// Check if an item is available to read, or wait until one is available otherwise. The operation can also complete
  /// if the inbox is marked as completed, and hence will receive any more items.
  /// The return value indicates whether the next item is available or not.
  abstract member WaitToReadAsync: CancellationToken -> ValueTask<bool>

/// Container where outbound messages can be posted to
type IOutbox<'Msg> =

  /// Post a message to the outbox. Returns true if successful, otherwise false.
  abstract member PostMessage: 'Msg -> bool

  /// Post a message to the outbox immediately, but wait until the operation completes if the outbox is full.
  abstract member PostMessageAsync: 'Msg -> ValueTask

  /// Mark the outbox as complete, meaning that no more messages can be written to the outbox.
  /// The inbox pair will be notified, to also propagate the completion to all consumers.
  abstract member Complete: exn option -> unit

/// An inbox and outbox pair
type IMailbox<'Msg> =
  inherit IInbox<'Msg>
  inherit IOutbox<'Msg>

module Mailbox =

  /// Try to read `cap` amount of items from the inbox. The result is available as a seq.
  let inline fastReadLimited cap (box: IMailbox<_>) =
    seq {
      let mutable i = 0

      while i <= cap do
        let read, item = box.TryRead()

        if read then
          yield item
        else
          i <- cap + 1 // Exit
    }

  /// Read and discard up to `amount` items in the inbox.
  let inline discard amount (box: IInbox<_>) =
    let mutable i = amount
    let mutable cont = true

    while cont do
      let read, _ = box.TryRead()
      i <- i - 1
      cont <- read && i > 0

  /// Creates a new inbox that will map all read items from the `box`, using the supplied `f` function.
  let inline mapInbox ([<InlineIfLambda>] f) (box: IInbox<_>) =
    { new IInbox<_> with

        override __.CanCount = box.CanCount
        override __.Count = box.Count

        override __.TryRead msg =
          match box.TryRead() with
          | true, m ->
            msg <- f m
            true
          | false, _ -> false

        override __.WaitToReadAsync(ct) = box.WaitToReadAsync(ct) }

  /// Creates a new outbox that will map all posted items to the `box`, using the supplied `f` function.
  let inline mapOutbox ([<InlineIfLambda>] f) (box: IOutbox<_>) =
    { new IOutbox<_> with

        override __.PostMessage(msg) =
          let mapped = f msg
          box.PostMessage(mapped)

        override __.PostMessageAsync(msg) =
          let mapped = f msg
          box.PostMessageAsync(mapped)

        override __.Complete(exn) = box.Complete(exn) }

  /// Try to write the `msg` to the `outbox`. All failed attemps will be retried until successful, or the cancellation token is
  /// marked as cancel.
  let inline post msg (ct: CancellationToken) (outbox: IOutbox<_>) =
    while not ct.IsCancellationRequested
          && not (outbox.PostMessage(msg)) do
      Thread.Sleep(20)

  /// Try to write all the messages in `msgs` to the `outbox`. All failed attemps will be retried until successful, or the cancellation token is
  /// marked as cancel.
  let inline postAll msgs (ct: CancellationToken) (outbox: IOutbox<_>) =
    for msg in msgs do
      post msg ct outbox

[<AutoOpen>]
module MailboxMethods =

  open System

  module Helpers =

    let inline postMessage (receiver: ^x) msg : bool =
      (^x: (member PostMessage: _ -> bool) (receiver, msg))

    let inline postMessageRouted (receiver: ^x) route msg : bool =
      (^x: (member PostMessage: _ * _ -> bool) (receiver, route, msg))

    let inline postMessageAsync (receiver: ^x) msg : ValueTask =
      (^x: (member PostMessageAsync: _ -> ValueTask) (receiver, msg))

    let inline postMessageRoutedAsync (receiver: ^x) route msg : ValueTask =
      (^x: (member PostMessageAsync: _ * _ -> ValueTask) (receiver, route, msg))

  open Helpers

  /// Try to post a message to the receiver, and return the result as a bool.
  let inline (<!?) (receiver: ^x) msg = postMessage receiver msg

  /// Try to post a message to the receiver. If the post is not successful, the message will be discarded.
  let inline (<!) (receiver: ^x) msg = postMessage receiver msg |> ignore

  /// Try to post a message to the receiver until it succeeds. Returns a ValueTask that completes when the operation is successful.
  let inline (<!!) (receiver: ^x) msg = postMessageAsync receiver msg

  /// Try to post a message using the given route to the receiver. If the post is not successful, the message will be discarded.
  let inline ( <<!) (receiver: ^x) (route, msg) =
    postMessageRouted receiver route msg |> ignore

  /// Try to post a message using the given route to the receiver, and return the result as a bool.
  let inline (<<!?) (receiver: ^x) (route, msg) =
    postMessageRouted receiver route msg |> ignore

  /// Try to post a message the receiver, using the given route, until it succeeds. Returns a ValueTask that completes when the operation is successful.
  let inline (<<!!) (receiver: ^x) (route, msg) = postMessageRouted receiver route msg

  /// Post a new request with default timeout (1m) to the target mailbox. The returned task can be used to
  /// await the response.
  let inline (-!>) (msg: Request<_> -> _) (mailboxOwner: ^x) =
    task {
      let request = Request.withTimeout (TimeSpan.FromMinutes(1))
      let toSend = request |> msg

      do! postMessageAsync mailboxOwner toSend

      return! request |> Request.asTask
    }
