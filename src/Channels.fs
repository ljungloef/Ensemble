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
open System.Threading.Channels

module Channel =

  /// Create a new channel
  let inline create (opts: ChannelOptions) =
    match opts with
    | :? UnboundedChannelOptions as opts -> Channel.CreateUnbounded(opts)
    | :? BoundedChannelOptions as opts -> Channel.CreateBounded(opts)
    | _ -> raise (Exception("Unknown channel options type"))

  let inline unbounded () = Channel.CreateUnbounded()

/// Channel based mailbox implementation. Basically just a wrapper around the given `channel`.
type ChannelMailbox<'Msg>(channel: Channel<'Msg>) =

  member __.Channel = channel

  interface IMailbox<'Msg> with

    override __.CanCount = channel.Reader.CanCount
    override __.Count = channel.Reader.Count
    override __.TryRead(msg) = channel.Reader.TryRead(&msg)
    override __.WaitToReadAsync(ct) = channel.Reader.WaitToReadAsync(ct)
    override __.PostMessage(msg) = channel.Writer.TryWrite(msg)
    override __.PostMessageAsync(msg) = channel.Writer.WriteAsync(msg)

    override __.Complete(?error: exn) =
      channel.Writer.Complete(error |> Option.toObj)

module ChannelMailbox =

  /// Create a new mailbox that will use a new channel. The channel will be created using the provided options
  let inline fromOpts opts =
    opts |> Channel.create |> ChannelMailbox :> IMailbox<_>

  /// Create a new mailbox backed by the provided `channel`.
  let inline create channel =
    channel |> ChannelMailbox :> IMailbox<_>
