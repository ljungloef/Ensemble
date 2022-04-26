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

/// Simple wrapper around TaskCompletionSource{} designed to be used to make a request
/// to an actor. The actor can respond by completing the request.
type Request<'T> = Request of TaskCompletionSource<'T>

module Request =

  /// Create a new request that will try to cancel the request when the token is cancelled
  let inline withCancellation (token: CancellationToken) =
    let tcs = TaskCompletionSource<_>()

    let reg = token.Register(fun () -> tcs.TrySetCanceled() |> ignore)

    tcs.Task.ContinueWith(fun (t: Task) -> reg.Dispose())
    |> ignore

    Request(tcs)

  /// Create a new request that will try to cancel the request after `timeout` amount of time
  let withTimeout (timeout: TimeSpan) =
    let cts = new CancellationTokenSource(timeout)
    cts.CancelAfter(timeout)
    cts.Token |> withCancellation

  /// Get the underlying task that can be used to await the request completion
  let asTask (Request tcs) = tcs.Task

  /// Try to set the result of the request
  let respond response (Request tcs) = response |> tcs.TrySetResult

  /// Try to mark the request as failed
  let fail (msg: string) (Request tcs) = Exception(msg) |> tcs.TrySetException

  /// Get whether the given request has run to completion (not necessarily successful)
  let isCompleted (Request tcs) = tcs.Task.IsCompleted

  /// Wait for the reqest to complete, and return the result as a `Result<_,string>`
  let toResult success (Request tcs) =
    task {
      try
        let! result = tcs.Task
        return success result
      with
      | e -> return Error e.Message
    }
