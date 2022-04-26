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
open FsUnit.CustomMatchers
open System.Threading

module RequestTests =

  open System

  let inline test f (Request tcs) = f tcs.Task

  [<Fact>]
  let ``should cancel request when token is cancelled`` () =
    use cts = new CancellationTokenSource()

    let request = Request.withCancellation cts.Token
    cts.Cancel()

    let isCancelled = request |> test (fun t -> t.IsCanceled)
    isCancelled |> should equal true

  [<Fact>]
  let ``should cancel request when timeout occur`` () =
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds(10))
    let request = Request.withTimeout (TimeSpan.FromMilliseconds(1))

    let mutable isCancelled = false

    while not cts.Token.IsCancellationRequested
          && not isCancelled do
      isCancelled <- request |> test (fun t -> t.IsCanceled)

    isCancelled |> should equal true
