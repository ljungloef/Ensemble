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

namespace Ensemble.Samples

open System
open System.Threading
open System.Threading.Tasks

module Program =

  let inline runTarget (target: string) ct =
    match target.ToLowerInvariant() with
    | "ms" -> MessageScheduling.run ct
    | "pc" -> ProducerConsumer.run ct
    | "sb" -> SwitchableBehavior.run ct
    | _ -> ProducerConsumer.run ct

  let inline run ct (args: string list) =
    match args with
    | [ target ]
    | target :: _ -> runTarget target ct
    | _ -> runTarget "pc" ct

  [<EntryPoint>]
  let main args =
    try
      use ct = new CancellationTokenSource()

      Console.CancelKeyPress
      |> Event.add (fun _ -> ct.Cancel())

      let task = args |> List.ofArray |> run ct.Token
      Task.Run<unit>(Func<unit>(fun () -> task.Wait())).Wait()

      0
    with
    | e -> raise (Exception("Error when running sample. See inner exception for details.", e))
