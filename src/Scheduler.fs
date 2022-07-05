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

type IMessageScheduler =

  abstract member ScheduleDelivery<'Msg> : PostDeliverySchedule<'Msg> -> IScheduledDelivery
  abstract member ScheduleDeliveries<'Msg> : PostDeliverySchedule<'Msg> seq -> IScheduledDelivery list

and PostDeliverySchedule<'Msg> =
  { Post: PostDelivery<'Msg>
    IsRecurring: bool
    Interval: TimeSpan }

and PostDelivery<'Msg> =
  { Message: 'Msg
    Outbox: IOutbox<'Msg> }

and IScheduledDelivery =

  abstract member Cancel: unit -> unit

module MessageScheduler =

  open TimingWheelScheduler

  module Helpers =

    let inline scheduleDelivery (scheduler: IScheduler) (schedule: PostDeliverySchedule<'Msg>) =
      let runSchedule =
        if schedule.IsRecurring then
          RunRepeateadly schedule.Interval
        else
          RunOnce schedule.Interval

      let timer =
        scheduler.Schedule(
          runSchedule,
          schedule.Post,
          fun state ->
            let delivery = state :?> PostDelivery<'Msg>
            delivery.Outbox <! delivery.Message
        )

      { new IScheduledDelivery with
          override __.Cancel() = timer.Cancel() }

  open Helpers

  let inline timingWheel interval wheelSize =
    let scheduler =
      TimingWheelScheduler.create interval wheelSize
      |> Scheduler.start CancellationToken.None

    { new IMessageScheduler with

        override __.ScheduleDelivery<'Msg>(schedule: PostDeliverySchedule<'Msg>) = scheduleDelivery scheduler schedule

        override __.ScheduleDeliveries<'Msg>(schedules: PostDeliverySchedule<'Msg> seq) =
          let map = scheduleDelivery scheduler
          schedules |> Seq.map map |> Seq.toList }

  let inline withDefaults () =
    timingWheel (TimeSpan.FromMilliseconds(50)) 2048
