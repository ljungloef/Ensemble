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

  abstract member ScheduleDelivery<'Msg> : DeliveryInstruction<'Msg> * IOutbox<'Msg> -> unit
  abstract member ScheduleDeliveries<'Msg> : DeliveryInstruction<'Msg> seq * IOutbox<'Msg> -> unit

and DeliveryInstruction<'Msg> =
  {
    /// The message to be sent when the `interval` expire.
    Message: 'Msg

    /// Determines if the message should be sent again.
    IsRecurring: bool

    /// The time to wait before the message should be sent. The `Interval` is also used to determine when
    /// the message should be sent again if it is a `IsRecurring` message.
    Interval: TimeSpan

    /// Cancellation token that can be used to signal if the delivery should be cancelled
    Cancellable: CancellationToken voption }

and DeliveryInstruction =

  /// Create a new instruction that will trigger a delivery once the `delay` has passed.
  static member inline Once(message, delay) =
    { Message = message
      IsRecurring = false
      Interval = delay
      Cancellable = ValueNone }

  /// Create a new cancellable instruction that will trigger a delivery once the `delay` has passed.
  static member inline Once(message, delay, canellable) =
    { Message = message
      IsRecurring = false
      Interval = delay
      Cancellable = ValueSome canellable }

  /// Create a new cancellable instruction that will trigger a delivery every `interval`.
  static member inline Recurring(message, interval, canellable) =
    { Message = message
      IsRecurring = true
      Interval = interval
      Cancellable = ValueSome canellable }

  /// Create a new instruction that will trigger a delivery every `interval`.
  static member inline Recurring(message, interval) =
    { Message = message
      IsRecurring = true
      Interval = interval
      Cancellable = ValueNone }

and IScheduledDelivery =

  abstract member Cancel: unit -> unit

module MessageScheduler =

  open TimingWheelScheduler

  module Helpers =

    type PostDeliveryState<'Msg> =
      { Message: 'Msg
        Outbox: IOutbox<'Msg> }

    let inline scheduleDeliveryCore
      (scheduler: IScheduler)
      (target: IOutbox<'Msg>)
      (schedule: DeliveryInstruction<'Msg>)
      =
      let runSchedule =
        if schedule.IsRecurring then
          RunRepeateadly schedule.Interval
        else
          RunOnce schedule.Interval

      scheduler.Schedule(
        runSchedule,
        { Message = schedule.Message
          Outbox = target },
        fun state ->
          let delivery = state :?> PostDeliveryState<'Msg>
          delivery.Outbox <! delivery.Message
      )

    let inline scheduleDelivery scheduler target schedule =
      match schedule.Cancellable with
      | ValueSome token when token.IsCancellationRequested -> ()
      | ValueSome token ->
        let timer = scheduleDeliveryCore scheduler target schedule

        token.Register(
          (fun (state: obj) ->
            let timer = state :?> ITimer
            timer.Cancel()),
          timer
        )
        |> ignore
      | _ ->
        scheduleDeliveryCore scheduler target schedule
        |> ignore

  open Helpers

  let inline timingWheel interval wheelSize =
    let scheduler =
      TimingWheelScheduler.create interval wheelSize
      |> Scheduler.start CancellationToken.None

    { new IMessageScheduler with

        override __.ScheduleDelivery<'Msg>(schedule: DeliveryInstruction<'Msg>, target: IOutbox<'Msg>) =
          scheduleDelivery scheduler target schedule

        override __.ScheduleDeliveries<'Msg>(schedules: DeliveryInstruction<'Msg> seq, target: IOutbox<'Msg>) =
          let iter = scheduleDelivery scheduler target
          schedules |> Seq.iter iter }

  let inline withDefaults () =
    timingWheel (TimeSpan.FromMilliseconds(50)) 2048
