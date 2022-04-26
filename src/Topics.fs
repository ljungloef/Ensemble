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

namespace Ensemble.Topics

open System
open Ensemble

/// A type representation of a topic
type TopicToken =

  /// The topic is empty.
  | EmptyTopic

  /// The topic is invalid.
  | InvalidTopic

  /// A topic segment and a reference to the next token
  | Token of string * TopicToken

  /// An ending topic segment
  | TokenLast of string

  /// Wildcard token with a refernce to the next token
  | WildcardToken of TopicToken

  /// An ending wildcard
  | WildcardTokenLast

  /// A full wildcard token, always put in the end of the topic, as it means match anything after this token.
  | FullWildcardToken

module TopicToken =

  let inline ifInvalid f topic =
    match topic with
    | InvalidTopic ->
      f topic
      topic
    | _ -> topic


module Topics =

  module TopicHelpers =

    /// Returns `true` if the provided topic token is an ending wildcard
    let inline isLastWildcard topic =
      match topic with
      | WildcardTokenLast -> true
      | _ -> false

    /// Returns `true` if the provided topic token is an invalid topic.
    let inline isInvalid topic =
      match topic with
      | InvalidTopic -> true
      | _ -> false

    let inline toStr (span: ReadOnlySpan<char>) = span.ToString()

    let inline lastOrNext iter prev last ([<InlineIfLambda>] other) = if iter = 0 then last else other prev

    let inline lastOrNext1 iter prev arg ([<InlineIfLambda>] last) ([<InlineIfLambda>] other) =
      if iter = 0 then
        last arg
      else
        other (arg, prev)

    let inline validateToken (token: ReadOnlySpan<char>) singleWildcard fullWildcard =
      let mutable valid = true
      let mutable i = 0

      while valid && i < token.Length do
        let c = token[i]

        valid <-
          Char.IsAscii c
          && c <> singleWildcard
          && c <> fullWildcard

        i <- i + 1

      valid

    let inline parseToken (token: ReadOnlySpan<char>) singleWildcard fullWildcard iter topic =
      if token.Length = 1 && token[0] = singleWildcard then
        lastOrNext iter topic WildcardTokenLast WildcardToken
      elif token.Length = 1 && token[0] = fullWildcard then
        lastOrNext iter topic FullWildcardToken (fun _ -> InvalidTopic)
      elif not (validateToken token singleWildcard fullWildcard) then
        InvalidTopic
      else
        lastOrNext1 iter topic (toStr token) TokenLast Token

  open TopicHelpers

  let rec parseIter (span: ReadOnlySpan<char>) (sep: char) (singleWildcard: char) (fullWildcard: char) iter topic =
    if span.Length = 0 then
      if iter = 0 then
        topic // EmptyTopic (topic is set to EmptyTopic the first iter)
      elif iter = 1 && isLastWildcard topic then
        InvalidTopic
      else
        topic
    elif isInvalid topic then
      topic
    else
      let next = span.LastIndexOf(sep)

      let curr =
        if next = -1 then
          span
        else
          span.Slice(next + 1)

      let next =
        if next = -1 then
          ReadOnlySpan.Empty
        else
          span.Slice(0, next)

      let structure = parseToken curr singleWildcard fullWildcard iter topic
      parseIter next sep singleWildcard fullWildcard (iter + 1) structure

  /// Parse a topic, and use the supplied `sep` as seperator char, `singleWildcard` as single segment wildcard char and `fullWildcard` as the full wildcard
  /// char.
  let inline parse (sep: char) (singleWildcard: char) (fullWildcard: char) (topic: string) =
    parseIter (topic.AsSpan()) sep singleWildcard fullWildcard 0 EmptyTopic

  /// Parse a topic using `.` as the seperating char, `*` as a single wildcard token and `>` as the full wildcard token.
  let defaultParse = parse '.' '*' '>'

type ITopicMatcher =

  abstract member MatchAgainst: string -> bool

module TopicMatcher =

  open System.Collections.Generic

  module TopicHelpers =

    let inline add item (arr: ResizeArray<_>) =
      arr.Add(item)
      arr

    let rec buildMatcher token (ranges: ResizeArray<_>) =
      match token with
      | EmptyTopic
      | InvalidTopic -> failwith "Cannot create a topic matcher from an empty or invalid topic"
      | Token (token, next) ->
        buildMatcher
          next
          (ranges
           |> add (fun t -> if t = token then 0 else -1))
      | WildcardToken next -> buildMatcher next (ranges |> add (fun _ -> 0))
      | TokenLast token ->
        (false,
         ranges
         |> add (fun t -> if t = token then 0 else -1))
      | WildcardTokenLast -> (false, ranges |> add (fun _ -> 0))
      | FullWildcardToken -> (true, ranges)

  let rec compare (topic: ReadOnlySpan<char>) (comparers: Queue<(string -> int)>) =
    let succ, comparer = comparers.TryDequeue()

    if not succ then
      topic.Length
    else
      let next = topic.IndexOf('.')

      if next = -1 then
        if comparers.Count > 0 then
          -1 // Topic is too short
        else
          comparer (topic.ToString())
      else
        let segment = topic.Slice(0, next)
        let result = comparer (segment.ToString())

        if result = 0 then
          compare (topic.Slice(next + 1)) comparers
        else
          result

  let inline createMatcher (comparers: ResizeArray<_>) isFullWildcard =
    { new ITopicMatcher with
        override __.MatchAgainst(topic) =
          let span = topic.AsSpan()
          let result = compare span (Queue(comparers))

          if isFullWildcard then
            result >= 0
          else
            result = 0 }

  /// Create a new topic matcher from a parsed topic
  let inline fromToken token =
    let isFullWildcard, comparers = TopicHelpers.buildMatcher token (ResizeArray())
    createMatcher comparers isFullWildcard

  /// Create a new topic matcher from the provided topic. This will raise an exception
  /// if the provided topic is invalid.
  let fromStringReq =
    Topics.defaultParse
    >> TopicToken.ifInvalid (fun t -> failwith $"Invalid topic: %A{t}")
    >> fromToken

  /// Check whether either `a` or `b` matches the given topic.
  let combine (a: ITopicMatcher) (b: ITopicMatcher) =
    { new ITopicMatcher with
        override __.MatchAgainst(topic) =
          a.MatchAgainst(topic) || b.MatchAgainst(topic) }

/// A topic convention is used to generate new topics based on different structures.
type ITopicConvention =
  abstract member MapName: string -> string
  abstract member MapMessage: 'Msg -> string
  abstract member MapType: Type -> string

module TopicMapper =

  open Microsoft.FSharp.Reflection
  open Microsoft.FSharp.Quotations.Patterns
  open System.Reflection

  let allMembers =
    BindingFlags.Public
    ||| BindingFlags.NonPublic
    ||| BindingFlags.Static
    ||| BindingFlags.Instance
    ||| BindingFlags.FlattenHierarchy

  let inline prefixed prefix t =
    match prefix with
    | Some prefix -> $"{prefix}.{t}"
    | _ -> t

  let inline namesFromExpr (expr: Quotations.Expr) =
    let rec find (expr: Quotations.Expr) =
      seq {
        match expr with
        | Lambda (_, expr)
        | Let (_, _, expr) -> yield! find expr
        | NewUnionCase (caseInfo, _) -> yield Some caseInfo.Name
        | NewTuple expressions -> yield! expressions |> Seq.collect find
        | _ -> yield None
      }

    find expr |> Seq.choose id

  let inline unionObjCaseName obj t =
    let info, _ = FSharpValue.GetUnionFields(obj, t)
    info.Name

  let inline mapType (t: Type) = t.Name

  let inline mapObj (o: obj) =
    let t = o.GetType()

    if FSharpType.IsUnion(t, allMembers) then
      unionObjCaseName o t
    else
      mapType t

  let inline defaultConvention prefix =
    match prefix with
    | Some prefix ->
      { new ITopicConvention with
          override __.MapName(n) = prefixed prefix n
          override __.MapType(t) = mapType t |> prefixed prefix
          override __.MapMessage(msg) = mapObj msg |> prefixed prefix }
    | None ->
      { new ITopicConvention with
          override __.MapName(n) = n
          override __.MapType(t) = mapType t
          override __.MapMessage(msg) = mapObj msg }

type SubscribeOpts =
  | SubscribeToEverything
  | SubscribeToTopic of string
  | SubscribeToTopics of string list
  | SubscribeToName of string
  | SubscribeToNames of string list
  | SubscribeToMessage of Type
  | SubscribeToMessages of Type list

type TopicRouterOptions =
  { TopicConvention: ITopicConvention }
  static member Default = { TopicConvention = TopicMapper.defaultConvention None }

[<RequireQualifiedAccess>]
module Routers =

  /// Create a new topic router using the supplied options
  let inline topicOpts (opts: TopicRouterOptions) =
    { new IMessageRouter<_, _> with
        override __.GenerateRouteFromName(n) = opts.TopicConvention.MapName(n)
        override __.GenerateRouteFromType(t) = opts.TopicConvention.MapType(t)
        override __.GenerateRoute(msg) = opts.TopicConvention.MapMessage(msg) }

  /// Create a new topic router using the default options
  let inline topic () = topicOpts TopicRouterOptions.Default

[<RequireQualifiedAccess>]
module Sub =

  let inline topicMatcher (opts: SubscribeOpts) =
    fun (groupOpts: GroupOpts<_, _>) ->
      let fromType =
        groupOpts.Router.GenerateRouteFromType
        >> TopicMatcher.fromStringReq

      let fromName =
        groupOpts.Router.GenerateRouteFromName
        >> TopicMatcher.fromStringReq

      let matcher =
        match opts with
        | SubscribeToEverything
        | SubscribeToMessages []
        | SubscribeToNames []
        | SubscribeToTopics [] -> TopicMatcher.fromStringReq ">"
        | SubscribeToTopic topic
        | SubscribeToTopics [ topic ] -> TopicMatcher.fromStringReq topic
        | SubscribeToName name
        | SubscribeToNames [ name ] -> name |> fromName
        | SubscribeToMessage t
        | SubscribeToMessages [ t ] -> t |> fromType
        | SubscribeToTopics (f :: rest) ->
          rest
          |> List.fold
               (fun agg next ->
                 next
                 |> TopicMatcher.fromStringReq
                 |> TopicMatcher.combine agg)
               (f |> TopicMatcher.fromStringReq)
        | SubscribeToMessages (f :: rest) ->
          rest
          |> List.fold (fun agg next -> next |> fromType |> TopicMatcher.combine agg) (f |> fromType)
        | SubscribeToNames (f :: rest) ->
          rest
          |> List.fold (fun agg next -> next |> fromName |> TopicMatcher.combine agg) (f |> fromName)

      { new IRouteMatcher<_, _> with
          override __.MatchAgainst(msg) = matcher.MatchAgainst(msg.Route) }

  /// Subscribe to a specific topic
  let inline topic (topic: string) = topicMatcher (SubscribeToTopic topic)

  /// Subscribe to the messages in the expression.
  /// The expression is mainly used for parameterized unions and should be in the form <@ GetState, PutState @>
  let inline messages (expr: Quotations.Expr) =
    let topics = TopicMapper.namesFromExpr expr |> Seq.toList

    match topics with
    | [] -> failwith "Could not extract any topics from the provided code quation"
    | [ topic ] -> topicMatcher (SubscribeToTopic topic)
    | topics -> topicMatcher (SubscribeToTopics topics)
