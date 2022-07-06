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
open Ensemble.Topics
open FsUnit.CustomMatchers
open Ensemble.Tests.TestDoubles.Actors
open Ensemble.Tests.TestDoubles.Groups
open System
open System.Threading

module TopicTests =

  module TopicTokenTests =

    let inline compare a b (msg: byref<_>) help =
      if a = b then
        true
      else
        msg <- help a b
        false

    let rec equalToCompare a b (msg: byref<string>) =
      match a, b with
      | (EmptyTopic, EmptyTopic) -> true
      | (InvalidTopic, InvalidTopic) -> true
      | (Token (t1, next1), Token (t2, next2)) ->
        compare t1 t2 &msg (fun t1 t2 -> $"Token {t1} not equal to {t2}")
        && equalToCompare next1 next2 &msg
      | (TokenLast t1, TokenLast t2) -> t1 = t2
      | (WildcardToken next1, WildcardToken next2) -> equalToCompare next1 next2 &msg
      | (WildcardTokenLast, WildcardTokenLast) -> true
      | (FullWildcardToken, FullWildcardToken) -> true
      | _ ->
        msg <- $"Missmatching cases: %A{a} and %A{b}"
        false

    let equalTo a b =
      let mutable msg = ""
      let result = equalToCompare a b &msg

      if result then () else failwith msg

    [<Fact>]
    let ``should parse empty topic correctly`` () =
      let topic = ""

      let result = Topics.defaultParse topic

      result |> equalTo EmptyTopic

    [<Fact>]
    let ``should parse single token topic correctly`` () =
      let topic = "foo"

      let result = Topics.defaultParse topic

      result |> equalTo (TokenLast "foo")

    [<Fact>]
    let ``should parse multi token topic correctly`` () =
      let topic = "foo.bar.l"

      let result = Topics.defaultParse topic

      result
      |> equalTo (Token("foo", Token("bar", TokenLast "l")))

    [<Fact>]
    let ``should parse valid full wildcard topic correctly`` () =
      let topic = "foo.bar.>"

      let result = Topics.defaultParse topic

      result
      |> equalTo (Token("foo", Token("bar", FullWildcardToken)))

    [<Theory>]
    [<InlineData("foo.>.l")>]
    [<InlineData(">.bar.l")>]
    [<InlineData(">.>.l")>]
    [<InlineData(">.>")>]
    let ``should parse invalid full wildcard topic correctly`` topic =
      let result = Topics.defaultParse topic

      result |> equalTo InvalidTopic

    [<Fact>]
    let ``should parse valid single wildcard topic correctly`` () =
      let topic = "foo.*.l"

      let result = Topics.defaultParse topic

      result
      |> equalTo (Token("foo", WildcardToken(TokenLast "l")))

    [<Fact>]
    let ``should parse valid combined topic correctly`` () =
      let topic = "foo.*.l.>"

      let result = Topics.defaultParse topic

      result
      |> equalTo (Token("foo", (WildcardToken(Token("l", FullWildcardToken)))))

    [<Fact>]
    let ``should parse single wildcard topic correctly`` () =
      let topic = "*"

      let result = Topics.defaultParse topic

      result |> equalTo InvalidTopic

    [<Fact>]
    let ``should parse single full wildcard topic correctly`` () =
      let topic = ">"

      let result = Topics.defaultParse topic

      result |> equalTo FullWildcardToken

    [<Theory>]
    [<InlineData("fo*o.bar")>]
    [<InlineData("fo>o.bar")>]
    [<InlineData("foo.b*r")>]
    [<InlineData("foo.b>r")>]
    [<InlineData("fo>o")>]
    [<InlineData("fo*o")>]
    [<InlineData("fö")>]
    [<InlineData("foo.lö")>]
    let ``should parse invalid token topic correctly`` topic =
      let result = Topics.defaultParse topic

      result |> equalTo InvalidTopic

  module TopicMatcherTests =

    [<Theory>]
    [<InlineData("foo.>", "foo.bar", true)>]
    [<InlineData("foo.>", "foo.bar.baz", true)>]
    [<InlineData(">", "foo", true)>]
    [<InlineData(">", "foo.bar", true)>]
    [<InlineData(">", "foo.bar.baz", true)>]
    [<InlineData("foo.*.baz", "foo.l.baz", true)>]
    [<InlineData("foo.bar.*", "foo.bar.l", true)>]
    let ``should compare topics correctly`` subTopic pubTopic expected =
      let sub = Topics.defaultParse subTopic
      let matcher = TopicMatcher.fromToken sub

      let actual = matcher.MatchAgainst(pubTopic)

      actual |> should equal expected

  module TopicMapperTests =

    type TestUnion<'a> =
      | Case1 of 'a
      | Case2 of int * string
      | EmptyCase

    [<Fact>]
    let ``should map du with parameter as expected`` () =
      let topic = Case1 "msg" |> TopicMapper.mapObj
      topic |> should equal "Case1"

    [<Fact>]
    let ``should map parameterless du expected`` () =
      let topic = EmptyCase |> TopicMapper.mapObj
      topic |> should equal "EmptyCase"

    [<Fact>]
    let ``should map expression with parameterized du as expected`` () =
      let topic =
        TopicMapper.namesFromExpr <@ Case1 "msg" @>
        |> Seq.toList

      topic |> should matchList [ "Case1" ]

    [<Fact>]
    let ``should map expression parameterless du as expected`` () =
      let topic =
        TopicMapper.namesFromExpr <@ EmptyCase @>
        |> Seq.toList

      topic |> should matchList [ "EmptyCase" ]

    [<Fact>]
    let ``should map expression with tupled du as expected`` () =
      let topic =
        TopicMapper.namesFromExpr <@ Case1, Case2, EmptyCase @>
        |> Seq.toList

      topic
      |> should equal [ "Case1"; "Case2"; "EmptyCase" ]

  module TopicRouterTests =

    let inline ct () =
      (new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        .Token

    let getState (group: IActorGroup<_, _, _>) ct =
      task {
        let request = Request.withCancellation ct
        do! group <!! GetState request
        return! request |> Request.asTask
      }

    let rec pollState (group: IActorGroup<_, _, _>) f (ct: CancellationToken) =
      async {
        let! state = getState group ct |> Async.AwaitTask

        ct.ThrowIfCancellationRequested()

        if f state then
          return state
        else
          return! pollState group f ct
      }

    [<Fact>]
    let ``should route message to subscribers only`` () =
      task {
        use system = ActorSystem.withDefaults ()
        let msg = $"{Guid.NewGuid():N}"
        let ct = ct ()

        let a = TestActor.create ()
        let b = TestActor.create ()

        use groupInstance =
          groupWith (Routers.topic ())
          |= add a (Sub.topic "actor.a") TestState.Default
          |= add b (Sub.topic "actor.b") TestState.Default
          |> build
          |> run system ct

        groupInstance <<! ("actor.a", PostMsg msg)
        groupInstance.Complete()

        let! result = groupInstance.Completion

        match result with
        | Error e -> failwith e
        | Ok (stateA, stateB) ->
          stateA.Messages |> should matchList [ msg ]
          stateB.Messages |> should haveLength 0
      }

    [<Fact>]
    let ``should route message to all subscribers`` () =
      task {
        use system = ActorSystem.withDefaults ()
        let msg = $"{Guid.NewGuid():N}"
        let ct = ct ()

        let a = TestActor.create ()
        let b = TestActor.create ()
        let c = TestActor.create ()

        use groupInstance =
          groupWith (Routers.topic ())
          |= add a (Sub.topic "actor.*") TestState.Default
          |= add b (Sub.topic "actor.*") TestState.Default
          |= add c (Sub.topic "actor.*") TestState.Default
          |> build
          |> run system ct

        groupInstance <<! ("actor.a", PostMsg msg)
        groupInstance.Complete()

        let! result = groupInstance.Completion

        match result with
        | Error e -> failwith e
        | Ok ((stateA, stateB), stateC) ->
          stateA.Messages |> should matchList [ msg ]
          stateB.Messages |> should matchList [ msg ]
          stateC.Messages |> should matchList [ msg ]
      }

    [<Fact>]
    let ``should send messages created from actor to others`` () =
      task {
        use system = ActorSystem.withDefaults ()
        let id = Guid.NewGuid().ToString()
        let ct = ct ()

        let recorder = TestActor.create ()
        let generator = TestActor.create ()

        use groupInstance =
          groupWith (Routers.topic ())
          |= add recorder (Sub.messages <@ PostMsg, GetState @>) TestState.Default
          |= add generator (Sub.topic "gen") TestState.Default
          |> build
          |> run system ct

        groupInstance <<! ("gen", PostNewMsg(PostMsg id))

        let! final = pollState groupInstance (fun x -> x.Messages |> List.contains id) ct

        final.Messages |> should haveLength 1
        final.Messages |> should contain id
      }
