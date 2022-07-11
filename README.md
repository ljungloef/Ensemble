# Ensemble

**Ensemble** is a lightweight, in-process messaging/actor framework to handle the complexity when building concurrent, event driven applications. The library was designed with network applications in mind, but suits well in many other use cases as well (e.g. GUI apps, state stores, state machines).

The framework is not a replacement to what other actor frameworks such as Akka.net, Microsoft Orleans brings. Ensemble lacks actor-to-actor communication, clusters, network communication, to name a few. Ensemble is more ment to be a tool, or approach, to handle concurrency, highly inspired from the Actor Model. Ensemble is semantically more like the `MailboxProcessor` but (according to some early tests) more performant and also inclues some additional features. The key features of ensemble are:

- High performance (mostly thanks to `System.Threading.Channels`)
- Switchable behavior
- Supervision
- Actor groups
- PubSub communication
- State and Completion
- Message scheduling (using the hashed timing wheel scheduler from [TimingWheelScheduler-dotnet](https://github.com/ljungloef/TimingWheelScheduler-dotnet))

NOTE: The library is in a development phase and not considered stable.

## Walkthrough

### 1. Define messages
The message type is shared by all in a group and can be any type, but works very well with discriminated unions:

```fsharp
type Msg =
  | Connect
  | Ping
  | Pong
  | Disconnect
  | LastWill of string
```

### 2. Define an actor
An Ensemble actor is very similar to the built-in `MailboxProcessor`, and consist of a state object, an inbox and an outbox.

#### 2a. Define state
```fsharp
  type AppState =
    { ConnectTime: DateTimeOffset option
      LastPingSent: DateTimeOffset option
      LastPingTime: DateTimeOffset option
      DisconnectTime: DateTimeOffset option }
    static member Default() =
      { ConnectTime = None
        LastPingSent = None
        LastPingTime = None
        DisconnectTime = None }
```
#### 2b. Define message handler
A `message handler` in Ensemble is the main place for adding functionality and behavior. The responsibility of the message handler is to react to a specific message based on an incoming `state`. The handler itself should not store any local state, it should be pure and only base its decision on incoming state and any state mutations is propogated back through the `set` instruction.

```fsharp
/// Defines the handler to use when in the disconnected state.
let rec disconnected state =
  function
  | Connect ->

    // Move to the connected state by switching to the
    // connected handler
    become connected

    // Update the state with some additional info
    <&> set { state with ConnectTime = Some DateTimeOffset.UtcNow }

  | _ ->

    // Ignore all other messages when we are in the disconnected state.
    success ()

/// Defines the handler to use when in the disconnected state.
and connected state =
  function
  | Ping ->

    post Pong
    <&> set { state with LastPingSent = Some DateTimeOffset.UtcNow }

  | Pong ->

    set { state with LastPingTime = Some DateTimeOffset.UtcNow }

  | Disconnect ->

    become disconnected
    <&> post (LastWill "bye")
    <&> set { state with DisconnectTime = Some DateTimeOffset.UtcNow }

  | _ -> success ()
```

The return operations of handlers can be combined using `<&>`. For example, to both set the new state and post a new message, use: `set { state with LastPingSent = Some DateTimeOffset.UtcNow } <&> post Pong`

The available return operations are:
- `success` - used to signal back that the message is handled but the message did not yield any change.
- `set` - set the new state
- `post` - the message resulted in a new message being generated
- `postMany` - the message resulted in many new messages being generated
- `postLater` - the message resulted in a future message
- `abort` - the combination of `state` and the incoming `message` are in a bad state, and the operation should be aborted.
- `stop` - the combination of `state` and the incoming `message` are in a bad state, and is advised to not continue after the operation is completed.
- `become` - switch to another handler once the return operation completes. The next message for the same actor will be served to the new handler.

#### 2c. Create the actor
An actor is created using `Actors.create handler`:

```fsharp
// Start in the disconnected state
let app = Actors.create disconnected
```

### 3. Define the actor group
`Ensemble` is all about gathering up and solving problems as a unit. When an `actor` is added to a group it is lifted to a group actor and gains:

- Supervision - A group actor's lifecycle state is monitored by the group host, and a configured supervision strategy is applied when an actor faults, is stopped or aborted.
- Communication - Any messages that are outputted from the handler via its `outbox` (usually from a post* handler operation) are forwarded to others in the group. To whom messages are forwarded are decided by an `IMessageRouter`. The default router is using PubSub through the `Routers.topic ()`.

```fsharp
task {
  let app = Actor.create disconnected

  // The actor system contains functionality that is shared across groups
  use system = ActorSystem.withDefaults ()

  use groupInstance =

    // Create the group using the built-in, topic-based, message routing.
    groupWith (Routers.topic ())

    // Add the actor defined in (2) to the group, and forward all group messages to the actor.
    |= add app (Sub.topic ">") AppState.Default
    |> build
    |> run system ct

  // Send the `Connect` message to the group. The connect message
  // will be forwarded to the `app` actor since that actor subscribes to all
  // messages
  groupInstance <! Connect

  let! finalResult = groupInstance.Completion

  match result with
  | Ok finalState -> printfn $"Result is %A{finalState}"
  | Error e -> printfn $"Error when running sample: %A{e}"

  return ()
}
```

The group will run until it completes. Completion occurs when either (1) all actors stop or complete, and supervision is configured to not restart those actors, or (2) when `groupInstance.Complete()` is called and all remaining messages have been handled.

## License

[Apache 2.0](https://raw.githubusercontent.com/ljungloef/Ensemble/main/LICENSE)