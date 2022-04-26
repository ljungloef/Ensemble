# Ensemble

**Ensemble** is a lightweight, in-process messaging/actor framework to tackle the complexity when building concurrent, event driven applications. The library was designed with network applications in mind, but suits well in many other use cases as well (e.g. GUI apps, state stores, state machines).

The framework is not a replacement to what other actor frameworks such as Akka.net, Microsoft Orleans brings. Ensemble lacks actor-to-actor communication, clusters, network communication, to name a few. Ensemble is more ment to be a tool, or approach, to handle concurrency, highly inspired from the Actor Model. Ensemble is semantically more like the `MailboxProcessor` but (according to some early tests) more performant and also inclues some additional features. The key features of ensemble are:

- High performance (mostly thanks to `System.Threading.Channels`)
- Switchable behavior
- Supervision
- Actor groups
- PubSub communication
- State and Completion

NOTE: The library is in an early development phase and not yet production ready.

## License

[Apache 2.0](https://raw.githubusercontent.com/ljungloef/Ensemble/main/LICENSE)