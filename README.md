[![Build Status](https://travis-ci.org/witoldsz/micro-message-hub.node.svg?branch=master)](https://travis-ci.org/witoldsz/micro-message-hub.node) [![npm](https://img.shields.io/npm/v/micro-message-hub.svg?maxAge=2592000)](https://www.npmjs.com/package/micro-message-hub)

Micro Message Hub
===
Pragmatic and opinionated messaging solution for your (micro) services, backed by AMQP.

Why should you be interested?
---
Micro message hub is not just a library, it's also the architecture skeleton for your application.

**As a library** it offers you a nice API, hiding AMQP complexity, exposing just the fine grained selection
of features. Don't forget there is also `fake-mmq.js`, so you can easily write your tests with *mmq* double
and execute them in isolation.

**As an architecture**, it offers a battle tested, hybrid (sync/async), communication scheme for your modules, often written in
different languages. It's all based on AMQP (with RabbitMQ extension), so you can communicate with
other services having proper driver.

**More to come...**

You are welcome
---
If you have question, suggestion, improvement, fix, port to a different language or what-else,
please create an issue or pull request. Everyone interested will get notified.
