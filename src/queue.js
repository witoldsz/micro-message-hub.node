import {parserOf} from './content_types'

export function Queue({conn, queueName, exchangeName, prefetchCount, queueOptions, msgOptions}) {
  let channel;
  const bindings = [];

  this.bind = (routingKey, callback) => {
    console.log(`Binding queue [${queueName}] to <${routingKey}>`);
    bindings.push({
      routingKey,
      callback,
      routingKeyPattern: new RegExp(routingKey.replace('.', '[.]').replace('[.]#', '([.].*)?'))});
    return this;
  };

  this._ready = () => {
    return conn.createChannel()
      .then(channel_ => {
        channel = channel_;
        channel.prefetch(prefetchCount);
        return channel.assertQueue(queueName, queueOptions)
      })
      .then(() => Promise.all(bindings.map(b => channel.bindQueue(queueName, exchangeName, b.routingKey))))
      .then(() => channel.consume(queueName, msgHandler, {noAck: msgOptions.noAck}))
  };

  const msgHandler = (msg) => {
    if (msg === null) {
      return console.warn(`Consumer of queue [${queueName}] has been canceled`);
    }
    Promise.resolve()
      .then(() => {

        const routingKey = msg.fields.routingKey;
        const accepted = bindings.filter(b => b.routingKeyPattern.test(routingKey));

        if (accepted.length < 1) {
          console.log(`No listener for <${routingKey}> on queue [${queueName}]`);
        }

        const parser = parserOf(msg);
        const trace = msg.properties.headers.trace || [];

        return Promise.all(accepted.map(b => b.callback(parser(msg), trace, msg)))
      })
      .then(
        results => msgOptions.onAck(channel, msg, results),
        err => {console.error(err.stack || err); msgOptions.onNack(channel, msg)})
    ;
  }
}
