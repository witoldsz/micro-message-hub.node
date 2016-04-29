import {parse} from './content_types'

export function Queue({log, conn, queueName, exchangeName, prefetchCount, queueOptions, msgOptions}) {
  let channel;
  const bindings = [];

  this.bind = (routingKey, callback) => {
    log.emit('binding', {queueName, routingKey});
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
      return log.emit('warn', {queueName}, `Consumer has been canceled`);
    }
    Promise.resolve()
      .then(() => {
        const routingKey = msg.fields.routingKey;
        const deliveryTag = msg.fields.deliveryTag;
        const accepted = bindings.filter(b => b.routingKeyPattern.test(routingKey));
        const trace = msg.properties.headers.trace || [];
        const load = parse(msg);

        log.emit('incoming',
          Object.assign({queueName, routingKey, load}, deliveryTag > 1 ? {deliveryTag} : null),
          trace);

        if (accepted.length < 1) {
          log.emit('warn', {queueName, routingKey}, 'No listener');
        }
        return Promise.all(accepted.map(b => b.callback(load, trace, msg)));
      })
      .then(
        results => msgOptions.onAck(channel, msg, results),
        err => {log.emit('error', err); msgOptions.onNack(channel, msg);}
      );
  }
}
