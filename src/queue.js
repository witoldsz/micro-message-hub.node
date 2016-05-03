import {parse} from './content_types'
import {QueueBindings} from './queue-bindings'

export function Queue({log, conn, queueName, exchangeName, prefetchCount, queueOptions, msgOptions}) {
  const queueBindings = new QueueBindings();

  this.bind = (routingKey, callback) => {
    log.emit('binding', {queueName, routingKey});
    queueBindings.bind(routingKey, callback);
    return this;
  };

  this._ready = () => {
    return conn
      .createChannel()
      .then(channel => {
        channel.prefetch(prefetchCount);
        return channel
          .assertQueue(queueName, queueOptions)
          .then(() => Promise.all(
            queueBindings.routingKeys.map(routingKey => channel.bindQueue(queueName, exchangeName, routingKey))
          ))
          .then(() => channel.consume(queueName, msgHandler(channel), {noAck: msgOptions.noAck}))
      })
  };

  const msgHandler = (channel) => (msg) => {
    if (msg === null) {
      return log.emit('warn', {queueName}, `Consumer has been canceled`);
    }

    Promise.resolve()
      .then(() => {
        const routingKey = msg.fields.routingKey;
        const deliveryTag = msg.fields.deliveryTag;
        const trace = msg.properties.headers.trace || [];
        const load = parse(msg);

        const logEvent = Object.assign({queueName, routingKey, load}, deliveryTag > 1 ? {deliveryTag} : null);
        log.emit('incoming', logEvent, trace);

        const bindingsEvent = {msg, routingKey, trace, load};
        return new Promise((resolve, reject) => queueBindings.emit('incoming', bindingsEvent, resolve, reject))
      })
      .then(
        results => {
          if (results.length < 1) {
            log.emit('warn', {queueName, routingKey}, 'There were no listeners');
          }
          msgOptions.onAck(channel, msg, results)
        },
        err => {
          log.emit('error', err); msgOptions.onNack(channel, msg);
        }
      );
  }
}
