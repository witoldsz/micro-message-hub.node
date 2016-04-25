import amqp from 'amqplib';
import uuid  from 'uuid';

const DEFAULT_URL = '';
const DEFAULT_EVENT_EXCHANGE = 'amq.topic';
const DEFAULT_QUERY_EXCHANGE = 'amq.topic';
const DEFAULT_QUERY_TIMEOUT = 3000;

const DEFAULT_CONTENT_TYPE = 'application/json';
const SYMBOL_CONTENT_TYPE = Symbol.for('content-type');

const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to';

function MicroMessageQueues({
    moduleName, url = DEFAULT_URL, socketOptions, conn,
    options: {
      eventExchangeName = DEFAULT_EVENT_EXCHANGE,
      queryExchangeName = DEFAULT_QUERY_EXCHANGE,
      queryTimeout = DEFAULT_QUERY_TIMEOUT
    } = {}
  }) {

  const queues = [];
  const queryResponseListeners = new Map();
  let eventPublishChannel, queryChannel;

  this.close = () => {
    return conn.close();
  };

  this.connect = () => {
    return Promise.resolve()
      .then(() => conn || amqp.connect(url, socketOptions))
      .then(conn_ => {
        conn = conn_;
        return Promise.all([conn.createConfirmChannel(), conn.createChannel()])
      })
      .then(([confirmChannel, nonConfirmChannel]) => {
        eventPublishChannel = confirmChannel;
        queryChannel = nonConfirmChannel;
        return queryChannel.assertQueue(moduleName, {exclusive: true});
      })
      .then(() => {
        const queryResponseHandler = msg => {
          const correlationId = msg.properties.correlationId;
          const resolve = queryResponseListeners.get(correlationId);
          if (resolve) {
            queryResponseListeners.delete(correlationId);
            resolve({
              msg,
              correlationId,
              contentType: msg.properties.contentType,
              trace: msg.properties.headers.trace,
              body: parserOf(msg)(msg)
            });
          }
        };
        return queryChannel.consume(DIRECT_REPLY_QUEUE, queryResponseHandler, {noAck: true});
      })
      ;
  };

  this.ready = () => {
    return Promise
      .all(queues.map(q => q._ready()))
      .then(() => {
        const pingResponseHandler = msg => {
          queryChannel.sendToQueue(msg.properties.replyTo, new Buffer(moduleName), {
            persistent: false,
            contentType: 'text/plain'
          });
        };
        return queryChannel.consume(moduleName, pingResponseHandler, {noAck: true});
      });
  };

  this.eventQueue = (name = 'events', {prefetchCount = 1} = {}) => {
    const q = new Queue({
      queueName: moduleName + ':' + name,
      exchangeName: eventExchangeName,
      prefetchCount,
      queueOptions: {
        exclusive: false,
        durable: true,
        autoDelete: false
      },
      msgOptions: {
        noAck: false,
        onAck: (channel, msg) => channel.ack(msg),
        onNack: (channel, msg) => channel.nack(msg)
      }
    });
    queues.push(q);
    return q;
  };

  this.queryQueue = (name = 'queries', {prefetchCount = 0} = {}) => {
    const q = new Queue({
      queueName: moduleName + ':' + name,
      exchangeName: queryExchangeName,
      prefetchCount,
      queueOptions: {
        exclusive: false,
        durable: false,
        autoDelete: true
      },
      msgOptions: {
        noAck: true,
        onAck: (channel, msg, results) => {
          try {
            const p = msg.properties;
            results.forEach(payload => {
              const options = publishOptions(p.headers.trace, payload, {
                persistent: false,
                correlationId: p.correlationId,
                contentType: payload[SYMBOL_CONTENT_TYPE] || DEFAULT_CONTENT_TYPE
              });
              channel.sendToQueue(p.replyTo, bufferOf(payload), options)
            });
          } catch (err) {
            console.error(err.stack || err);
          }
        },
        onNack: () => {}
      }
    });
    queues.push(q);
    return q;
  };

  this.publish = (routingKey, payload, trace = [], options = {}) => {
    const isQuery = routingKey.startsWith('query.');
    options = publishOptions(trace, payload, options, {isQuery});
    const newTrace = options.headers.trace;
    const newTracePoint = newTrace[newTrace.length - 1];
    const buffer = bufferOf(payload);
    return isQuery
      ? publishQueryPromise(routingKey, buffer, newTrace, newTracePoint, options)
      : publishEventPromise(routingKey, buffer, newTrace, newTracePoint, options);
  };

  const publishQueryPromise = (routingKey, buffer, trace, tracePoint, options) => new Promise((resolve, reject) => {
    const correlationId = options.correlationId;
    const rejectBackup = () => {
      queryResponseListeners.delete(correlationId) && reject(new Error(`Timeout on <${routingKey}>`));
    };
    queryResponseListeners.set(correlationId, resolve);
    setTimeout(rejectBackup, queryTimeout);
    queryChannel.publish(queryExchangeName, routingKey, buffer, options);
  });

  const publishEventPromise = (routingKey, buffer, trace, tracePoint, options) => new Promise((resolve, reject) => {
    const cb = (isErr) => isErr ? reject(new Error('Message NACKed!')) : resolve(tracePoint);
    eventPublishChannel.publish(eventExchangeName, routingKey, buffer, options, cb);
  });

  function Queue({queueName, exchangeName, prefetchCount, queueOptions, msgOptions}) {
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

  const publishOptions = (trace, payload, options, {isQuery = false} = {}) => {
    const newTracePoint = uuid.v4();
    const headers = Object.assign({
      trace: trace.concat(newTracePoint),
      ts: new Date().toJSON(),
      publisher: moduleName
    }, options.headers);

    return Object.assign({
        replyTo: isQuery ? DIRECT_REPLY_QUEUE : undefined,
        correlationId: isQuery ? newTracePoint : undefined,
        persistent: !isQuery,
        contentType: payload[SYMBOL_CONTENT_TYPE] || DEFAULT_CONTENT_TYPE
      }, options, {headers}
    );
  }
}

function bufferOf(payload) {
  const serializers = {
    'application/json': (body) => new Buffer(JSON.stringify(body)),
    'text/plain': (body) => new Buffer(body),
    'default': (body) => body
  };
  const contentType = payload[SYMBOL_CONTENT_TYPE] || DEFAULT_CONTENT_TYPE;
  const body = payload[SYMBOL_CONTENT_TYPE] ? payload.body : payload;
  const serializer = serializers[contentType] || serializers['default'];
  return serializer(body);
}

function parserOf(msg) {
  const parsers = {
    'application/json': (msg) => JSON.parse(msg.content.toString()),
    'text/plain': (msg) => msg.content.toString(),
    'default': (msg) => msg.content
  };
  return parsers[msg.properties.contentType] || parsers['default'];
}

export {MicroMessageQueues};
