import amqp from 'amqplib';
import uuid  from 'uuid';

const EVENT_EXCHANGE = 'amq.topic';
const QUERY_EXCHANGE = 'amq.topic';
const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to';
const DEFAULT_QUERY_TIMEOUT = 3000;

function MicroMessageHub({
    moduleName, url, socketOptions, conn,
    options: {
      eventExchangeName = EVENT_EXCHANGE,
      queryExchangeName = QUERY_EXCHANGE
    } = {}
  }) {

  const queues = [];
  const queryResponseListeners = new Map();
  let eventPublishChannel, queryChannel;

  this.connect = () => {
    return Promise.resolve(conn || amqp.connect(url, socketOptions))
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
            resolve(parserOf(msg)(msg));
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
          queryChannel.sendToQueue(msg.properties.replyTo, new Buffer(moduleName), {contentType: 'text/plain'});
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
            const options = publishOptions(p.headers.trace, {
              persistent: false,
              correlationId: p.correlationId
            });
            results.forEach(r => channel.sendToQueue(p.replyTo, bufferOf(r, options), options));
          } catch (err) {
            console.error(err);
          }
        },
        onNack: () => {}
      }
    });
    queues.push(q);
    return q;
  };

  this.publish = (routingKey, body = {}, trace = [], options = {}) => {
    const isQuery = routingKey.startsWith('query.');
    options = publishOptions(trace, options, {isQuery});
    const newTrace = options.headers.trace;
    const newTracePoint = newTrace[newTrace.length - 1];
    const buffer = bufferOf(body, options);
    return isQuery
      ? publishQueryPromise(routingKey, buffer, newTrace, newTracePoint, options)
      : publishEventPromise(routingKey, buffer, newTrace, newTracePoint, options);
  };

  const publishQueryPromise = (routingKey, buffer, trace, tracePoint, options) => new Promise((resolve, reject) => {
    const correlationId = options.correlationId;
    const rejectBackup = () => queryResponseListeners.delete(correlationId) && reject(new Error('Timeout'));
    queryResponseListeners.set(correlationId, resolve);
    setTimeout(rejectBackup, DEFAULT_QUERY_TIMEOUT);
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
      const routingKey = msg.fields.routingKey;
      const accepted = bindings.filter(b => b.routingKeyPattern.test(routingKey));

      if (accepted.length < 1) {
        console.log(`No listener for <${routingKey}> on queue [${queueName}]`);
      }

      const parser = parserOf(msg);
      const trace = msg.properties.headers.trace || [];

      Promise.resolve()
        .then(() => Promise.all(accepted.map(b => b.callback(parser(msg), trace, msg))))
        .then(results => msgOptions.onAck(channel, msg, results), () => msgOptions.onNack(channel, msg))
        .catch(err => console.error(err));
    }
  }

  const publishOptions = (trace, options, {isQuery = false} = {}) => {
    const newTracePoint = uuid.v4();
    const headers = Object.assign({
      trace: trace.concat(newTracePoint),
      ts: new Date().toJSON(),
      publisher: moduleName
    }, options.headers);

    return Object.assign({
        replyTo: isQuery ? DIRECT_REPLY_QUEUE : undefined,
        correlationId: isQuery ? newTracePoint : undefined,
        contentType: 'application/json',
        persistent: true
      }, options, {headers}
    );
  }
}

function bufferOf(body, options) {
  const serializers = {
    'application/json': (body) => new Buffer(JSON.stringify(body)),
    'text/plain': (body) => new Buffer(body),
    'default': (body) => body
  };
  const serializer = serializers[options.contentType] || serializers['default'];
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

export {MicroMessageHub};
