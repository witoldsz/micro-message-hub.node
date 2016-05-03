import amqp from 'amqplib';
import uuid  from 'uuid';
import {Queue} from './queue';
import {DEFAULT_CONTENT_TYPE, SYMBOL_CONTENT_TYPE, bufferOf, parse} from './content_types'
import {ConsoleLogger} from './loggers'

const DEFAULT_URL = '';
const DEFAULT_EVENT_EXCHANGE = 'amq.topic';
const DEFAULT_QUERY_EXCHANGE = 'amq.topic';
const DEFAULT_QUERY_TIMEOUT = 3000;

const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to';

export function MicroMessageQueues({
    moduleName, url = DEFAULT_URL, socketOptions, conn,
    options: {
      log = new ConsoleLogger(),
      eventExchangeName = DEFAULT_EVENT_EXCHANGE,
      queryExchangeName = DEFAULT_QUERY_EXCHANGE,
      queryTimeout = DEFAULT_QUERY_TIMEOUT
    } = {}
  }) {

  const amqpConnWasProvided = !!conn;
  const queues = [];
  const queryResponseListeners = new Map();
  let eventPublishChannel, queryChannel;

  /**
   * Closes the connection if it was established here or the parameter "force" is true. It is untouched otherwise.
   * All the channels are closed always.
   * Once closed, the MicroMessageQueues cannot be used again, it has to be recreated from scratch.
   * @param force when true, the AMPQ connection will be closed even if it was not opened here.
   * @returns {Promise}
   * */
  this.close = ({force = false} = {}) => {
    return Promise
      .all(queues.map(q => q._close()))
      .then(() => eventPublishChannel && eventPublishChannel.close())
      .then(() => queryChannel && queryChannel.close())
      .then(() => conn && (!amqpConnWasProvided || force) && conn.close())
    ;
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
              body: parse(msg)
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
      log,
      conn,
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
      log,
      conn,
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
            log.emit('error', err);
          }
        },
        onNack: () => {}
      }
    });
    queues.push(q);
    return q;
  };

  this.publish = (routingKey, load, trace = [], options = {}) => {
    const isQuery = routingKey.startsWith('query.');
    options = publishOptions(trace, load, options, {isQuery});
    const newTrace = options.headers.trace;
    const newTracePoint = newTrace[newTrace.length - 1];
    const buffer = bufferOf(load);

    log.emit('publishing', {routingKey, load}, newTrace);

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

  const publishOptions = (trace, load, options, {isQuery = false} = {}) => {
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
        contentType: load[SYMBOL_CONTENT_TYPE] || DEFAULT_CONTENT_TYPE
      }, options, {headers}
    );
  }
}
