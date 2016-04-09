import amqp from 'amqplib';
import uuid  from 'uuid';

const DELIVERY_MODE_PERSISTENT = 2;
const EVENT_EXCHANGE = 'amq.topic';
const QUERY_EXCHANGE = 'amq.topic';

function MicroMessageHub({
    _moduleName, _url, _socketOptions, _conn,
    options: {
      _eventExchangeName = EVENT_EXCHANGE,
      _queryExchangeName = QUERY_EXCHANGE
    }
  }) {

  let _publishChannel;

  this.parsers = {
    'application/json': (msg) => JSON.parse(msg.content.toString()),
    'text/plain': (msg) => msg.content.toString(),
    'default': (msg) => msg.content
  };

  this.serializers = {
    'application/json': (body) => new Buffer(JSON.stringify(body)),
    'text/plain': (body) => new Buffer(body),
    'default': (body) => body
  };

  this.connect = () => {
    return (_conn ? Promise.resolve(_conn) : amqp.connect(_url, _socketOptions))
      .then(conn => (_conn = conn).createConfirmChannel())
      .then(chan => _publishChannel = chan)
      ;
  };

  this.ready = () => {

  };

  this.eventQueue = (name = 'events', {prefetchCount = 1}) => {
    const queueName = _moduleName + ':' + name;
    return _conn.createConfirmChannel()
      .then(channel => {
        channel.prefetch(prefetchCount);
        return channel.assertQueue(queueName, {
            exclusive: false,
            durable: true,
            autoDelete: false
          })
          .then(() => new MicroMessageQueue({
            channel,
            queueName,
            exchangeName: _eventExchangeName,
            autoAck: false,
            msgHandler: _eventMsgHandler
          }))
      });
  };

  this.queryQueue = (name = 'queries', {prefetchCount = 0}) => {
    const queueName = _moduleName + (name ? ':' : '') + name;
    return _conn.createChannel()
      .then(channel => {
        channel.prefetch(prefetchCount);
        return channel.assertQueue(_moduleName + ':' + name, {
            exclusive: true,
            durable: false,
            autoDelete: true,
            noAck: true
          })
          .then(() => new MicroMessageQueue({
            channel,
            queueName,
            exchangeName: _queryExchangeName,
            autoAck: true,
            _msgHandler: _queryMsgHandler
          }));
      });
  };

  this.publish = (routingKey, content = {}, trace = [], options = {}) => {
    return new Promise((reject, resolve) => {
      const newTracePoint = uuid.v4();
      const headers = Object.assign({
        trace: trace.concat(newTracePoint),
        ts: new Date().toJSON()
      }, options.headers);

      options = Object.assign({
          contentType: 'application/json',
          deliveryMode: DELIVERY_MODE_PERSISTENT
        }, options, {headers}
      );
      const buffer = new Buffer(JSON.stringify(content));
      const cb = (err) => err ? reject('Message nacked!') : resolve(newTracePoint);
      _publishChannel.publish(_eventExchangeName, routingKey, buffer, options, cb)
    });
  };

  const _eventMsgHandler = (channel, queueName, bindings) => (msg) => {
    const routingKey = msg.fields.routingKey;
    const accepted = bindings.filter(b => b.test(routingKey));
    if (accepted.length < 1) {
      console.log(`MMH: No listener for <${routingKey}> on queue [${queueName}]`);
      return;
    }
    const parser = this.parsers[msg.fields.contentType] || this.parsers['default'];
    const event = parser(msg);
    const trace = msg.properties.headers.trace || [];

    return Promise.all(accepted.map(b => b.callback(event, trace, msg)))
      .then(() => channel.ack(msg), () => channel.nack(msg));
  };

  const _queryMsgHandler = (channel, queueName, bindings) => (msg) => {
    //TODO...
  };
}

function MicroMessageQueue({_channel, _queueName, _exchangeName, _noAck, _msgHandler}) {

  const _bindings = [];

  this.bind = (routingKey, callback) => {
    console.log(`MMH: binding queue [${_queueName}] to <${routingKey}>`);

    const regex = routingKey.replace('.', '[.]').replace('[.]#', '([.].*)?');
    const routingKeyPattern = new RegExp(regex);

    _bindings.push({
      callback,
      test: (currentRoutingKey) => routingKeyPattern.test(currentRoutingKey)
    });

    return _channel.bind(_queueName, _exchangeName, routingKey)
      .then(() => this);
  };

  this.done = () => {
    return _channel.consume(_queueName, _msgHandler(_channel, _queueName, _bindings), {noAck: _noAck});
  };
}

export {MicroMessageHub};
