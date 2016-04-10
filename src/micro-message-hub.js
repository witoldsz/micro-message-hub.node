import amqp from 'amqplib';
import uuid  from 'uuid';

const EVENT_EXCHANGE = 'amq.topic';
const QUERY_EXCHANGE = 'amq.topic';
const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to';

function MicroMessageHub({
    moduleName, url, socketOptions, conn,
    options: {
      eventExchangeName = EVENT_EXCHANGE,
      queryExchangeName = QUERY_EXCHANGE
    } = {}
  }) {

  const queues = [];
  let   publishChannel;

  this._responsesChannel = null;

  this.connect = () => {
    return (conn ? Promise.resolve(conn) : amqp.connect(url, socketOptions))
      .then(c => {
        conn = c;
        return Promise.all([conn.createConfirmChannel(), conn.createChannel()])
      })
      .then(([confirmChannel, simpleChannel]) => {
        publishChannel = confirmChannel;
        this._responsesChannel = simpleChannel;
      })
      ;
  };

  this.ready = () => {
    return Promise.all(queues.map(q => q._ready()));
  };

  this.eventQueue = (name = 'events', {prefetchCount = 1} = {}) => {
    const q = new EventQueue({
      conn,
      queueName: moduleName + ':' + name,
      exchangeName: eventExchangeName,
      prefetchCount
    });
    queues.push(q);
    return q;
  };

  this.queryQueue = (name = 'queries', {prefetchCount = 0} = {}) => {
    const q = new QueryQueue({
      conn,
      queueName: moduleName + ':' + name,
      exchangeName: queryExchangeName,
      prefetchCount,
      responsesChannel: this._responsesChannel
    });
    queues.push(q);
    return q;
  };

  this.publish = (routingKey, body = {}, trace = [], options = {}) => {
    options = publishOptions(trace, options, {isQuery: routingKey.startsWith('query.')});
    return new Promise((resolve, reject) => {
      const cb = (isErr) => isErr ? reject(new Error('Message nacked!')) : resolve(/*TODO: tracePoint*/);
      publishChannel.publish(eventExchangeName, routingKey, bufferOf(body, options), options, cb);
    });
  };

  this.___publish = (routingKey, body = {}, trace = [], options = {}) => {
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
      const serializer = this._serializers[options.contentType] || this._serializers['default'];
      const buffer = serializer(body);
      const cb = (err) => err ? reject('Message nacked!') : resolve(newTracePoint);
      publishChannel.publish(eventExchangeName, routingKey, buffer, options, cb)
    });
  };

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

function publishOptions(trace, options, {isQuery = false}) {
  const newTracePoint = uuid.v4();
  const headers = Object.assign({
    correlationId: isQuery ? newTracePoint : undefined,
    replyTo: isQuery ? DIRECT_REPLY_QUEUE : undefined,
    trace: trace.concat(newTracePoint),
    ts: new Date().toJSON()
  }, options.headers);

  return Object.assign({
      contentType: 'application/json',
      persistent: true
    }, options, {headers}
  );
}

class AbstractQueue {
  constructor({conn, queueName, exchangeName, prefetchCount, noAck, queueOptions}) {
    this._conn = conn;
    this._queueName = queueName;
    this._exchangeName = exchangeName;
    this._prefetchCount = prefetchCount;
    this._noAck = noAck;
    this._queueOptions = queueOptions;
    this._bindings = [];
    this._parsers = {
      'application/json': (msg) => JSON.parse(msg.content.toString()),
      'text/plain': (msg) => msg.content.toString(),
      'default': (msg) => msg.content
    };
  }

  bind(routingKey, callback) {
    console.log(`binding queue [${this._queueName}] to <${routingKey}>`);
    const patternStr = routingKey.replace('.', '[.]').replace('[.]#', '([.].*)?');
    const routingKeyPattern = new RegExp(patternStr);
    this._bindings.push({callback, routingKey, routingKeyPattern});
    return this;
  }

  _ready() {
    return this._createChannel()
      .then(channel => {
        this._channel = channel;
        this._channel.prefetch(this._prefetchCount);
        return this._channel.assertQueue(this._queueName, this._queueOptions);
      })
      .then(() => Promise.all(this._bindings.map(b =>
        this._channel.bindQueue(this._queueName, this._exchangeName, b.routingKey))))
      .then(() => this._channel.consume(this._queueName, (msg) => this._msgHandler(msg), {noAck: this._noAck}));
  }

  _msgHandler(msg) {
    const routingKey = msg.fields.routingKey;
    const accepted = this._bindings.filter(b => b.routingKeyPattern.test(routingKey));

    if (accepted.length < 1) {
      console.log(`MMH: No listener for <${routingKey}> on queue [${this._queueName}]`);
    }

    const parser = this._parsers[msg.properties.contentType] || this._parsers['default'];
    const trace = msg.properties.headers.trace || [];

    return Promise
      .all(accepted.map(b => b.callback(parser(msg), trace, msg)))
      .then(results => this._ack(msg, results), () => this._nack(msg));
  }
}

class EventQueue extends AbstractQueue {
  constructor(args) {
    super(Object.assign({
      _noAck: false,
      queueOptions: {
        exclusive: false,
        durable: true,
        autoDelete: false
      }}, args));
  }
  _createChannel() {return this._conn.createConfirmChannel()}
  _ack(msg, results) {return this._channel.ack(msg)}
  _nack(msg) {return this._channel.nack(msg)}
}

class QueryQueue extends AbstractQueue {
  constructor({args}) {
    super(Object.assign({
      noAck: true,
      queueOptions: {
        exclusive: true,
        durable: false,
        autoDelete: true
      }}, args));
    this._responsesChannel = args.responsesChannel;
  }
  _createChannel() {return this._conn.createChannel()}
  _ack(msg, results) {
    const options = publishOptions(msg.properties.headers.trace, {
      persistent: false,
      correlationId: msg.properties.correlationId
    });
    results.forEach(result => {
      this._responsesChannel.sendToQueue(msg.properties.replyTo, options)
    });
    return Promise.resolve();
  }
  _nack(msg) {return Promise.resolve()}
}

export {MicroMessageHub};
