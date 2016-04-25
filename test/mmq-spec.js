import assert from 'assert'
import amqp from 'amqplib'

import {MicroMessageQueues} from '../src/mmq'

const TS_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;

const DELIVERY_MODE_PERSISTENT = 2;
const DELIVERY_MODE_NON_PERSISTENT = 1;

describe('micro message hub', () => {

  let mmq1, mmq2;
  let amqpConnection, amqpChannel;

  before(() => {
    return amqp.connect()
      .then(conn => {amqpConnection = conn; return conn.createChannel();})
      .then((chan) => {amqpChannel = chan;});
  });

  beforeEach(() => {
    return Promise.all([
        amqpChannel.deleteQueue('mmq1:events'),
        amqpChannel.deleteQueue('mmq2:events'),
        amqpChannel.deleteQueue('mmq1:queries'),
        amqpChannel.deleteQueue('mmq2:queries')
      ])
      .then(() => {
        mmq1 = new MicroMessageQueues({moduleName: 'mmq1'});
        mmq2 = new MicroMessageQueues({moduleName: 'mmq2'});
        return Promise.all([mmq1.connect(), mmq2.connect()])
      })
  });

  afterEach(() => Promise.all([mmq1.close(), mmq2.close()]));
  after(() => amqpConnection.close());

  describe('basics', () => {
    it('should create event queue and receive published event', (done) => {
      mmq1.eventQueue()
        .bind('command.sayHi', (event, trace, msg) => {
          try {
            assert.equal(event.name, 'Stefan', 'name');

            assert.equal(msg.properties.deliveryMode, DELIVERY_MODE_PERSISTENT, 'delivery mode');
            const headers = msg.properties.headers;
            assert(headers, 'headers');
            const ts = headers.ts;
            assert(ts, 'ts header');
            assert(TS_PATTERN.test(ts), `ts header ${ts} format`);
            assert(Array.isArray(headers.trace), 'trace header is array');
            assert.deepEqual(headers.trace, trace, 'trace header equal to trace argument');
            assert.equal(headers.publisher, 'mmq2', 'publisher name');
            assert.equal(msg.properties.contentType, 'application/json', 'content type');
            done();
          } catch (err) {
            done(err);
          }
        });
      Promise
        .all([mmq1.ready(), mmq2.ready()])
        .then(() => mmq2.publish('command.sayHi', {name: 'Stefan'}))
        .catch(done);
    });

    it('should create query queue and answer the question', (done) => {
      mmq1.queryQueue()
        .bind('query.plusOne', (event, trace, msg) => {
          try {
            assert.equal(msg.properties.deliveryMode, DELIVERY_MODE_NON_PERSISTENT, 'query handler: delivery mode');
            const headers = msg.properties.headers;
            assert(headers, 'query handler: headers');
            const ts = headers.ts;
            assert(ts, 'query handler: ts header');
            assert(TS_PATTERN.test(ts), `query handler: ts header ${ts} format`);
            assert(Array.isArray(headers.trace), 'query handler: trace header is array');
            assert.deepEqual(headers.trace, trace, 'query handler: trace header equal to trace argument');
            assert.equal(headers.publisher, 'mmq2', 'query handler: publisher name');
            assert.equal(msg.properties.contentType, 'application/json', 'query handler: content type');
          } catch (err) {
            done(err);
          }
          return {number: event.number + 1};
        });
      Promise
        .all([mmq1.ready(), mmq2.ready()])
        .then(() => mmq2.publish('query.plusOne', {number: 12}))
        .then(result => {
          assert(result.body, 'query response: result has body');
          assert.equal(result.body.number, 13, 'query response: body.number');
          assert(result.correlationId, 'query response: result has correlationId');
          const msg = result.msg;
          assert(msg, 'query response: result has msg');
          assert.equal(msg.properties.deliveryMode, DELIVERY_MODE_NON_PERSISTENT, 'query response: delivery mode');
          const headers = msg.properties.headers;
          assert(headers, 'query response: headers');
          const ts = headers.ts;
          assert(ts, 'query response: ts header');
          assert(TS_PATTERN.test(ts), `query response: ts header ${ts} format`);
          assert(Array.isArray(headers.trace), 'query response: trace header is array');
          assert.deepEqual(headers.trace, result.trace, 'query response: trace header equal to trace property');
          assert.equal(headers.publisher, 'mmq1', 'query response: publisher name');
          assert.equal(msg.properties.contentType, 'application/json', 'query response: content type');
          done();
        })
        .catch(done);
    });

  });
});
