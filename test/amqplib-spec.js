'use strict';

import assert from 'assert';
import amqp from 'amqplib';

const DIRECT_REPLY_QUEUE = 'amq.rabbitmq.reply-to';

const str = (object) => JSON.stringify(object, null, 2)

describe('amqplib', () => {

  xit('should send direct reply to "amq.rabbitmq.reply-to" pseudo queue', (done) => {
    Promise.resolve()
      .then(() => amqp.connect())
      .then(conn => conn.createChannel())
      .then(chan => Promise.resolve()
        .then(() => {
          chan.assertQueue('query.increment');
          chan.consume('query.increment', msg => {
            const n = msg.content.toString();
            console.log(`query: ${n} ${str(msg.properties)}`)
            chan.sendToQueue(msg.properties.replyTo, new Buffer(`${parseInt(n) + 1}`))
          }, {noAck: true})
        })
      )
      .then(() => amqp.connect())
      .then(conn => Promise.resolve()
        .then(() => conn.createChannel())
        .then(chan => {
          chan.consume(DIRECT_REPLY_QUEUE, msg => {
            const n = msg.content.toString();
            console.log(`response: ${n} ${str(msg.fields)}`);
            // done();
          }, {noAck: true});
          chan.sendToQueue('query.increment', new Buffer('0'), {
            correlationId: undefined,
            replyTo: DIRECT_REPLY_QUEUE
          });
        })
        .then(() => conn.createChannel())
        .then(chan => {
          chan.consume(DIRECT_REPLY_QUEUE, msg => {
            const n = msg.content.toString();
            console.log(`response: ${n} ${JSON.stringify(msg.fields, null, 2)}`);
            // done();
          }, {noAck: true});
          chan.sendToQueue('query.increment', new Buffer('2'), {
            replyTo: DIRECT_REPLY_QUEUE
          });
        }))
      .catch(done)
  });
});

// chan.consume(DIRECT_REPLY_QUEUE, msg => {
//   const payload = msg.content.toString();
//   assert.equal(payload, '7');
//   chan.sendToQueue(msg.properties.replyTo, new Buffer(`${parseInt(payload) + 1}`));
// });
