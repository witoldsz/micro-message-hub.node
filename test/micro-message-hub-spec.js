'use strict';

const assert = require('assert');
const amqp = require('amqplib');

describe('micro message hub', () => {

  before(() => {

  });

  it('should verify the infrastructure', (done) => {
    Object.keys(process.env).sort().forEach(k => console.log(k + ': ' + process.env[k]));
    Promise.resolve()
      .then(() => amqp.connect())
      .then(conn => conn.createChannel())
      .then(chan => {
        chan.assertQueue('tasks');
        return chan.sendToQueue('tasks', new Buffer('something to do'));
      })
      .then(() => amqp.connect())
      .then(conn => conn.createChannel())
      .then(chan => {
        chan.assertQueue('tasks');
        chan.consume('tasks', msg => {
          assert.equal(msg.content.toString(), 'something to do');
          chan.ack(msg);
          done();
        });
      })
  });

});
