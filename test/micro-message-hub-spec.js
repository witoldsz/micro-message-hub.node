import assert from 'assert'
import amqp from 'amqplib'

import {MicroMessageHub} from '../src/micro-message-hub'

describe('micro message hub', () => {

  const hub1 = new MicroMessageHub({moduleName: 'test-1'});
  const hub2 = new MicroMessageHub({moduleName: 'test-2'});

  before(() => Promise.all([hub1.connect(), hub2.connect()]));

  it('should create event queue and receive published event', (done) => {
    hub1.eventQueue()
      .bind('command.sayHi', (event, trace) => {
        assert.equal(event.name, 'Hub2');
        done();
      });
    Promise
      .all([hub1.ready(), hub2.ready()])
      .then(() => hub2.publish('command.sayHi', {name: 'Hub2'}))
      .catch(done);
  });

  it('should create query queue and answer the question', () => {
    hub1.queryQueue()
      .bind('query.plusOne', (event, trace) => {
        return {number: event.number + 1};
      });
    return Promise
      .all([hub1.ready(), hub2.ready()])
      .then(() => hub2.publish('query.plusOne', {number: 0}))
      .then(result => {
        assert.equal(result.number, 1);
      })
  });
});
