import assert from 'assert'
import {FakeMicroMessageQueues} from '../src/fake/fake-mmq'

describe('fake micro message queues', () => {

  const mmq = new FakeMicroMessageQueues({moduleName: 'TestModule'});

  beforeEach(() => {
    mmq.reset();
  });

  describe('triggering real listeners by fake API: having.incoming(...)', () => {
    it('should trigger real event listener with plain routingKey', () => {
      let hiHasBeenSaid = false;
      mmq.eventQueue().bind('command.sayHi', (event, trace) => {
        hiHasBeenSaid = true;
        assert.deepEqual(event, {name: 'Jerry'});
        assert.deepEqual(trace, ['my-trace']);
      });

      return Promise.resolve()
        .then(() => mmq.having.incoming('command.sayHi', {name: 'Jerry'}, ['my-trace']))
        .then(() => {
          assert(hiHasBeenSaid, 'hi has been said');
        });
    });

    it('should trigger real event listeners with wildcard routingKey', () => {
      let count = 0;
      let routes = [];
      mmq.eventQueue().bind('command.#', ((event, trace, msg) => {
        count++;
        routes.push(msg.fields.routingKey);
      }));
      return Promise.resolve()
        .then(() => mmq.having.incoming('command.sayHi', {}))
        .then(() => mmq.having.incoming('command.sayYo', {}))
        .then(() => mmq.having.incoming('command', {}))
        .then(() => mmq.having.incoming('event.ignoreMe', {}))
        .then(() => {
          assert.equal(count, 3, 'count');
          assert.deepEqual(routes, ['command.sayHi', 'command.sayYo', 'command'], 'routes');
        });
    });

    it('should trigger real query and get response', () => {
      let count = 0, theTraceWas, routingKeyWas;
      mmq.queryQueue().bind('query.exchangeRate', ((query, trace, msg) => {
        count++;
        theTraceWas = trace;
        routingKeyWas = msg.fields.routingKey;
        return query.BID_EUR_PLN ? {BID_EUR_PLN: '4.3832'} : {};
      }));
      return mmq
        .having.incoming('query.exchangeRate', {BID_EUR_PLN: true})
        .then(response => {
          assert.deepEqual(response, {BID_EUR_PLN: '4.3832'});
          assert.deepEqual(theTraceWas, ['incoming-trace'], 'the trace was');
          assert.equal(count, 1, 'count');
          assert.equal(routingKeyWas, 'query.exchangeRate', 'routing key was');
        });
    });
  });

  describe('recording query responses: when.published(query...).willReturn(...)', () => {
    it('should record a query response, ignore criteria, object result', () => {
      mmq.when.published('query.other').willReturn({some: 'other'});
      mmq.when.published('query.exchangeRate').willReturn({some: 'response'});
      return mmq
        .publish('query.exchangeRate', {ignoreMe: true}, [])
        .then(result => {
          assert.deepEqual(result, {some: 'response'});
        });
    });

    it('should record a query response, object criteria, object result', () => {
      mmq.when.published('query.exchangeRate', {BID_EUR_PLN: true}).willReturn({BID_EUR_PLN: '4.3832'});
      mmq.when.published('query.exchangeRate', {BID_EUR_USD: true}).willReturn({BID_EUR_USD: '1.1528'});
      return mmq
        .publish('query.exchangeRate', {BID_EUR_PLN: true}, [])
        .then(result => {
          assert.deepEqual(result, {BID_EUR_PLN: '4.3832'});
        });
    });

    it('should record a query response, function criteria, function result', () => {
      mmq.when.published('query.exchangeRate', (load) => load.BID_EUR_PLN).willReturn(load => ({queryBodyWas: load}));
      return mmq
        .publish('query.exchangeRate', {BID_EUR_PLN: true}, [])
        .then(result => {
          assert.deepEqual(result, {queryBodyWas: {BID_EUR_PLN: true}});
        });
    });

    it('should throw Error when nothing to return', () => {
      mmq.when.published('query.exchangeRate', {BID_EUR_PLN: true}).willReturn({BID_EUR_PLN: '4.3832'});
      return Promise.resolve()
        .then(() => mmq.publish('query.somethingElse', {}, []))
        .then(
          () => assert.fail('no error', 'an error', 'should throw for query.somethingElse'),
          err => assert.equal(err.message, 'There is no response for query.somethingElse'))

        .then(() => mmq.publish('query.exchangeRate', {ASK_EUR_PLN: true}, []))
        .then(
          () => assert.fail('no error', 'an error', 'should throw for query.exchangeRate'),
          err => assert.equal(err.message,
            'There are responses for query.exchangeRate but neither matches the {"ASK_EUR_PLN":true}'))
    });
  });

  describe('recording event feedback: when.published(event...).thenExecute(...)', () => {
    it('should execute callback with parameters provided when recording', () => {
      return mmq
        .when.published('command.sayHi', {I_AM: 'Sam'})
        .thenExecute((routingKey, event) => {
          assert.equal(routingKey, 'command.sayHi');
          assert.deepEqual(event, {I_AM: 'Sam'});
        })
    });
  });
});
