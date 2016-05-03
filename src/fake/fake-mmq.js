import assert from 'assert'
import {FakeQueue} from './fake-queue'
import {isDeepEqual, isFunction, isQuery} from './helpers'

export function FakeMicroMessageQueues({moduleName}) {

  let fakeQueues = [];
  let fakeResponses = [];

  this.reset = () => {
    fakeQueues = [];
    fakeResponses = [];
  };

  this.having = {
    incoming: (routingKey, load, trace = ['incoming-trace'], msg = {}) => {
      msg.fields = Object.assign({routingKey}, msg.fields);
      return Promise
        .all(fakeQueues.map(q => q._incoming({msg, routingKey, trace, load})))
        .then(results => isQuery(routingKey) ? (results[0] || [])[0] : undefined);
    }
  };

  this.when = {
    published: (routingKey, loadOrPredicate = () => true) => {
      return isQuery(routingKey)
        ?
      {willReturn: (responseOrResponseFn) => {
        assert(routingKey.startsWith('query.'), 'routing key must start with "query.", it was: ' + routingKey);
        fakeResponses.push({routingKey, loadOrPredicate, responseOrResponseFn});
      }}
        :
      {/* TODO... */}
    }
  };

  this.close = () => {
    return Promise.resolve();
  };

  this.connect = () => {
    return Promise.resolve();
  };

  this.ready = () => {
    return Promise.resolve();
  };

  this.eventQueue = (name = 'events') => {
    const q = new FakeQueue({queueName: moduleName + ':' + name});
    fakeQueues.push(q);
    return q;
  };

  this.queryQueue = (name = 'queries') => {
    const q = new FakeQueue({queueName: moduleName + ':' + name});
    fakeQueues.push(q);
    return q;
  };

  this.publish = (routingKey, load, trace, options = {}) => {
    return Promise.resolve()
      .then(() => {
        if (!trace) throw new Error(`The ${routingKey} was published with no trace.`);
        if (isQuery(routingKey)) {
          const matchingByRoutingKey = fakeResponses.filter(f => f.routingKey === routingKey);
          const matchingResponses = matchingByRoutingKey.filter(f => (isFunction(f.loadOrPredicate)
            ? f.loadOrPredicate(load)
            : isDeepEqual(load, f.loadOrPredicate))
          );

          if (matchingByRoutingKey.length < 1) throw new Error(
            `There is no response for ${routingKey}`);
          if (matchingResponses.length < 1) throw new Error(
            `There are responses for ${routingKey} but neither matches load/predicate`);
          if (matchingResponses.length > 1) throw new Error(
            `There is more than one matching response for ${routingKey}`);

          const responseOrResponseFn = matchingResponses[0].responseOrResponseFn;
          return isFunction(responseOrResponseFn) ? responseOrResponseFn(load) : responseOrResponseFn;
        }
      });
  };
}
