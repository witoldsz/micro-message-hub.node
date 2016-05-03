import EventEmitter from 'events'

export class QueueBindings extends EventEmitter {
  constructor() {
    super();
    this._bindings = [];
    this.on('incoming', onIncoming);
  }

  bind(routingKey, callback) {
    this._bindings.push({
      routingKey,
      callback,
      routingKeyPattern: new RegExp(routingKey.replace(/[.]/g, '[.]').replace('[.]#', '([.].*)?'))});
  };

  get routingKeys() {
    return this._bindings.map(b => b.routingKey);
  }
}

function onIncoming({msg, routingKey, trace, load}, resolve, reject) {
  Promise.resolve()
    .then(() => Promise.all(
      this._bindings
        .filter(b => b.routingKeyPattern.test(routingKey))
        .map(b => b.callback(load, trace, msg))
    ))
    .then(resolve, reject);
}
