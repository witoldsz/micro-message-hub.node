import {QueueBindings} from '../queue-bindings'

export function FakeQueue({queueName}) {

  let queueBindings = new QueueBindings();

  this.bind = (routingKey, callback) => {
    queueBindings.bind(routingKey, callback);
    return this;
  };

  this._incoming = (incoming) => {
    return new Promise((resolve, reject) => {
      queueBindings.emit('incoming', incoming, resolve, reject);
    });
  };
}
