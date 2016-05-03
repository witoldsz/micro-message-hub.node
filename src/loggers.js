import EventEmitter from 'events';

export class NoopLogger extends EventEmitter {}

export class ConsoleLogger extends EventEmitter {
  constructor({verbose = true} = {}) {
    super();
    this.on('incoming', (context, trace) => verbose && console.info('incoming %j', context));
    this.on('publishing', (context, trace) => verbose && console.info('publishing %j', context));
    this.on('binding', (context) => verbose && console.info('binding %j', context));
    this.on('error', (err) => console.error(err.stack || err));
    this.on('warn', (context, message) => console.warn(message + ' %j', context));
  }
}

export class BunyanAdapter extends EventEmitter {
  constructor(bunyanLog, levels = {
    incoming: 'trace', publishing: 'trace', binding: 'trace', error: 'error', warn: 'warn'
  }) {
    super();
    const log = (action, context, message) => {
      const level = levels[action];
      if (level) bunyanLog[level](context, message);
    };

    this.on('incoming', (context, trace) => log('incoming', Object.assign({trace}, context), 'incoming'));
    this.on('publishing', (context, trace) => log('publishing', Object.assign({trace}, context), 'publishing'));
    this.on('binding', (context) => log('binding', context, 'binding'));
    this.on('error', (err) => log('error', {err}));
    this.on('warn', (context, message) => log('warn', context, message));
  }
}
