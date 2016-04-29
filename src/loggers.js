import EventEmitter from 'events';

export class NoopLogger extends EventEmitter {}

export class ConsoleLogger extends EventEmitter {
  constructor() {
    super();
    this.on('incoming', (context, trace) => console.info('incoming %j', context));
    this.on('publishing', (context, trace) => console.info('publishing %j', context));
    this.on('binding', (context) => console.info('binding %j', context));
    this.on('error', (err) => console.error(err.stack || err));
    this.on('warn', (context, message) => console.warn(message + ' %j', context));
  }
}

export class BunyanAdapter extends EventEmitter {
  constructor(bunyanLog, levels = {
    incoming: 'trace', publishing: 'trace', binding: 'trace', error: 'error', warn: 'warn'
  }) {
    super();
    this.bunyanLog = bunyanLog;
    this.levels = levels;

    this.on('incoming', (context, trace) => this._log('incoming', Object.assign({trace}, context), 'incoming'));
    this.on('publishing', (context, trace) => this._log('publishing', Object.assign({trace}, context), 'publishing'));
    this.on('binding', (context) => this._log('binding', context, 'binding'));
    this.on('error', (err) => this._log('error', {err}));
    this.on('warn', (context, message) => this._log('warn', context, message));
  }

  _log(action, context, message) {
    const level = this.levels[action];
    if (level) this.bunyanLog[level](context, message);
  }
}
