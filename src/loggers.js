export const noopLogger = {
  name: 'noop',
  error: (err) => {
    console.error(err.stack || err);
  },
  warn: (context, message) => {},
  info: (context, message) => {}
};

export const consoleLogger = {
  name: 'console',
  error: (err) => {
    console.error(err.stack || err);
  },
  warn: (context, message) => {
    console.warn(message + ' %j', context);
  },
  info: (context, message) => {
    console.info(message + ' %j', context);
  }
};

export const bunyanAdapter = (bunyanLog, {
  levels: {error = 'error', warn = 'warn', info = 'info'} = {}
}) => ({

  name: 'bunyan',
  error: (err) => {
    bunyanLog[error](err);
  },
  warn: (context, message) => {
    bunyanLog[warn](context, message)
  },
  info: (context, message) => {
    bunyanLog[info](context, message)
  }
});
