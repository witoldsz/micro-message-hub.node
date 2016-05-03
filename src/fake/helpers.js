import assert from 'assert'

/**
 * Forgive me, it's just a helper for fakes
 */
export function isDeepEqual(a, b) {
  try {
    assert.deepEqual(a, b);
    return true;
  } catch (e) {
    return false;
  }
}

export function isFunction(a) {
  return Object.prototype.toString.call(a) === '[object Function]';
}

export function isQuery(routingKey) {
  return routingKey.startsWith('query.');
}
