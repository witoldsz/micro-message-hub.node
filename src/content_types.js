export const DEFAULT_CONTENT_TYPE = 'application/json';
export const SYMBOL_CONTENT_TYPE = Symbol.for('content-type');

export function bufferOf(payload) {
  const serializers = {
    'application/json': (body) => new Buffer(JSON.stringify(body)),
    'text/plain': (body) => new Buffer(body),
    'default': (body) => body
  };
  const contentType = payload[SYMBOL_CONTENT_TYPE] || DEFAULT_CONTENT_TYPE;
  const body = payload[SYMBOL_CONTENT_TYPE] ? payload.body : payload;
  const serializer = serializers[contentType] || serializers['default'];
  return serializer(body);
}

export function parse(msg) {
  const buffer = msg.content;
  switch (msg.properties.contentType) {
    case 'application/json': return JSON.parse(buffer.toString());
    case 'text/plain': return buffer.toString();
    default: return buffer;
  }
}
