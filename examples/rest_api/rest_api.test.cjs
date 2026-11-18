const test = require('node:test');
const assert = require('node:assert/strict');
const { decodeMultipart, decodeReply } = require('./rest_api.js');
const type = 'multipart/mixed; boundary="example"';
function body(blobs = []) {
    const parts = [Buffer.from('--example\r\nContent-Type: application/json\r\n\r\n[{"FindBlob":{"status":0}}]')];
    for (const blob of blobs) {
        parts.push(Buffer.from(`\r\n--example\r\nContent-Type: application/octet-stream\r\nContent-Length: ${blob.length}\r\n\r\n`), blob);
    }
    parts.push(Buffer.from('\r\n--example--\r\n'));
    return Buffer.concat(parts);
}
test('raw binary data, embedded boundaries, and empty blobs', () => {
    const blobs = [Buffer.from([0, 255, 13, 10]), Buffer.from('\r\n--example\r\ninside blob'), Buffer.alloc(0)];
    const reply = decodeReply(body(blobs), type, '2');
    assert.equal(reply.helloVersion, 2);
    assert.equal(reply.json[0].FindBlob.status, 0);
    assert.deepEqual(reply.blobs.map(value => [...value]), blobs.map(value => [...value]));
});
test('legacy base64 envelope remains supported', () => {
    const bytes = Buffer.from(JSON.stringify({ json: [], blobs: ['AP8=', ''] }));
    assert.deepEqual(decodeReply(bytes, 'application/json', null).blobs.map(value => [...value]), [[0, 255], []]);
});
test('requires the hello header and complete MIME terminator', () => {
    assert.throws(() => decodeReply(body(), type, null));
    assert.throws(() => decodeMultipart(body().subarray(0, body().length - 8), type));
});
test('checks declared blob sizes and duplicate part headers', () => {
    assert.throws(() => decodeMultipart(Buffer.from(body([Buffer.from('a')]).toString().replace('Content-Length: 1', 'Content-Length: 1000')), type));
    assert.throws(() => decodeMultipart(Buffer.from(body([Buffer.from('a')]).toString().replace('Content-Length: 1', 'Content-Length: 1\r\nContent-Length: 1')), type));
});
