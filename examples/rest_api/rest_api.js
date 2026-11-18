/* A small dual-version browser example. Binary response values stay raw. */
const MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
const utf8 = bytes => new TextDecoder('utf-8', { fatal: true }).decode(bytes);
const ascii = value => Uint8Array.from(value, character => character.charCodeAt(0));
const matches = (bytes, offset, expected) => offset + expected.length <= bytes.length &&
    expected.every((byte, index) => bytes[offset + index] === byte);

function find(bytes, expected, start) {
    for (let offset = start; offset <= bytes.length - expected.length; ++offset) {
        if (matches(bytes, offset, expected)) return offset;
    }
    throw new Error('Incomplete multipart response.');
}

function decodeMultipart(bytes, contentType) {
    const match = contentType.match(/;\s*boundary=(?:"([^"]+)"|([^;\s]+))/i);
    const boundary = match && (match[1] || match[2]);
    if (!boundary || boundary.length > 70 || !/^[\x21-\x7e]+$/.test(boundary)) {
        throw new Error('Invalid multipart boundary.');
    }
    const delimiter = ascii(`--${boundary}`);
    const separator = ascii(`\r\n--${boundary}`);
    const crlf = ascii('\r\n');
    let offset = 0, json;
    const blobs = [];
    while (true) {
        if (!matches(bytes, offset, delimiter)) throw new Error('Invalid multipart delimiter.');
        offset += delimiter.length;
        if (matches(bytes, offset, ascii('--'))) {
            offset += 2;
            if (matches(bytes, offset, crlf)) offset += 2;
            if (json === undefined || offset !== bytes.length) throw new Error('Incomplete multipart response.');
            return { json, blobs, helloVersion: 2 };
        }
        if (!matches(bytes, offset, crlf)) throw new Error('Invalid multipart separator.');
        offset += 2;
        const end = find(bytes, ascii('\r\n\r\n'), offset);
        if (end - offset > 16384) throw new Error('Multipart headers are too large.');
        const headers = {};
        for (const line of utf8(bytes.subarray(offset, end)).split('\r\n')) {
            const colon = line.indexOf(':');
            const key = line.slice(0, colon).trim().toLowerCase();
            if (colon <= 0 || key in headers) throw new Error('Invalid multipart header.');
            headers[key] = line.slice(colon + 1).trim();
        }
        offset = end + 4;
        if (json === undefined) {
            if (headers['content-type'] !== 'application/json') throw new Error('Expected JSON metadata first.');
            const jsonEnd = find(bytes, separator, offset);
            json = JSON.parse(utf8(bytes.subarray(offset, jsonEnd)));
            if (json === null || typeof json !== 'object') throw new Error('Invalid result JSON.');
            offset = jsonEnd + 2;
        } else {
            if (headers['content-type'] !== 'application/octet-stream' || !/^\d+$/.test(headers['content-length'] || '')) {
                throw new Error('Binary parts require an exact Content-Length.');
            }
            const size = Number(headers['content-length']);
            if (!Number.isSafeInteger(size) || size > bytes.length - offset) throw new Error('Incomplete binary part.');
            blobs.push(bytes.subarray(offset, offset + size));
            offset += size;
            if (!matches(bytes, offset, crlf)) throw new Error('Incomplete binary part.');
            offset += 2;
        }
    }
}

function decodeReply(bytes, contentType, helloHeader) {
    if (/^multipart\/mixed(?:\s*;|$)/i.test(contentType)) {
        if (helloHeader !== '2') throw new Error('Unsupported HTTP hello version.');
        return decodeMultipart(bytes, contentType);
    }
    if (helloHeader !== null && helloHeader !== '1') throw new Error('Unsupported HTTP hello version.');
    const reply = JSON.parse(utf8(bytes));
    if (!reply || !('json' in reply) || !Array.isArray(reply.blobs)) throw new Error('Invalid legacy response.');
    return { json: reply.json, helloVersion: 1,
        blobs: reply.blobs.map(value => Uint8Array.from(atob(value), character => character.charCodeAt(0))) };
}

async function readResponse(response) {
    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    try {
        while (true) {
            const chunk = await reader.read();
            if (chunk.done) break;
            size += chunk.value.length;
            if (size > MAX_RESPONSE_BYTES) {
                await reader.cancel();
                throw new Error('Response exceeds this preview example\'s 64 MiB limit.');
            }
            chunks.push(chunk.value);
        }
    } finally {
        reader.releaseLock();
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
    return decodeReply(bytes, response.headers.get('content-type') || '',
        response.headers.get('ApertureDB-Hello-Version'));
}

async function postQuery(url, query, blobs, token, version) {
    const body = new FormData();
    body.append('query', JSON.stringify(query));
    for (const file of blobs) {
        body.append('blobs', new Blob([file], { type: 'application/octet-stream' }), 'blobs');
    }
    const headers = { 'ApertureDB-Hello-Version': String(version) };
    if (token) headers.Authorization = `Bearer ${token}`;
    // Redirects can replay a write. Failed requests require an explicit user action.
    const response = await fetch(url, { method: 'POST', headers, body, redirect: 'error' });
    if (!response.ok) throw new Error(`HTTP ${response.status}; the request was not replayed.`);
    return readResponse(response);
}

function setupExample() {
    let token = null, version = null, endpoint = '';
    const output = document.getElementById('output');
    const form = document.getElementById('addimage');
    const show = value => { output.textContent += `${typeof value === 'string' ? value : JSON.stringify(value, null, 2)}\n`; };
    const run = async (query, blobs = []) => postQuery(endpoint, query, blobs, token, version);
    const check = reply => {
        if (!Array.isArray(reply.json) || reply.json.some(command => Object.values(command)[0].status < 0)) {
            throw new Error(JSON.stringify(reply.json));
        }
        return reply.json;
    };
    const listImages = async () => {
        const options = { blobs: false, limit: 10 };
        if (version === 2) options.uniqueids = true;
        else options.results = { uniqueids: true };
        const metadata = check(await run([{ FindImage: options }]));
        show(metadata);
        const first = (metadata[0].FindImage.entities || [])[0];
        if (!first) return show('Upload an image to display a preview.');
        const find = { blobs: true, as_format: 'png', constraints: version === 2 ?
            ['$_uniqueid', '==', first._uniqueid] : { _uniqueid: ['==', first._uniqueid] } };
        const reply = await run([{ FindImage: find }]);
        check(reply);
        const preview = document.getElementById('preview');
        preview.replaceChildren();
        for (const bytes of reply.blobs) {
            const image = document.createElement('img');
            const url = URL.createObjectURL(new Blob([bytes], { type: 'image/png' }));
            image.onload = () => URL.revokeObjectURL(url);
            image.src = url;
            image.alt = 'Database image';
            preview.appendChild(image);
        }
    };
    document.getElementById('connect').addEventListener('submit', async event => {
        event.preventDefault();
        token = null;
        form.querySelector('button').disabled = true;
        output.textContent = '';
        try {
            endpoint = new URL(document.getElementById('endpoint').value, window.location.href).href;
            if (!endpoint.endsWith('/')) endpoint += '/';
            // An empty query is harmless discovery; no GetStatus.version lookup.
            version = (await postQuery(endpoint, [], [], null, 2)).helloVersion;
            const auth = check(await run([{ Authenticate: {
                username: document.getElementById('username').value,
                password: document.getElementById('password').value } }]));
            token = auth[0].Authenticate.session_token;
            show('Connected.');
            form.querySelector('button').disabled = false;
            await listImages();
        } catch (error) { show(error.message); }
    });
    form.addEventListener('submit', async event => {
        event.preventDefault();
        const button = form.querySelector('button');
        button.disabled = true;
        try {
            const file = document.getElementById('fileupload').files[0];
            if (!file) throw new Error('Select an image first.');
            const options = { properties: { rest_api_example_id: Date.now() } };
            if (version === 2) options.class = document.getElementById('imageclass').value;
            show(check(await run([{ AddImage: options }], [file])));
            await listImages();
        } catch (error) { show(error.message); }
        finally { button.disabled = false; }
    });
}

if (typeof document !== 'undefined') document.addEventListener('DOMContentLoaded', setupExample);
if (typeof module !== 'undefined') module.exports = { decodeMultipart, decodeReply, postQuery };
