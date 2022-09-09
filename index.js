const {Client} = require('@elastic/elasticsearch');
const {username, password} = require('./credentials');
const client = new Client({
    node: 'http://elastic:changeme@localhost:9200'
    // node: `https://${username}:${password}@seven-sixteen.es.westus2.azure.elastic-cloud.com:9243`
});
const config = require('./config');

// Different things to investigate:
// - Lots of data in single index vs. many indices
// - Flattened, nested, join, range types

(async function generateDocs(index = 'es-generate', size = 1) {
    const docs = Array(size).fill(null).map(() => generateDoc(index, config));
    await bulkInsert(docs);
})();

function generate(type, params) {
    const generator = (
        type === 'boolean' ? generateBoolean
        : type === 'integer' ? generateInteger
        : type === 'float' ? generateFloat
        : type === 'date' ? generateDate
        : type === 'ip' ? generateIp
        : type === 'string' ? generateString
        : type === 'text' ? generateText
        : type === 'geo_point' ? generateGeoPoint
        : () => { throw new Error(`Unsupported type: ${type}`); }
    );
    return generator(params);
}

function generateBoolean() {
    return Math.random() < 0.5;
}

function generateInteger({min, max} = {}) {
    return Math.floor(generateFloat({min, max}));
}

function generateFloat({min = -Math.pow(2, 32), max = Math.pow(2, 32)} = {}) {
    return Math.random() * (max - min) + min;
}

function generateDate({min = 0, max = Date.now()} = {}) {
    const ms = generateInteger({min, max});
    return new Date(ms);
}

function generateIp({min = 0, max = Math.pow(2, 8)} = {}) {
    const generateAddress = () => generateInteger({min, max});
    return Array(4).fill(null).map(generateAddress).join('.');
}

function generateString({options = []} = {}) {
    if (options?.length) return options[generateInteger({min: 0, max: options.length})];
    return Math.random().toString(36).substr(2);
}

function generateText({size = 100} = {}) {
    return Array(size).fill(null).map(() => generateString()).join(' ');
}

function generateGeoPoint({minLat = -90, maxLat = 90, minLon = -180, maxLon = 180} = {}) {
    const lat = generateFloat({min: minLat, max: maxLat});
    const lon = generateFloat({min: minLon, max: maxLon});
    return {lat, lon};
}

function generateDoc(index = 'generated', config = {}) {
    const body = Object.keys(config).reduce((doc, field) => {
        const {type, ...params} = config[field];
        const value = generate(type, params);
        return {...doc, [field]: value};
    }, {});
    return {index, body};
}

async function bulkInsert(docs = [], size = 5000) {
    if (docs.length <= 0) return;
    const batch = docs.splice(0, size);
    console.log(`Indexing ${batch.length} docs...`);

    const body = batch.reduce((actions, {index, body}) => actions.concat([
        {index: {_index: index }},
        body
    ]), []);

    try {
        const response = await client.bulk({body});
        console.log(JSON.stringify(response));
    } catch (e) {
        console.log(e);
    }

    if (docs.length) return bulkInsert(docs);
}
