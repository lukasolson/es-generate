const {Client} = require('@elastic/elasticsearch');
const datemath = require('@elastic/datemath');
const { appendFileSync } = require('fs');
const config = require('./config');

const nodes = [
    // {node: 'http://elastic:changeme@localhost:9200'},
    // {node: 'http://elastic_serverless:changeme@localhost:9200'},
    // {node: 'CLOUD_URL', auth: {apiKey: 'MY_API_KEY'}},
];
const clients = nodes.map(node => new Client(node));
const STRINGS = ['a','ac','accumsan','ad','adipiscing','aenean','aliquam','aliquet','amet','ante','aptent','arcu','at','auctor','augue','bibendum','blandit','class','commodo','condimentum','congue','consectetur','consequat','conubia','convallis','cras','cubilia','curabitur','curae','cursus','dapibus','diam','dictum','dictumst','dignissim','dis','dolor','donec','dui','duis','efficitur','egestas','eget','eleifend','elementum','elit','enim','erat','eros','est','et','etiam','eu','euismod','ex','facilisi','facilisis','fames','faucibus','felis','fermentum','feugiat','finibus','fringilla','fusce','gravida','habitant','habitasse','hac','hendrerit','himenaeos','iaculis','id','imperdiet','in','inceptos','integer','interdum','ipsum','justo','lacinia','lacus','laoreet','lectus','leo','libero','ligula','litora','lobortis','lorem','luctus','maecenas','magna','magnis','malesuada','massa','mattis','mauris','maximus','metus','mi','molestie','mollis','montes','morbi','mus','nam','nascetur','natoque','nec','neque','netus','nibh','nisi','nisl','non','nostra','nulla','nullam','nunc','odio','orci','ornare','parturient','pellentesque','penatibus','per','pharetra','phasellus','placerat','platea','porta','porttitor','posuere','potenti','praesent','pretium','primis','proin','pulvinar','purus','quam','quis','quisque','rhoncus','ridiculus','risus','rutrum','sagittis','sapien','scelerisque','sed','sem','semper','senectus','sit','sociosqu','sodales','sollicitudin','suscipit','suspendisse','taciti','tellus','tempor','tempus','tincidunt','torquent','tortor','tristique','turpis','ullamcorper','ultrices','ultricies','urna','ut','varius','vehicula','vel','velit','venenatis','vestibulum','vitae','vivamus','viverra','volutpat','vulputate'];

// Different things to investigate:
// - Lots of data in single index vs. many indices
// - Flattened, nested, join, range types

(async function generateDocs(index = 'logs-generated', size = 1000000) {
    const docs = Array(size).fill(null).map(() => generateDoc(index, config));
    await bulkInsert(docs);
    // writeBulkToFile(docs);
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
    min = typeof min === 'string' ? datemath.parse(min).valueOf() : min;
    max = typeof max === 'string' ? datemath.parse(max).valueOf() : max;
    const ms = generateInteger({min, max});
    return new Date(ms);
}

function generateIp({min = 0, max = Math.pow(2, 8)} = {}) {
    const generateAddress = () => generateInteger({min, max});
    return Array(4).fill(null).map(generateAddress).join('.');
}

function generateString({options = []} = {}) {
    if (options?.length) return options[generateInteger({min: 0, max: options.length})];
    return STRINGS[generateInteger({min: 0, max: STRINGS.length})];
    // return Math.random().toString(36).substr(2);
}

function generateText({size = 100} = {}) {
    const count = generateInteger({min: 0, max: size});
    return Array(count).fill(null).map(() => generateString()).join(' ');
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

async function bulkInsert(docs = [], size = 1000) {
    if (docs.length <= 0) return;
    const batch = docs.splice(0, size);
    console.log(`Indexing ${batch.length} docs...`);

    const body = batch.reduce((actions, {index, body, id}) => actions.concat([
        {index: {_index: index, _id: id }},
        body
    ]), []);

    try {
        const responses = clients.map(client => client.bulk({body}));
        await Promise.all(responses);
    } catch (e) {
        console.log(e);
    }

    if (docs.length) return bulkInsert(docs);
}

// If the config results in an "Invalid string length" error, try using this method in conjunction with a curl request:
// curl -s -H "Content-Type: application/x-ndjson" -XPOST elastic:changeme@localhost:9200/_bulk --data-binary "@bulk.ndjson"; echo
async function writeBulkToFile(docs) {
    docs.forEach(({index, body, id}, i) => {
        const action = {index: {_index: index, _id: id }};
        appendFileSync('/Users/lukas/Downloads/bulk.ndjson', `${JSON.stringify(action)}\n${JSON.stringify(body)}\n`);
    })
}
