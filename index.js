// load drill events with cycle, merge and retry
// Will help control memory and cpu constraints
// 10 000-row batches → 1 part per batch
// merge every 5 parts with OPTIMIZE FINAL (20-min timeout)
// retries on ClickHouse 253 and socket ECONNRESET / EPIPE
// ------------------------------------------------------------------

// index.js  — 10 k-row batches, adaptive merge, safe-RAM guard
// ------------------------------------------------------------------

/* 0 · PARAMETERS -------------------------------------------------- */
const TOTAL_ROWS       = 10_000_000;
const BATCH_SIZE       = 10_000;      // rows per part
const PARTS_PER_CYCLE  = 2;           // scheduled merge after N parts
const RAM_WATERMARK_GB = 25;          // early merge when RAM ≥ 25 GB
const SAFE_RAM_GB      = 24;          // resume inserts only below this
const RETRY_WAIT_MS    = 10_000;      // back-off on retryable errors
const SLEEP_EVERY      = 20_000;      // progress throttle
const SLEEP_MS         = 5_000;
const OPT_TIMEOUT_MS   = 1_200_000;   // 20 min timeout for OPTIMIZE

/* 1 · IMPORTS & CLIENT FACTORIES --------------------------------- */
import { createClient } from '@clickhouse/client';
import { v4 as uuidv4 }  from 'uuid';
import { Readable }      from 'stream';
import crypto            from 'crypto';

const insertClient = () => createClient({
  url: 'http://localhost:8123',
  username: 'default',
  keep_alive: { enabled: false },
  clickhouse_settings: {
    async_insert: 0,
    optimize_on_insert: 0,
    input_format_parallel_parsing: 0,
    max_insert_block_size: BATCH_SIZE
  }
});
const ctlClient = () => createClient({
  url: 'http://localhost:8123',
  username: 'default',
  keep_alive: { enabled: false },
  request_timeout: OPT_TIMEOUT_MS
});

/* 2 · TIMELINE ---------------------------------------------------- */
const NOW       = Date.now();
const RANGE_MS  = 30 * 24 * 60 * 60 * 1_000;
const START_MS  = NOW - RANGE_MS;
const STEP_MS   = Math.floor(RANGE_MS / TOTAL_ROWS);
const DISORDER  = new Set();
while (DISORDER.size < TOTAL_ROWS * 0.01)
  DISORDER.add(Math.floor(Math.random() * TOTAL_ROWS));

/* 3 · CONSTANT ARRAYS & RAND ------------------------------------- */
const CONST_A = crypto.randomBytes(12).toString('hex');
const EVENT_TYPES  = ['[CLY]_session','[CLY]_view','[CLY]_action',
                      '[CLY]_crash','[CLY]_star_rating','[CLY]_push'];
const CMP_CHANNELS = ['Organic','Direct','Email','Paid'];
const SG_KEYS      = Array.from({ length: 8_000 },
                      (_, i) => `k${(i+1).toString().padStart(4,'0')}`);
const CUSTOM_POOL  = [
  { 'Account Types':'Savings' }, { 'Account Types':'Investment' },
  { 'Communication Preference':'Phone' }, { 'Communication Preference':'Email' },
  { 'Credit Cards':'Premium' },  { 'Credit Cards':'Basic'  },
  { 'Customer Type':'Retail' },  { 'Customer Type':'Business' },
  { 'Total Assets':'$0 - $50,000' },      { 'Total Assets':'$50,000 - $500,000' }
];
const LANG_CODES    = ['en','de','fr','es','pt','ru','zh','ja','ko','hi'];
const COUNTRY_CODES = ['US','DE','FR','ES','PT','RU','CN','JP','KR','IN',
                       'GB','CA','AU','BR','MX'];
const PLATFORMS  = ['Macintosh','Windows','Linux','iOS','Android'];
const OS_NAMES   = ['MacOS','Windows','Android','iOS'];
const RESOLUTIONS= ['360x640','768x1024','1920x1080'];
const BROWSERS   = ['Chrome','Firefox','Edge','Safari'];
const SOURCES    = ['MacOS','Windows','Android','iOS','Web'];
const SOURCE_CH  = ['Direct','Search','Email','Social'];
const VIEW_NAMES = ['Settings','Home','Profile','Dashboard',
                    'ProductPage','Checkout'];
const SAMPLE_WORDS = ['lorem','ipsum','dolor','sit','amet',
                      'consectetur','adipiscing','elit'];
const POSTFIXES  = ['S','V','A'];

const rand = {
  el: a => a[Math.floor(Math.random()*a.length)],
  int:(mn,mx)=>Math.floor(Math.random()*(mx-mn+1))+mn,
  float:(mn,mx,d)=>Number((Math.random()*(mx-mn)+mn).toFixed(d)),
  bool:()=>Math.random()<0.5,
  hex:b=>crypto.randomBytes(b).toString('hex'),
  ts7d:()=>Math.floor((Date.now()-Math.random()*7*24*60*60*1_000)/1_000),
  sub:(a,mn,mx)=>{const n=rand.int(mn,mx),c=a.slice();
    for(let i=c.length-1;i>0;--i){const j=Math.random()*(i+1)|0;[c[i],c[j]]=[c[j],c[i]];}
    return c.slice(0,n);}
};
const UID_POOL = Math.floor(TOTAL_ROWS * 0.07);

/* 3.1 · ROW GENERATORS ------------------------------------------ */
const makeUp = () => {
  const br = rand.el(BROWSERS);
  return {
    fs: rand.ts7d(), ls: rand.ts7d(), sc: rand.int(1,3),
    d : rand.el(PLATFORMS), cty:'Unknown', rgn:'Unknown',
    cc: rand.el(COUNTRY_CODES), p: rand.el(OS_NAMES),
    pv:`o${rand.int(10,13)}:${rand.int(0,5)}`,
    av:`${rand.int(1,6)}:${rand.int(0,10)}:${rand.int(0,10)}`,
    c :'Unknown', r: rand.el(RESOLUTIONS), brw: br,
    brwv:`[${br}]_${rand.int(100,140)}:0:0:0`,
    la: rand.el(LANG_CODES), src: rand.el(SOURCES),
    src_ch: rand.el(SOURCE_CH), lv: rand.el(VIEW_NAMES),
    hour: rand.int(0,23), dow: rand.int(0,6)
  };
};

function makeRow(i) {
  let ts = START_MS + i * STEP_MS;
  if (DISORDER.has(i)) {
    ts += rand.int(-2*STEP_MS, 2*STEP_MS);
    ts  = Math.max(START_MS, Math.min(ts, NOW));
  }
  const uid = i % UID_POOL;
  const _id = `${rand.hex(20)}_${uid}_${ts}`;

  const sgObj = {};
  rand.sub(SG_KEYS, 15, 20).forEach(k => sgObj[k] = rand.el(SAMPLE_WORDS));
  Object.assign(sgObj, {
    request_id: _id,
    postfix: rand.el(POSTFIXES),
    ended: rand.bool().toString()
  });

  return {
    a: CONST_A, e: rand.el(EVENT_TYPES), uid,
    did: uuidv4(), lsid: _id, _id, ts,
    up: makeUp(), custom: rand.el(CUSTOM_POOL),
    cmp:{ c: rand.el(CMP_CHANNELS) }, sg: sgObj,
    c: rand.int(1,5), s: rand.float(0,1,6), dur: rand.int(100,90_000)
  };
}

/* STREAM helper */
function batchStream(offset, rows) {
  let i = 0;
  const s = new Readable({
    objectMode: true,
    read() { i === rows ? this.push(null) : this.push(makeRow(offset + i++)); }
  });
  return s;
}

/* 4 · SAFE resident-RAM probe ----------------------------------- */
async function residentGB() {
  const ch = ctlClient();
  const rs = await ch.query({
    query: `SELECT value/1e9 AS gb
            FROM system.metrics
            WHERE metric = 'MemoryResident'`,
    format: 'JSONEachRow'
  });
  const rows = await rs.json();      // rows = [] if metric missing
  await ch.close();
  return rows.length ? Number(rows[0].gb) : 0;
}

/* wait until RAM < SAFE_RAM_GB ---------------------------------- */
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function waitSafeRAM() {
  while (await residentGB() >= SAFE_RAM_GB) await sleep(1_000);
}

/* insert with retry --------------------------------------------- */
async function insertWithRetry(offset) {
  let st = batchStream(offset, BATCH_SIZE);
  while (true) {
    const ch = insertClient();
    try {
      await ch.insert({ table:'drill_events', values:st, format:'JSONEachRow' });
      await ch.close();
      return;
    } catch (e) {
      await ch.close();
      const reset = e.code==='ECONNRESET'||e.code==='EPIPE'||
                    (e.message && e.message.includes('ECONNRESET'));
      const mem   = e.code==='241'||/MEMORY_LIMIT_EXCEEDED/.test(e.message);
      if (!reset && !mem && e.code!=='253') throw e;

      console.log(`⚠️  ${e.code||''} ${e.message.trim()} – retry in 10 s`);
      await sleep(RETRY_WAIT_MS);
      await waitSafeRAM();           // make sure RAM is safe before retry
      st = batchStream(offset, BATCH_SIZE);
    }
  }
}

/* merge helper -------------------------------------------------- */
async function merge(reason) {
  console.log(`🔄 OPTIMIZE FINAL (${reason}) …`);
  const ctl = ctlClient();
  await ctl.query({ query:'OPTIMIZE TABLE drill_events FINAL',
                    clickhouse_settings:{wait_end_of_query:1}});
  await ctl.close();
  console.log('✅ Merge finished. Waiting for RAM to drop …');
  await waitSafeRAM();
}

/* 5 · MAIN LOOP ------------------------------------------------- */
(async () => {
  let inserted = 0;
  let parts    = 0;

  while (inserted < TOTAL_ROWS) {

    /* early merge on RAM watermark */
    if (parts && await residentGB() >= RAM_WATERMARK_GB) {
      await merge(`RAM ≥ ${RAM_WATERMARK_GB} GB`);
      parts = 0;
    }

    /* insert one part */
    await insertWithRetry(inserted);
    inserted += BATCH_SIZE;
    parts    += 1;

    console.log(`✔ ${inserted.toLocaleString()} / ${TOTAL_ROWS.toLocaleString()} inserted`);
    if (inserted % SLEEP_EVERY === 0) {
      console.log(`⏳ Sleeping ${SLEEP_MS/1000}s …`);
      await sleep(SLEEP_MS);
    }

    /* scheduled merge every 2 parts */
    if (parts === PARTS_PER_CYCLE && inserted < TOTAL_ROWS) {
      await merge(`every ${PARTS_PER_CYCLE} parts`);
      parts = 0;
    }
  }
  console.log('🎉 Ingestion complete.');
})();
