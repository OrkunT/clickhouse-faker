// load drill events with cycle, merge and retry
// Will help control memory and cpu constraints
// 10 000-row batches → 1 part per batch
// merge every 5 parts with OPTIMIZE FINAL (20-min timeout)
// retries on ClickHouse 253 and socket ECONNRESET / EPIPE
// ------------------------------------------------------------------

/* 0. PARAMETERS */
const TOTAL_ROWS      = 10_000_000;
const BATCH_SIZE      = 10_000;
const PARTS_PER_CYCLE = 5;
const SLEEP_EVERY     = 20_000;
const SLEEP_MS        = 5_000;
const RETRY_WAIT_MS   = 10_000;

/* 1. IMPORTS & CLIENT FACTORIES */
import { createClient } from '@clickhouse/client';
import { v4 as uuidv4 } from 'uuid';
import { Readable } from 'stream';
import crypto from 'crypto';

function insertClient() {
  return createClient({
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
}
function optimizeClient() {
  return createClient({
    url: 'http://localhost:8123',
    username: 'default',
    keep_alive: { enabled: false },
    request_timeout: 1_200_000          // 20-minute timeout
  });
}

/* 2. TIMELINE */
const NOW      = Date.now();            // FIXED name
const RANGE_MS = 30 * 24 * 60 * 60 * 1_000;
const START_MS = NOW - RANGE_MS;
const STEP_MS  = Math.floor(RANGE_MS / TOTAL_ROWS);

const DISORDER = new Set();
while (DISORDER.size < Math.floor(TOTAL_ROWS * 0.01)) {
  DISORDER.add(Math.floor(Math.random() * TOTAL_ROWS));
}

/* 3. CONSTANT ARRAYS & RAND HELPERS */
const CONST_A      = crypto.randomBytes(12).toString('hex');
const EVENT_TYPES  = ['[CLY]_session','[CLY]_view','[CLY]_action',
                      '[CLY]_crash','[CLY]_star_rating','[CLY]_push'];
const CMP_CHANNELS = ['Organic','Direct','Email','Paid'];
const SG_KEYS      = Array.from({ length: 8_000 },
                      (_, i) => `k${(i+1).toString().padStart(4,'0')}`);
const CUSTOM_POOL  = [
  { 'Account Types':'Savings' }, { 'Account Types':'Investment' },
  { 'Communication Preference':'Phone' }, { 'Communication Preference':'Email' },
  { 'Credit Cards':'Premium' }, { 'Credit Cards':'Basic' },
  { 'Customer Type':'Retail' }, { 'Customer Type':'Business' },
  { 'Total Assets':'$0 - $50,000' }, { 'Total Assets':'$50,000 - $500,000' }
];
const LANG_CODES   = ['en','de','fr','es','pt','ru','zh','ja','ko','hi'];
const COUNTRY_CODES= ['US','DE','FR','ES','PT','RU','CN','JP','KR','IN',
                      'GB','CA','AU','BR','MX'];
const PLATFORMS    = ['Macintosh','Windows','Linux','iOS','Android'];
const OS_NAMES     = ['MacOS','Windows','Android','iOS'];
const RESOLUTIONS  = ['360x640','768x1024','1920x1080'];
const BROWSERS     = ['Chrome','Firefox','Edge','Safari'];
const SOURCES      = ['MacOS','Windows','Android','iOS','Web'];
const SOURCE_CH    = ['Direct','Search','Email','Social'];
const VIEW_NAMES   = ['Settings','Home','Profile','Dashboard',
                      'ProductPage','Checkout'];
const SAMPLE_WORDS = ['lorem','ipsum','dolor','sit','amet',
                      'consectetur','adipiscing','elit'];
const POSTFIXES    = ['S','V','A'];

const rand = {
  el:a=>a[Math.floor(Math.random()*a.length)],
  int:(mn,mx)=>Math.floor(Math.random()*(mx-mn+1))+mn,
  float:(mn,mx,d)=>Number((Math.random()*(mx-mn)+mn).toFixed(d)),
  bool:()=>Math.random()<0.5,
  hex:b=>crypto.randomBytes(b).toString('hex'),
  ts7d:()=>Math.floor((Date.now()-Math.random()*7*24*60*60*1_000)/1_000),
  sub:(a,mn,mx)=>{
    const n=rand.int(mn,mx), arr=a.slice();
    for(let i=arr.length-1;i>0;--i){
      const j=Math.floor(Math.random()*(i+1));
      [arr[i],arr[j]]=[arr[j],arr[i]];
    }
    return arr.slice(0,n);
  }
};

const UID_POOL = Math.floor(TOTAL_ROWS * 0.07);

/* 3.1 ROW GENERATORS */
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

function makeRow(idx) {
  let ts = START_MS + idx * STEP_MS;
  if (DISORDER.has(idx)) {
    ts += rand.int(-2*STEP_MS, 2*STEP_MS);
    ts = Math.max(START_MS, Math.min(ts, NOW));   // FIXED var
  }
  const uid = idx % UID_POOL;
  const _id = `${rand.hex(20)}_${uid}_${ts}`;

  const sgSel = rand.sub(SG_KEYS, 15, 20);
  const sgObj = {};
  sgSel.forEach(k => (sgObj[k] = rand.el(SAMPLE_WORDS)));
  Object.assign(sgObj, {
    request_id:_id,
    postfix: rand.el(POSTFIXES),
    ended: rand.bool().toString()
  });

  return {
    a: CONST_A,
    e: rand.el(EVENT_TYPES),
    uid,
    did: uuidv4(),
    lsid: _id,
    _id,
    ts,
    up: makeUp(),                 // Object
    custom: rand.el(CUSTOM_POOL),
    cmp: { c: rand.el(CMP_CHANNELS) },
    sg: sgObj,                    // Object
    c: rand.int(1,5),
    s: rand.float(0,1,6),
    dur: rand.int(100,90_000)
  };
}

/* STREAM helper */
function batchStream(start, rows) {
  let i = 0;
  const stream = new Readable({
    objectMode: true,
    read() { i === rows ? this.push(null) : this.push(makeRow(start + i++)); }
  });
  stream.startIdx = start;
  return stream;
}

/* 4. RETRY INSERT (handles 253 + socket resets) */
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function insertWithRetry(startIdx) {
  let stream = batchStream(startIdx, BATCH_SIZE);
  while (true) {
    const ch = insertClient();
    try {
      await ch.insert({ table:'drill_events', values:stream, format:'JSONEachRow' });
      await ch.close();
      return;                          // ok
    } catch (err) {
      await ch.close();
      const reset = (err.code==='ECONNRESET'||err.code==='EPIPE'||
                    (err.message && err.message.includes('ECONNRESET')));
      if (err.code!=='253' && !reset) throw err;

      console.log(`⚠️  ${err.code||''} ${err.message.trim()} – retry in 10 s`);
      await sleep(RETRY_WAIT_MS);
      stream = batchStream(startIdx, BATCH_SIZE);   // rebuild
    }
  }
}

/* 5. MAIN LOOP */
(async () => {
  let inserted = 0;
  const batchesPerCycle = PARTS_PER_CYCLE;
  const cycles = Math.ceil(TOTAL_ROWS / (BATCH_SIZE * batchesPerCycle));

  for (let cyc = 0; cyc < cycles; ++cyc) {

    for (let p = 0; p < batchesPerCycle && inserted < TOTAL_ROWS; ++p) {
      await insertWithRetry(inserted);
      inserted += BATCH_SIZE;

      console.log(`✔ ${inserted.toLocaleString()} / ${TOTAL_ROWS.toLocaleString()} inserted`);
      if (inserted % SLEEP_EVERY === 0) {
        console.log(`⏳ Sleeping ${SLEEP_MS/1_000}s …`);
        await sleep(SLEEP_MS);
      }
    }

    if (inserted < TOTAL_ROWS) {
      console.log('🔄 OPTIMIZE FINAL (merging last 5 parts) …');
      const ctl = optimizeClient();
      await ctl.query({
        query: 'OPTIMIZE TABLE drill_events FINAL',
        clickhouse_settings: { wait_end_of_query: 1 }
      });
      await ctl.close();
      console.log('✅ Merge finished. Continuing ingestion.');
    }
  }

  console.log('🎉 Ingestion complete.');
})();
