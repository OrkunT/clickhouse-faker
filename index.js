// index_no_merge.js  – 10 000-row batches, no merges during ingest
// ----------------------------------------------------------------
//  * Keeps ≤ 2 parts live → arenas ≈ 0.8-1 GiB
//  * Retries on ClickHouse 253 / 241 and socket resets
//  * Prints a safe, disk-spilling OPTIMIZE command when finished

/* 0 · CONFIG ---------------------------------------------------- */
const TOTAL_ROWS        = 10_000_000;
const BATCH_SIZE        = 10_000;          // rows per part
const PARTS_PER_CYCLE   = 2;               // still counted (no merge call)
const RETRY_WAIT_MS     = 10_000;
const SLEEP_EVERY       = 20_000;
const SLEEP_MS          = 5_000;

/* 1 · IMPORTS & CLIENTS ---------------------------------------- */
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

/* 2 · TIMELINE -------------------------------------------------- */
const NOW       = Date.now();
const RANGE_MS  = 30 * 24 * 60 * 60 * 1_000;
const START_MS  = NOW - RANGE_MS;
const STEP_MS   = Math.floor(RANGE_MS / TOTAL_ROWS);

const DISORDER  = new Set();
while (DISORDER.size < TOTAL_ROWS * 0.01)
  DISORDER.add(Math.floor(Math.random() * TOTAL_ROWS));

/* 3 · CONSTANT ARRAYS & RAND ----------------------------------- */
const CONST_A       = crypto.randomBytes(12).toString('hex');
const EVENT_TYPES   = ['[CLY]_session','[CLY]_view','[CLY]_action',
                       '[CLY]_crash','[CLY]_star_rating','[CLY]_push'];
const CMP_CHANNELS  = ['Organic','Direct','Email','Paid'];
const SG_KEYS       = Array.from({ length: 8_000 },
                       (_, i) => `k${(i+1).toString().padStart(4,'0')}`);
const CUSTOM_POOL   = [
  { 'Account Types':'Savings' }, { 'Account Types':'Investment' },
  { 'Communication Preference':'Phone' }, { 'Communication Preference':'Email' },
  { 'Credit Cards':'Premium' },  { 'Credit Cards':'Basic'  },
  { 'Customer Type':'Retail' },  { 'Customer Type':'Business' },
  { 'Total Assets':'$0 - $50,000' },      { 'Total Assets':'$50,000 - $500,000' }
];
const LANG_CODES    = ['en','de','fr','es','pt','ru','zh','ja','ko','hi'];
const COUNTRY_CODES = ['US','DE','FR','ES','PT','RU','CN','JP','KR','IN',
                       'GB','CA','AU','BR','MX'];
const PLATFORMS     = ['Macintosh','Windows','Linux','iOS','Android'];
const OS_NAMES      = ['MacOS','Windows','Android','iOS'];
const RESOLUTIONS   = ['360x640','768x1024','1920x1080'];
const BROWSERS      = ['Chrome','Firefox','Edge','Safari'];
const SOURCES       = ['MacOS','Windows','Android','iOS','Web'];
const SOURCE_CH     = ['Direct','Search','Email','Social'];
const VIEW_NAMES    = ['Settings','Home','Profile','Dashboard',
                       'ProductPage','Checkout'];
const SAMPLE_WORDS  = ['lorem','ipsum','dolor','sit','amet',
                       'consectetur','adipiscing','elit'];
const POSTFIXES     = ['S','V','A'];

const rand = {
  el: a => a[Math.floor(Math.random()*a.length)],
  int:(mn,mx)=>Math.floor(Math.random()*(mx-mn+1))+mn,
  float:(mn,mx,d)=>Number((Math.random()*(mx-mn)+mn).toFixed(d)),
  bool:()=>Math.random()<0.5,
  hex: b=>crypto.randomBytes(b).toString('hex'),
  ts7d:()=>Math.floor((Date.now()-Math.random()*7*24*60*60*1_000)/1_000),
  sub:(a,mn,mx)=>{const n=rand.int(mn,mx),c=a.slice();
    for(let i=c.length-1;i>0;--i){const j=Math.floor(Math.random()*(i+1));[c[i],c[j]]=[c[j],c[i]];}
    return c.slice(0,n);}
};
const UID_POOL = Math.floor(TOTAL_ROWS * 0.07);

/* 3.1 · ROW GENERATORS ----------------------------------------- */
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
function makeRow(i){
  let ts = START_MS + i * STEP_MS;
  if (DISORDER.has(i)) {
    ts += rand.int(-2*STEP_MS, 2*STEP_MS);
    ts  = Math.max(START_MS, Math.min(ts, NOW));
  }
  const uid = i % UID_POOL;
  const _id = `${rand.hex(20)}_${uid}_${ts}`;

  const sgObj = {};
  rand.sub(SG_KEYS, 15, 20).forEach(k => sgObj[k] = rand.el(SAMPLE_WORDS));
  Object.assign(sgObj, { request_id:_id,
                         postfix: rand.el(POSTFIXES),
                         ended: rand.bool().toString() });

  return {
    a: CONST_A, e: rand.el(EVENT_TYPES), uid,
    did: uuidv4(), lsid: _id, _id, ts,
    up: makeUp(), custom: rand.el(CUSTOM_POOL),
    cmp:{ c: rand.el(CMP_CHANNELS) }, sg: sgObj,
    c: rand.int(1,5), s: rand.float(0,1,6), dur: rand.int(100,90_000)
  };
}

/* STREAM helper */
const stream=(off,rows)=>{let i=0;return new Readable({
  objectMode:true,
  read(){ i===rows ? this.push(null) : this.push(makeRow(off+i++)); }
});};

/* 4 · RETRY INSERT (handles 253 / 241 / socket reset) */
const sleep = ms=>new Promise(r=>setTimeout(r,ms));
async function insertPart(offset){
  let st=stream(offset,BATCH_SIZE);
  while(true){
    const ch=insertClient();
    try{
      await ch.insert({table:'drill_events',values:st,format:'JSONEachRow'});
      await ch.close(); return;
    }catch(e){
      await ch.close();
      const reset = e.code==='ECONNRESET'||e.code==='EPIPE'||
                    (e.message&&e.message.includes('ECONNRESET'));
      const mem   = e.code==='241'||/MEMORY_LIMIT_EXCEEDED/.test(e.message);
      if(!reset && !mem && e.code!=='253') throw e;
      console.log(`⚠️  ${e.code||''} ${e.message.trim()} → retry in 10 s`);
      await sleep(RETRY_WAIT_MS);
      st = stream(offset,BATCH_SIZE);
    }
  }
}

/* 5 · MAIN LOOP (no merges during ingest) */
(async()=>{
  let inserted = 0, parts = 0;
  while(inserted < TOTAL_ROWS){
    await insertPart(inserted);
    inserted += BATCH_SIZE; parts += 1;

    console.log(`✔ ${inserted.toLocaleString()} / ${TOTAL_ROWS.toLocaleString()} inserted`);
    if (inserted % SLEEP_EVERY === 0) {
      console.log(`⏳ Sleeping ${SLEEP_MS/1000}s …`);
      await sleep(SLEEP_MS);
    }

    if (parts === PARTS_PER_CYCLE) parts = 0;   // just reset counter
  }

  console.log('\n🎉 Ingestion complete.');
  console.log(
    '\nTo merge the parts later, run:\n' +
    'clickhouse-client -q "' +
    'OPTIMIZE TABLE drill_events FINAL ' +
    'SETTINGS allow_disk_spill_for_merge = 1, max_memory_usage = \'20G\'"' +
    '\n'
  );
})();
