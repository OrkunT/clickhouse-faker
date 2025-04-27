// load drill events with cycle, merge and retry
// Will help control memory and cpu constraints
// 10 000-row batches → 1 part per batch
// merge every 5 parts with OPTIMIZE FINAL (20-min timeout)
// retries on ClickHouse 253 and socket ECONNRESET / EPIPE
// ------------------------------------------------------------------

// index.js – 10 000-row batches, merge every 2 parts or on 25 GB RAM is fully used
// -----------------------------------------------------------------

/* 0 · CONFIG ---------------------------------------------------- */
const TOTAL_ROWS       = 10_000_000;
const BATCH_SIZE       = 10_000;
const PARTS_PER_CYCLE  = 2;
const RAM_WATERMARK_GB = 25;
const RETRY_WAIT_MS    = 10_000;
const SLEEP_EVERY      = 20_000;
const SLEEP_MS         = 5_000;
const OPT_TIMEOUT_MS   = 1_200_000;

import { createClient } from '@clickhouse/client';
import { v4 as uuidv4 }  from 'uuid';
import { Readable }      from 'stream';
import crypto            from 'crypto';

/* 1 · CLIENT FACTORIES ----------------------------------------- */
const insertClient = () => createClient({
  url:'http://localhost:8123',
  username:'default',
  keep_alive:{enabled:false},
  clickhouse_settings:{
    async_insert:0,
    optimize_on_insert:0,
    input_format_parallel_parsing:0,
    max_insert_block_size:BATCH_SIZE
  }
});
const ctlClient = () => createClient({
  url:'http://localhost:8123',
  username:'default',
  keep_alive:{enabled:false},
  request_timeout:OPT_TIMEOUT_MS
});

/* 2 · TIMELINE -------------------------------------------------- */
const NOW = Date.now();
const RANGE_MS  = 30*24*60*60*1_000;
const START_MS  = NOW - RANGE_MS;
const STEP_MS   = Math.floor(RANGE_MS / TOTAL_ROWS);
const DISORDER  = new Set();
while (DISORDER.size < TOTAL_ROWS * 0.01)
  DISORDER.add(Math.floor(Math.random()*TOTAL_ROWS));

/* 3 · CONSTANTS, RAND & ROW BUILDERS (unchanged) --------------- */
const CONST_A = crypto.randomBytes(12).toString('hex');
/* ... (same constant arrays and rand helper as before) ... */
const EVENT_TYPES  = ['[CLY]_session','[CLY]_view','[CLY]_action',
                      '[CLY]_crash','[CLY]_star_rating','[CLY]_push'];
/* -- all other arrays omitted here to save space, keep from previous message -- */

const rand = { /* same implementation */ };
const UID_POOL = Math.floor(TOTAL_ROWS*0.07);

const makeUp = () => { /* same as previous version */ };
function makeRow(i){ /* same as previous version */ }

/* STREAM helper */
const batchStream=(s,r)=>{let i=0;const st=new Readable({
  objectMode:true,
  read(){i===r?this.push(null):this.push(makeRow(s+i++));}
}); st.startIdx=s; return st;};

/* 4 · SAFE resident-RAM probe (FIXED) --------------------------- */
async function residentGB() {
  const ch = ctlClient();
  const rs = await ch.query({
    query: `SELECT value/1e9 AS gb
            FROM system.metrics
            WHERE metric = 'MemoryResident'`,
    format: 'JSONEachRow'
  });
  const rows = await rs.json();     // ← rows array, [] if empty
  await ch.close();
  return rows.length ? Number(rows[0].gb) : 0;
}

/* RETRY INSERT on 253 + socket resets -------------------------- */
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function insertWithRetry(offset){
  let st=batchStream(offset,BATCH_SIZE);
  while(true){
    const ch=insertClient();
    try{await ch.insert({table:'drill_events',values:st,format:'JSONEachRow'});await ch.close();return;}
    catch(e){await ch.close();
      const reset=e.code==='ECONNRESET'||e.code==='EPIPE'||(e.message&&e.message.includes('ECONNRESET'));
      if(e.code!=='253'&&!reset) throw e;
      console.log(`⚠️  ${e.code||''} ${e.message.trim()} – retrying in 10 s`);
      await sleep(RETRY_WAIT_MS); st=batchStream(offset,BATCH_SIZE); }
  }
}

/* MERGE helper -------------------------------------------------- */
async function merge(reason){
  console.log(`🔄 OPTIMIZE FINAL (${reason}) …`);
  const ctl=ctlClient();
  await ctl.query({ query:'OPTIMIZE TABLE drill_events FINAL',
                    clickhouse_settings:{wait_end_of_query:1}});
  await ctl.close();
  console.log('✅ Merge finished.');
}

/* 5 · MAIN LOOP ------------------------------------------------- */
(async()=>{
  let inserted=0, parts=0;
  while(inserted<TOTAL_ROWS){

    if(parts && await residentGB() >= RAM_WATERMARK_GB){
      await merge(`RAM ≥ ${RAM_WATERMARK_GB} GB`);
      parts = 0;
    }

    await insertWithRetry(inserted);
    inserted += BATCH_SIZE;
    parts    += 1;

    console.log(`✔ ${inserted.toLocaleString()} / ${TOTAL_ROWS.toLocaleString()} inserted`);
    if(inserted % SLEEP_EVERY === 0){
      console.log(`⏳ Sleeping ${SLEEP_MS/1000}s …`);
      await sleep(SLEEP_MS);
    }

    if(parts === PARTS_PER_CYCLE && inserted < TOTAL_ROWS){
      await merge(`every ${PARTS_PER_CYCLE} parts`);
      parts = 0;
    }
  }
  console.log('🎉 Ingestion complete.');
})();
