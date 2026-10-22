// Run the shipped loaders against synthetic responses, never financial records.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function fixture() {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
  const elements = new Map(), reads = [];
  const element = id => {
    if (!elements.has(id)) elements.set(id, {value:'',textContent:'',innerHTML:'',style:{},
      hidden:false,addEventListener(){},classList:{add(){},remove(){},toggle(){}}});
    return elements.get(id);
  };
  const context = vm.createContext({URLSearchParams,setTimeout:()=>0,clearTimeout(){},
    window:{addEventListener(){}},document:{addEventListener(){},getElementById:element,querySelectorAll:()=>[]},
    fetch:url=>new Promise((resolve,reject)=>reads.push({params:new URLSearchParams(url.split('?')[1]),
      reply:data=>resolve({ok:true,json:async()=>data}),fail:()=>reject(new Error('synthetic outage'))}))});
  vm.runInContext(source + '\nrenderCharts=()=>{};', context);
  const run = code => vm.runInContext(code,context);
  const dates = (from,to) => {element('filter-date-from').value=from;element('filter-date-to').value=to;};
  return {run,element,reads,dates};
}

for (const [from,to,priorFrom,priorTo,days] of [
  ['2026-10-16','2026-10-22','2026-10-09','2026-10-15',7],
  ['2026-10-01','2026-10-22','2026-09-09','2026-09-30',22],
  ['2026-01-01','2026-01-01','2025-12-31','2025-12-31',1],
  ['2024-03-01','2024-03-30','2024-01-31','2024-02-29',30],
  ['2026-03-03','2026-03-09','2026-02-24','2026-03-02',7],
  ['2026-10-27','2026-11-02','2026-10-20','2026-10-26',7],
  ['2024-01-01','2024-12-31','2022-12-31','2023-12-31',366],
]) test(`equal inclusive calendar days for ${from} through ${to}`,()=>{
  const {run}=fixture();
  const result=run(`previousComparisonRange(new URLSearchParams({date_from:'${from}',date_to:'${to}'}))`);
  assert.deepEqual(JSON.parse(JSON.stringify(result)),{from:priorFrom,to:priorTo,days});
});

test('unbounded, reversed, malformed and out-of-domain dates have no comparison',()=>{
  const {run}=fixture();
  for (const [from,to] of [['',''],['2026-10-01',''],['','2026-10-22'],['2026-10-22','2026-10-01'],
    ['2026-02-30','2026-03-01'],['2026-1-1','2026-01-02'],['0001-01-01','0001-01-02']])
    assert.equal(run(`previousComparisonRange(new URLSearchParams(${JSON.stringify({date_from:from,date_to:to})}))`),null);
});

test('comparison reuses all current summary filters and preserves the selected dates',async()=>{
  const {run,element,reads,dates}=fixture(); dates('2026-10-16','2026-10-22');
  element('filter-source').value='Demo';element('filter-card').value='1234';
  element('filter-import').value='synthetic.csv';element('search').value='groceries';
  element('filter-type').value='expense';
  run('tagFilters=new Set(["Food","Family"]);tagMatchMode="all"');
  const loading=run('loadStats()');
  assert.equal(reads.length,2);
  for (const read of reads) {
    assert.equal(read.params.get('source'),'Demo');assert.equal(read.params.get('card_last4'),'1234');
    assert.equal(read.params.get('import_file'),'synthetic.csv');assert.equal(read.params.get('search'),'groceries');
    assert.deepEqual(read.params.getAll('tag'),['Food','Family']);assert.equal(read.params.get('tag_match'),'all');
  }
  const previous=reads.find(r=>r.params.get('date_from')==='2026-10-09');
  const current=reads.find(r=>r.params.get('date_from')==='2026-10-16');
  previous.reply({total:100,count:3});current.reply({total:125,count:4});await loading;
  assert.equal(element('card-total').textContent,'$125.00');
  assert.equal(element('card-sub').textContent,'2026-10-16 – 2026-10-22');
  assert.match(element('comparison-result').textContent,/\$100\.00 across 3 transactions\. \$25\.00 more \(25%\)/);
  assert.match(element('comparison-range').textContent,/2026-10-09 – 2026-10-15 · 7 days/);
  assert.equal(element('filter-date-from').value,'2026-10-16');
});

test('all time does not send an extra request or keep a stale comparison',async()=>{
  const {run,reads,element}=fixture();element('comparison-result').textContent='stale';
  const loading=run('loadStats()');assert.equal(reads.length,1);
  reads[0].reply({total:20,count:2});await loading;
  assert.match(element('comparison-result').textContent,/Choose a valid start and end date/);
  assert.equal(element('comparison-range').textContent,'');
  assert.equal(element('card-sub').textContent,'all time');
});

test('zero, negative, absent and invalid baselines avoid misleading percentage changes',()=>{
  const {run}=fixture();
  const comparison=(current,previous)=>run(`periodComparisonText(${JSON.stringify(current)},${JSON.stringify(previous)})`);
  assert.match(comparison({total:20},{total:0,count:0}),/No matching transactions/);
  assert.match(comparison({total:20},{total:0,count:2}),/\$20\.00 more.*zero or negative/);
  assert.match(comparison({total:-20},{total:-40,count:2}),/\$20\.00 more.*zero or negative/);
  assert.match(comparison({total:0},{total:40,count:2}),/\$40\.00 less \(100%\)/);
  assert.match(comparison({total:40},{total:40,count:1}),/1 transaction\. No change/);
  assert.match(comparison({total:0.3},{total:0.1+0.2,count:1}),/No change/);
  assert.match(comparison({total:20},{total:'bad',count:2}),/unavailable/);
  assert.match(comparison({total:20},{total:20,count:-1}),/unavailable/);
});

test('a failed previous request preserves current totals and can recover on refresh',async()=>{
  const {run,reads,element,dates}=fixture();dates('2026-10-16','2026-10-22');
  const first=run('loadStats()');reads[0].fail();reads[1].reply({total:75,count:2});await first;
  assert.equal(element('card-total').textContent,'$75.00');
  assert.match(element('comparison-result').textContent,/Previous period unavailable/);
  const retry=run('loadStats()');reads[2].reply({total:50,count:1});reads[3].reply({total:75,count:2});await retry;
  assert.match(element('comparison-result').textContent,/\$25\.00 more \(50%\)/);
});

test('failed current totals cannot produce a comparison with stale cards',async()=>{
  const {run,reads,element,dates}=fixture();dates('2026-10-16','2026-10-22');
  const first=run('loadStats()');reads[0].reply({total:50,count:1});reads[1].fail();await first;
  assert.match(element('comparison-result').textContent,/until current totals load/);
  assert.equal(element('dashboard-error').hidden,false);
});

test('late previous responses cannot overwrite a newer range or error state',async()=>{
  const {run,reads,element,dates}=fixture();dates('2026-10-16','2026-10-22');
  const first=run('loadStats()');reads[1].reply({total:100,count:2});await new Promise(setImmediate);
  dates('2026-10-01','2026-10-22');const second=run('loadStats()');
  reads[2].reply({total:200,count:2});reads[3].reply({total:300,count:4});await second;
  reads[0].reply({total:999,count:9});await first;
  assert.match(element('comparison-range').textContent,/2026-09-09 – 2026-09-30 · 22 days/);
  assert.match(element('comparison-result').textContent,/\$200\.00 across 2/);
  assert.equal(element('card-total').textContent,'$300.00');
});

test('typing invalidates in-flight comparisons before the search debounce',async()=>{
  const {run,reads,element,dates}=fixture();dates('2026-10-16','2026-10-22');
  const first=run('loadStats()');reads[1].reply({total:100,count:2});await new Promise(setImmediate);
  element('search').value='new';run('scheduleSearch()');
  reads[0].reply({total:999,count:9});await first;
  assert.equal(element('comparison-result').textContent,'Loading comparison…');
});
