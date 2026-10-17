// Exercise the shipped filter handlers with a synthetic clock and no network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function fixture(now) {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {value:'',textContent:'',innerHTML:'',style:{},attributes:{},
        setAttribute(key,value){this.attributes[key]=value;},addEventListener(){},
        classList:{add:key=>classes.add(key),remove:key=>classes.delete(key),
          toggle:(key,on)=>on ? classes.add(key) : classes.delete(key),contains:key=>classes.has(key)}});
    }
    return elements.get(id);
  };
  class Clock extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
  }
  const context = vm.createContext({Date:Clock, URLSearchParams, setTimeout, clearTimeout,
    window:{addEventListener(){}},document:{addEventListener(){},getElementById:element,querySelectorAll:()=>[]}});
  vm.runInContext(source + '\nlet reads=[]; loadTotals=()=>reads.push("totals"); loadTransactions=()=>reads.push("transactions");', context);
  return {element, run:code=>vm.runInContext(code,context), range:()=>[element('filter-date-from').value,element('filter-date-to').value]};
}

test('Last 7 Days is inclusive, preserves other filters, and refreshes the existing loaders', () => {
  const {run,element,range} = fixture('2026-10-17T12:00:00');
  element('filter-source').value='Synthetic Card';
  element('search').value='groceries';
  element('filter-type').value='expense';
  run('tagFilters.add("Food"); monthFilter="2026-09"; currentPage=3; reviewIndex=2; selectedIds.add(1); setDatePreset("7days")');
  assert.deepEqual(range(),['2026-10-11','2026-10-17']);
  const params = new URLSearchParams(run('buildFilterParams()'));
  assert.equal(params.get('source'),'Synthetic Card');
  assert.equal(params.get('search'),'groceries');
  assert.equal(params.get('transaction_type'),'expense');
  assert.deepEqual(params.getAll('tag'),['Food']);
  assert.equal(run('monthFilter'),'');
  assert.equal(run('currentPage'),1);
  assert.equal(run('reviewIndex'),0);
  assert.equal(run('selectedIds.size'),0);
  assert.deepEqual(Array.from(run('reads')),['totals','transactions']);
  assert.equal(element('preset-7days').attributes['aria-pressed'],'true');
  assert.equal(element('preset-30days').attributes['aria-pressed'],'false');
});

for (const [now,preset,expected] of [
  ['2026-01-03T00:10:00','7days',['2025-12-28','2026-01-03']],
  ['2024-03-01T00:10:00','30days',['2024-02-01','2024-03-01']],
  ['2026-03-09T00:10:00','7days',['2026-03-03','2026-03-09']],
  ['2026-11-02T00:10:00','7days',['2026-10-27','2026-11-02']],
  ['2026-10-17T23:55:00','30days',['2026-09-18','2026-10-17']],
  ['2026-10-17T23:55:00','month',['2026-10-01','2026-10-17']],
  ['2026-01-03T00:10:00','last-month',['2025-12-01','2025-12-31']],
  ['2024-03-01T00:10:00','last-month',['2024-02-01','2024-02-29']],
  ['2026-01-03T00:10:00','3months',['2025-11-01','2026-01-03']],
  ['2026-01-03T00:10:00','year',['2025-01-01','2025-12-31']],
  ['2026-10-17T23:55:00','ytd',['2026-01-01','2026-10-17']],
]) test(`${preset} uses calendar days at ${now}`,()=>{
  const {run,range}=fixture(now);
  run(`setDatePreset(${JSON.stringify(preset)})`);
  assert.deepEqual(range(),expected);
});

test('manual dates, All, and Clear reset shortcut state without stale date filters',()=>{
  const {run,element,range}=fixture('2026-10-17T12:00:00');
  run('setDatePreset("30days")');
  element('filter-date-from').value='2026-05-01';
  run('onDateInput()');
  assert.equal(element('preset-30days').attributes['aria-pressed'],'false');
  assert.deepEqual(range(),['2026-05-01','2026-10-17']);
  run('setDatePreset("all")');
  assert.deepEqual(range(),['','']);
  assert.equal(element('preset-all').attributes['aria-pressed'],'true');
  run('setDatePreset("7days"); clearFilter()');
  assert.deepEqual(range(),['','']);
  assert.equal(element('preset-7days').attributes['aria-pressed'],'false');
});

test('date shortcuts are visible and expose their pressed state',()=>{
  const html=fs.readFileSync(path.join(__dirname,'index.html'),'utf8');
  for (const [id,label] of [['7days','Last 7 Days'],['30days','Last 30 Days']]) {
    assert.match(html,new RegExp(`id="preset-${id}" aria-pressed="false"[^>]*>${label}</button>`));
  }
});
