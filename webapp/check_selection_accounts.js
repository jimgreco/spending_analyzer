const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];

function fixture(rows) {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {
      value:'', textContent:'', innerHTML:'', checked:false, open:false, style:{}, dataset:{},
      classList:{add(){},remove(){},toggle(){}}, addEventListener(){},
      showModal(){this.open=true;}, close(){this.open=false;}, appendChild(){}, setAttribute(){},
    });
    return elements.get(id);
  };
  const context = vm.createContext({rows, URLSearchParams, window:{addEventListener(){},location:{reload(){}}},
    fetch:async () => ({ok:true}), document:{body:{dataset:{}}, addEventListener(){},
      getElementById:element, querySelector:element, querySelectorAll:() => []}});
  vm.runInContext(source + `
    tagsCell=typeCell=sourceTag=()=>''; renderSavedViews=()=>{};
    currentUser={id:1,auth_id:10,role:'edit',email:'editor@example.test'};
    renderTable(rows); selectedIds=new Set(rows.map(row=>row.id)); updateBulkBar();
  `, context);
  return {element,run:code => vm.runInContext(code, context)};
}

test('groups only selected page rows by source and card, preserving corrected signed cents', () => {
  const {run,element} = fixture([
    {id:1,source:'Card',card_last4:'0123',amount:'12.34'},
    {id:2,source:'Card',card_last4:'0123',amount:'-2.34',original_amount:'2.34',tags:['Excluded']},
    {id:3,source:'Card',card_last4:'9876',amount:'8.00'},
    {id:4,source:'Checking',amount:'-5.00',transaction_type:'income'},
    {id:5,source:'Card',card_last4:'0123',amount:'900.00'},
  ]);
  run('selectedIds.delete(5); selectedIds.add(999); openSelectionSummary();');
  assert.deepEqual(JSON.parse(run('JSON.stringify(selectedAccountGroups())')), [
    {source:'Card',card:'0123',count:2,positive:1234,negative:-234},
    {source:'Card',card:'9876',count:1,positive:800,negative:0},
    {source:'Checking',card:'',count:1,positive:0,negative:-500},
  ]);
  assert.equal(element('selection-summary-overview').textContent, '4 transactions · 3 accounts · Net $13.00');
  const content = element('selection-summary-accounts').innerHTML;
  assert.match(content, /•••• 0123/);
  assert.match(content, /Negative amounts<\/dt><dd>-\$2\.34/);
  assert.match(content, /Net amount<\/dt><dd>\$10\.00/);
  assert.equal(element('selection-summary-dialog').open, true);
  assert.doesNotMatch(run('selectedTransactionsCsv()'), /900\.00/);
});

test('uses cents, handles zero and missing accounts, and escapes account labels', () => {
  const {run,element} = fixture([
    {id:1,source:'<img src=x onerror=alert(1)>',card_last4:'<b>',amount:0.1},
    {id:2,source:'<img src=x onerror=alert(1)>',card_last4:'<b>',amount:0.2},
    {id:3,source:'<img src=x onerror=alert(1)>',card_last4:'<b>',amount:-0.3},
    {id:4,source:null,card_last4:null,amount:0},
  ]);
  run('openSelectionSummary()');
  assert.equal(element('selection-summary-overview').textContent, '4 transactions · 2 accounts · Net $0.00');
  const content = element('selection-summary-accounts').innerHTML;
  assert.match(content, /Unknown account/);
  assert.match(content, /&lt;img src=x onerror=alert\(1\)&gt;/);
  assert.match(content, /•••• &lt;b&gt;/);
  assert.doesNotMatch(content, /<img|<b>|-\$0\.00/);
});

test('refresh updates an open breakdown and drops missing rows without changing selection intent', () => {
  const {run,element} = fixture([{id:1,source:'Card',amount:20},{id:2,source:'Card',amount:30}]);
  run('openSelectionSummary(); renderTable([{id:1,source:"Updated account",amount:-20}]);');
  assert.equal(element('selection-summary-overview').textContent, '1 transaction · 1 account · Net -$20.00');
  assert.match(element('selection-summary-accounts').innerHTML, /Updated account/);
  assert.deepEqual(Array.from(run('selectedIds')), [1]);
  run('clearSelection()');
  assert.equal(element('selection-summary-dialog').open, false);
  assert.equal(element('selection-summary-overview').textContent, '');
  assert.equal(element('selection-summary-btn').disabled, true);
  run('openSelectionSummary()');
  assert.equal(element('selection-summary-dialog').open, false);
});

test('closing a breakdown preserves existing selection, export and bulk action IDs', () => {
  const {run,element} = fixture([{id:1,source:'Card',amount:4}]);
  run('openSelectionSummary(); closeSelectionSummary();');
  assert.equal(element('selection-summary-dialog').open, false);
  assert.deepEqual(Array.from(run('selectedIds')), [1]);
  assert.match(run('selectedTransactionsCsv()'), /,4\.00,/);
  assert.equal(element('bulk-total').textContent, 'Selected amount: $4.00');
});

test('viewer and signed-out users cannot open the selection breakdown', () => {
  for (const user of ["{id:1,role:'read'}", 'null']) {
    const {run,element} = fixture([{id:1,amount:4}]);
    run(`currentUser=${user}; renderTable(rows); openSelectionSummary();`);
    assert.equal(element('selection-summary-dialog').open, false);
    assert.equal(element('selection-summary-btn').disabled, true);
  }
});

test('account or actor changes clear the open breakdown and selection before new rows arrive', () => {
  for (const user of ["{id:2,auth_id:10,role:'edit'}", "{id:1,auth_id:11,role:'edit'}", "{id:1,auth_id:10,role:'read'}"]) {
    const {run,element} = fixture([{id:1,amount:4}]);
    run(`openSelectionSummary(); showUserHeader({...${user},email:'next@example.test'}); openSelectionSummary();`);
    assert.equal(element('selection-summary-dialog').open, false);
    assert.equal(element('selection-summary-accounts').textContent, '');
    assert.equal(run('selectedIds.size'), 0);
  }
});

test('auth refresh requires fresh rows before the existing selection can be summarized again', () => {
  const {run,element} = fixture([{id:1,amount:4}]);
  run('openSelectionSummary(); showUserHeader({...currentUser}); openSelectionSummary();');
  assert.equal(element('selection-summary-dialog').open, false);
  assert.equal(element('selection-summary-btn').disabled, true);
  assert.equal(run('selectedIds.size'), 1);
  run('renderTable(rows); openSelectionSummary();');
  assert.equal(element('selection-summary-dialog').open, true);
});

test('an old transaction response cannot re-enable a previous account breakdown', async () => {
  const {run,element} = fixture([{id:1,amount:4}]);
  run('api=()=>new Promise(resolve=>globalThis.complete=resolve); globalThis.pending=loadTransactions(); showUserHeader({id:2,auth_id:20,role:"edit",email:"next@example.test"});');
  await run('complete({transactions:[{id:1,amount:999}],page:1,pages:1,total:1}); pending;');
  run('selectedIds.add(1); openSelectionSummary()');
  assert.equal(element('selection-summary-dialog').open, false);
  assert.equal(run('currentTxs[0].amount'), 4);
});

test('logout immediately clears the open breakdown', async () => {
  const {run,element} = fixture([{id:1,amount:4}]);
  await run('openSelectionSummary(); logout();');
  assert.equal(element('selection-summary-dialog').open, false);
  assert.equal(element('selection-summary-overview').textContent, '');
});
