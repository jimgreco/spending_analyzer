const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];

function fixture(rows, role = 'edit') {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) {
      let content = '';
      elements.set(id, {
        value:'', open:false, checked:false, style:{}, dataset:{}, listeners:{},
        get innerHTML(){ return content; }, set innerHTML(value){ content = value; },
        get textContent(){ return content; }, set textContent(value){ content = value; },
        classList:{add(){},remove(){},toggle(){}},
        addEventListener(name, callback){ this.listeners[name] = callback; },
        showModal(){ this.open = true; }, close(){ this.open = false; },
        appendChild(){}, setAttribute(){}, scrollIntoView(){},
      });
    }
    return elements.get(id);
  };
  const buttons = rows.map(() => ({disabled:false}));
  const requests = [];
  const context = vm.createContext({rows, role, requests, URLSearchParams,
    location:{hash:'#analyze'}, window:{addEventListener(){},scrollTo(){},location:{reload(){}}},
    fetch:async (url, options) => { requests.push({url,options}); return {ok:true}; },
    document:{body:{dataset:{}},addEventListener(){},getElementById:element,querySelector:element,
      querySelectorAll:selector => selector === '.transaction-details-open' ? buttons : []}});
  vm.runInContext(source + `
    tagsCell=typeCell=sourceTag=()=>''; renderSavedViews=()=>{};
    currentUser={id:1,auth_id:10,role,email:'viewer@example.test'};
    renderTable(rows);
  `, context);
  return {element,buttons,requests,run:code => vm.runInContext(code, context)};
}

const sample = {id:1,date:'2026-10-28',description:'Synthetic purchase',amount:'-25.50',original_amount:'25.50',
  source:'Example source',import_file:'Exact_original_filename.csv',status:'active',primary_tag:'Groceries',
  primary_tag_implicit:['Food'],tags:['Shared'],transaction_type:'refund',needs_review:true,
  suggested_tag:'Household',categorization_reason:'A mixed purchase needs context.',correction_note:'One-time purchase.'};

test('viewers can inspect complete current row metadata without a fetch or mutation', () => {
  const {run,element,requests} = fixture([sample], 'read');
  run('openTransactionDetails(1)');
  const content = element('transaction-details-content').innerHTML;
  for (const text of ['Synthetic purchase','-$25.50','2026-10-28','Account source','Example source',
    'Exact_original_filename.csv','Groceries','Food','Shared','Refund','Active','Household',
    'A mixed purchase needs context.','One-time purchase.','sign has been corrected',
    'Total Spent excludes rows','sign alone does not determine transaction type']) assert.ok(content.includes(text), text);
  assert.equal(element('transaction-details-dialog').open, true);
  assert.deepEqual(requests, []);
  assert.equal(run('JSON.stringify(currentTxs[0])'), JSON.stringify(sample));
  assert.doesNotMatch(content, /<input|<select|<textarea|onclick=/);
  assert.match(element('tx-body').innerHTML, /View details for Synthetic purchase/);
});

test('all imported strings are escaped, and absent metadata is described without guessing', () => {
  const hostile = '<img src=x onerror=alert(1)>';
  const {run,element} = fixture([{...sample,description:hostile,source:hostile,import_file:hostile,
    primary_tag:hostile,primary_tag_implicit:[hostile],tags:[hostile],suggested_tag:hostile,
    categorization_reason:hostile,correction_note:hostile}]);
  run('openTransactionDetails(1)');
  assert.doesNotMatch(element('transaction-details-content').innerHTML, /<img/);
  assert.match(element('transaction-details-content').innerHTML, /&lt;img/);
  run('renderTable([{id:2,amount:0}]); openTransactionDetails(2)');
  const content = element('transaction-details-content').innerHTML;
  for (const text of ['No description','Not provided','Unassigned','Unreviewed','None','$0.00']) assert.ok(content.includes(text));
  assert.doesNotMatch(content, /Needs category review|sign has been corrected|undefined|NaN|••••/);
});

test('status explains omitted totals, and review suggestions are separate from assigned category', () => {
  for (const [status,expected] of [['deleted','deleted.'],['deduped','a duplicate.']]) {
    const {run,element} = fixture([{...sample,status,primary_tag:null,suggested_tag:'Suggested only'}]);
    run('openTransactionDetails(1)');
    const content = element('transaction-details-content').innerHTML;
    assert.ok(content.includes('not counted in Total Spent because it is ' + expected));
    assert.match(content, /Primary category<\/dt><dd>Unassigned/);
    assert.match(content, /Suggested category: Suggested only/);
  }
});

test('close, Escape close event and repeated opening clear details without changing selection', () => {
  const {run,element} = fixture([sample,{...sample,id:2,description:'Second row'}]);
  run('selectedIds.add(1); openTransactionDetails(1); closeTransactionDetails();');
  assert.equal(element('transaction-details-content').textContent, '');
  assert.deepEqual(Array.from(run('selectedIds')), [1]);
  run('openTransactionDetails(2)');
  element('transaction-details-dialog').listeners.close(); // delayed event for row 1
  assert.match(element('transaction-details-content').innerHTML, /Second row/);
  element('transaction-details-dialog').open = false; // native Escape dismissal
  element('transaction-details-dialog').listeners.close();
  assert.equal(element('transaction-details-content').textContent, '');
  for (let i = 0; i < 5; i++) run('openTransactionDetails(1); closeTransactionDetails();');
  assert.deepEqual(Array.from(run('selectedIds')), [1]);
});

test('new loads close details immediately and interrupted responses cannot reopen stale rows', async () => {
  const {run,element,buttons} = fixture([sample]);
  run('api=()=>new Promise(resolve=>globalThis.finish=resolve); openTransactionDetails(1); globalThis.pending=loadTransactions(); openTransactionDetails(1);');
  assert.equal(element('transaction-details-dialog').open, false);
  assert.equal(element('transaction-details-content').textContent, '');
  assert.equal(buttons[0].disabled, true);
  run('globalThis.oldFinish=finish; globalThis.oldPending=pending; globalThis.pending=loadTransactions();');
  await run('oldFinish({transactions:[{id:1,amount:999}],page:1,pages:1,total:1}); oldPending;');
  run('openTransactionDetails(1)');
  assert.equal(element('transaction-details-dialog').open, false);
  await run('finish({transactions:[{id:2,description:"New page",amount:2}],page:1,pages:1,total:1}); pending;');
  run('openTransactionDetails(1)');
  assert.equal(element('transaction-details-dialog').open, false);
  run('openTransactionDetails(2)');
  assert.match(element('transaction-details-content').innerHTML, /New page/);
});

test('failed loads keep previous details inaccessible until a successful refresh', async () => {
  const {run,element} = fixture([sample]);
  await run('openTransactionDetails(1); api=async()=>{throw Error("offline")}; loadTransactions();');
  run('openTransactionDetails(1)');
  assert.equal(element('transaction-details-dialog').open, false);
  assert.equal(element('transaction-details-content').textContent, '');
  assert.match(element('dashboard-error-text').textContent, /Could not load transactions/);
  run('renderTable(rows); openTransactionDetails(1)');
  assert.equal(element('transaction-details-dialog').open, true);
});

test('page, filter and sort changes clear details before waiting for the next response', () => {
  for (const action of ['currentPage=1;totalPages=2;changePage(1)', 'applyFilters()', 'sortBy("amount")', 'changePageSize(25)']) {
    const {run,element} = fixture([sample]);
    run(`loadTotals=()=>{}; api=()=>new Promise(()=>{}); openTransactionDetails(1); ${action}; openTransactionDetails(1);`);
    assert.equal(element('transaction-details-dialog').open, false, action);
    assert.equal(element('transaction-details-content').textContent, '', action);
  }
});

test('actor, dataset and auth refresh boundaries reject rows from the earlier session', async () => {
  for (const identity of [{id:2,auth_id:10}, {id:1,auth_id:11}, {id:1,auth_id:10}]) {
    const {run,element} = fixture([sample]);
    run(`api=()=>new Promise(resolve=>globalThis.finish=resolve); openTransactionDetails(1); globalThis.pending=loadTransactions(); showUserHeader({...${JSON.stringify(identity)},role:'read',email:'new@example.test'});`);
    await run('finish({transactions:[{id:1,amount:999}],page:1,pages:1,total:1}); pending;');
    run('openTransactionDetails(1)');
    assert.equal(element('transaction-details-dialog').open, false);
    assert.equal(element('transaction-details-content').textContent, '');
  }
});

test('logout, signed-out state, row refresh and route changes dismiss details', async () => {
  const {run,element} = fixture([sample]);
  await run('openTransactionDetails(1); logout();');
  run('openTransactionDetails(1)');
  assert.equal(element('transaction-details-dialog').open, false);
  run('renderTable(rows); openTransactionDetails(1); applyPageFromHash();');
  assert.equal(element('transaction-details-dialog').open, false);
  run('openTransactionDetails(1); renderTable([{id:2,amount:2}]); openTransactionDetails(1);');
  assert.equal(element('transaction-details-dialog').open, false);
  run('currentUser=null; renderTable(rows); openTransactionDetails(1)');
  assert.equal(element('transaction-details-dialog').open, false);
});
