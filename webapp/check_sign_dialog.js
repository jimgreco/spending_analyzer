// Run the shipped sign workflow with deferred, synthetic API responses.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const {webcrypto} = require('node:crypto');

function fixture() {
  const html = fs.readFileSync(process.env.SPENDING_UI_TEST_SOURCE || path.join(__dirname, 'index.html'), 'utf8');
  const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {textContent:'',innerHTML:'',disabled:false,hidden:false,open:false,
      handlers:{},addEventListener(event, fn){this.handlers[event] = fn;},
      showModal(){this.open = true;},close(){this.open = false; this.handlers.close?.();}});
    return elements.get(id);
  };
  const requests = [];
  const context = vm.createContext({crypto:webcrypto,
    window:{addEventListener(){}}, document:{addEventListener(){},getElementById:element},
    fetch:(url, options = {}) => new Promise((resolve, reject) => requests.push({url,options,reject,
      resolve:(body, status = 200) => resolve({ok:status < 400,status,json:async () => body,text:async () => JSON.stringify(body)})})),
  });
  vm.runInContext(source + '\ncurrentUser={role:"edit"}; showToast=()=>{}; loadTotals=loadTransactions=async()=>{};', context);
  return {context, element, requests, run:code => vm.runInContext(code, context)};
}
const preview = (id, revision = 0) => ({id,date:'2026-10-01',description:`Transaction ${id}`,source:'Synthetic',
  amount:revision ? '-25.00' : '25.00',original_amount:'25.00',amount_revision:revision,
  status:'active',transaction_type:'expense',reverse_preview:revision ? '25.00' : '-25.00',can_undo:false,history:[]});
const nextTurn = () => new Promise(resolve => setImmediate(resolve));
async function ready() {
  const f = fixture();
  const loading = f.run('openSignDialog(1)');
  f.requests[0].resolve(preview(1));
  await loading;
  return f;
}

test('a delayed older preview cannot replace the transaction opened later', async () => {
  const {run,requests,element} = fixture();
  const old = run('openSignDialog(1)');
  const current = run('openSignDialog(2)');
  requests[1].resolve(preview(2));
  await current;
  requests[0].resolve(preview(1));
  await old;
  assert.match(element('amount-transaction').textContent, /Transaction 2/);
});

test('closing a loading preview prevents a delayed response from reopening it', async () => {
  const {run,requests,element} = fixture();
  const pending = run('openSignDialog(1)');
  assert.equal(element('amount-dialog').open, true);
  element('amount-dialog').close();
  requests[0].resolve(preview(1));
  await pending;
  assert.equal(element('amount-dialog').open, false);
  assert.equal(run('amountPreviewState'), null);
});

test('failed preview loading can retry the selected transaction', async () => {
  const {run,requests,element} = fixture();
  const failed = run('openSignDialog(2)');
  requests[0].reject(new Error('Offline'));
  await failed;
  assert.equal(element('amount-retry-btn').hidden, false);
  assert.equal(element('amount-reverse-btn').hidden, true);
  const retry = run('openSignDialog(amountTargetId)');
  assert.equal(requests[1].url, '/api/transactions/2/amount');
  requests[1].resolve(preview(2));
  await retry;
  assert.equal(element('amount-retry-btn').hidden, true);
  assert.equal(element('amount-reverse-btn').hidden, false);
});

test('sign saves reject overlapping actions, preview changes, and Escape dismissal', async () => {
  const {run,requests,element} = await ready();
  const saving = run('saveSignChange("reverse")');
  const overlap = run('saveSignChange("reverse")');
  const reopen = run('openSignDialog(2)');
  assert.equal(requests.length, 2);
  await Promise.all([overlap, reopen]);
  assert.equal(element('amount-close-btn').disabled, true);
  let cancelled = false;
  element('amount-dialog').handlers.cancel?.({preventDefault(){cancelled = true;}});
  assert.equal(cancelled, true);
  requests[1].resolve({});
  await nextTurn();
  requests[2].resolve(preview(1, 1));
  await saving;
  assert.equal(element('amount-close-btn').disabled, false);
  assert.equal(run('pendingAmountOperation'), null);
});

test('a committed save with failed preview refresh reuses its exact operation on retry', async () => {
  const {run,requests,element} = await ready();
  const saving = run('saveSignChange("reverse")');
  const originalBody = requests[1].options.body;
  requests[1].resolve({});
  await nextTurn();
  requests[2].reject(new Error('Preview refresh unavailable'));
  await saving;
  assert.match(element('amount-error').textContent, /Retry this button safely/);
  const retry = run('saveSignChange("reverse")');
  assert.equal(requests[3].options.body, originalBody);
  requests[3].resolve({replayed:true});
  await nextTurn();
  requests[4].resolve(preview(1, 1));
  await retry;
  assert.equal(run('pendingAmountOperation'), null);
});

test('an uncertain save prevents switching to a different sign action', async () => {
  const {run,requests,element} = await ready();
  run('amountPreviewState.can_undo=true; amountPreviewState.history=[{id:9}]');
  const saving = run('saveSignChange("reverse")');
  requests[1].reject(new Error('Connection lost'));
  await saving;
  const alternate = run('saveSignChange("undo")');
  assert.equal(requests.length, 2);
  await alternate;
  assert.equal(element('amount-undo-btn').disabled, true);
  assert.equal(element('amount-reverse-btn').disabled, false);
});

test('a conflict with failed refresh cannot submit the stale amount again', async () => {
  const {run,requests,element} = await ready();
  const saving = run('saveSignChange("reverse")');
  requests[1].resolve({detail:'Changed elsewhere'}, 409);
  await nextTurn();
  requests[2].reject(new Error('Offline'));
  await saving;
  assert.equal(run('amountPreviewState'), null);
  assert.equal(element('amount-reverse-btn').hidden, true);
  assert.equal(element('amount-retry-btn').hidden, false);
  const repeated = run('saveSignChange("reverse")');
  assert.equal(requests.length, 3);
  await repeated;
});

test('a sign save completing after forced dialog closure does not restore the old preview', async () => {
  const {run,requests,element} = await ready();
  const saving = run('saveSignChange("reverse")');
  element('amount-dialog').close();
  requests[1].resolve({});
  await nextTurn();
  assert.equal(requests.length, 2);
  await saving;
  assert.equal(run('amountPreviewState'), null);
  assert.equal(element('amount-dialog').open, false);
});
