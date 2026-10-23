const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
};
const tick = () => new Promise(resolve => setImmediate(resolve));
const csvResponse = (count = 105, body = Promise.resolve(new Blob(['synthetic CSV']))) => ({
  ok:true, status:200,
  headers:new Map([['X-Export-Row-Count', String(count)], ['Content-Type', 'text/csv; charset=utf-8']]),
  blob:() => body,
});

function fixture() {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
  const elements = new Map(), reads = [], downloads = [], callbacks = {};
  const element = id => {
    if (!elements.has(id)) elements.set(id, {value:'', textContent:'', innerHTML:'', hidden:false,
      disabled:false, dataset:{}, style:{}, classList:{add(){}, remove(){}, toggle(){}},
      setAttribute(){}, addEventListener(){}, querySelector:() => null});
    return elements.get(id);
  };
  let nextResponse = () => Promise.resolve(csvResponse()), account = {id:1, auth_id:2};
  const context = vm.createContext({
    window:{addEventListener:(event, fn) => { callbacks[event] = fn; }},
    document:{addEventListener(){}, getElementById:element, querySelectorAll:() => [],
      querySelector:() => element('query'), body:{appendChild(){}},
      createElement:() => ({click(){downloads.push(this.download);}, remove(){}})},
    URLSearchParams, AbortController, Blob, console, setTimeout:fn => {fn();}, clearTimeout(){},
    URL:{createObjectURL:() => 'blob:synthetic', revokeObjectURL(){}},
    fetch:(url, options = {}) => {
      reads.push({url, options});
      if (url === '/auth/me') return Promise.resolve({ok:!!account, json:async () => account});
      return nextResponse();
    },
  });
  vm.runInContext(source + '\ncurrentUser={id:1,auth_id:2,role:"read"};', context);
  return {context, element, reads, downloads, callbacks,
    response:fn => {nextResponse=fn;}, account:value => {account=value;},
    run:code => vm.runInContext(code, context)};
}

test('all matches export retains filters, status, sort and account but not pagination', async () => {
  const f = fixture();
  f.element('filter-source').value = 'Synthetic card';
  f.element('filter-card').value = '0012';
  f.element('filter-import').value = 'October.csv';
  f.element('filter-type').value = 'refund';
  f.element('search').value = 'Café & tea';
  await f.run('currentPage=3; viewStatus="review"; sortCol="amount"; sortDir="asc"; tagFilters=new Set(["Food","Travel"]); tagMatchMode="all"; exportMatchingCsv()');
  const params = new URLSearchParams(f.reads[0].url.split('?')[1]);
  for (const [key, value] of Object.entries({source:'Synthetic card', card_last4:'0012', import_file:'October.csv',
    transaction_type:'refund', search:'Café & tea', status:'review', sort_by:'amount', sort_dir:'asc',
    tag_match:'all', expected_dataset_id:'1', expected_actor_id:'2'})) assert.equal(params.get(key), value);
  assert.deepEqual(params.getAll('tag'), ['Food','Travel']);
  for (const key of ['page','per_page','date_from','date_to']) assert.equal(params.has(key), false);
  assert.deepEqual(f.downloads, ['spending-filtered-transactions.csv']);
  assert.equal(f.element('export-matches-status').textContent, 'Downloaded 105 matching transactions.');
  assert.equal(f.element('export-matches-btn').disabled, false);
});

test('repeated clicks share one operation and cancel allows a safe immediate retry', async () => {
  const f = fixture(), old = deferred(), fresh = deferred();
  f.response(() => old.promise);
  const first = f.run('exportMatchingCsv()');
  await f.run('exportMatchingCsv()');
  assert.equal(f.reads.length, 1);
  f.run('cancelMatchingExport()');
  assert.equal(f.reads[0].options.signal.aborted, true);
  f.response(() => fresh.promise);
  const second = f.run('exportMatchingCsv()');
  old.resolve(csvResponse(99));
  await first;
  assert.equal(f.element('export-matches-btn').disabled, true);
  assert.equal(f.downloads.length, 0);
  fresh.resolve(csvResponse(2));
  await second;
  assert.equal(f.downloads.length, 1);
  assert.equal(f.element('export-matches-status').textContent, 'Downloaded 2 matching transactions.');
});

test('a filter changed while bytes arrive suppresses the stale download', async () => {
  const f = fixture(), body = deferred();
  f.response(async () => csvResponse(3, body.promise));
  const operation = f.run('exportMatchingCsv()');
  await tick();
  f.element('search').value = 'new filter';
  body.resolve(new Blob(['old data']));
  await operation;
  assert.equal(f.downloads.length, 0);
  assert.match(f.element('export-matches-status').textContent, /changed/);
  assert.equal(f.element('export-matches-btn').disabled, false);
});

test('sort and status changes invalidate in-flight snapshots', async () => {
  for (const change of ['sortDir="asc"', 'viewStatus="deleted"']) {
    const f = fixture(), result = deferred();
    f.response(() => result.promise);
    const operation = f.run('exportMatchingCsv()');
    f.run(change);
    result.resolve(csvResponse());
    await operation;
    assert.equal(f.downloads.length, 0);
  }
});

test('local and other-tab account changes cannot download another snapshot', async () => {
  for (const change of [f => f.run('currentUser={id:9,role:"owner"}'),
                       f => f.run('currentUser.id=9'),
                       f => f.account({id:9}), f => f.account({id:1,auth_id:8}),
                       f => f.account(null)]) {
    const f = fixture(), result = deferred();
    f.response(() => result.promise);
    const operation = f.run('exportMatchingCsv()');
    change(f);
    result.resolve(csvResponse());
    await operation;
    assert.equal(f.downloads.length, 0);
    assert.match(f.element('export-matches-status').textContent, /account|Account/);
  }
});

test('network interruptions and size or validation errors leave retry available', async () => {
  for (const failure of [() => Promise.reject(new TypeError('offline')),
                        async () => ({ok:false,status:413}), async () => ({ok:false,status:422}),
                        async () => ({ok:false,status:401}), async () => ({ok:false,status:500})]) {
    const f = fixture();
    f.response(failure);
    await f.run('exportMatchingCsv()');
    assert.equal(f.downloads.length, 0);
    assert.equal(f.element('export-matches-btn').disabled, false);
    assert.ok(f.element('export-matches-status').textContent);
    f.response(async () => csvResponse(1));
    await f.run('exportMatchingCsv()');
    assert.equal(f.downloads.length, 1);
  }
});

test('empty results and invalid export metadata do not download files', async () => {
  for (const response of [csvResponse(0), {...csvResponse(),headers:new Map()},
                          csvResponse('NaN'), csvResponse(-1)]) {
    const f = fixture();
    f.response(async () => response);
    await f.run('exportMatchingCsv()');
    assert.equal(f.downloads.length, 0);
    assert.equal(f.element('export-matches-btn').disabled, false);
  }
});

test('page navigation interrupts the request without a later download', async () => {
  const f = fixture(), result = deferred();
  f.response(() => result.promise);
  const operation = f.run('exportMatchingCsv()');
  f.callbacks.pagehide();
  result.resolve(csvResponse());
  await operation;
  assert.equal(f.reads[0].options.signal.aborted, true);
  assert.equal(f.downloads.length, 0);
});
