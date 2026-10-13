const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function fixture() {
  const html = fs.readFileSync(process.env.SPENDING_UI_TEST_SOURCE || path.join(__dirname, 'index.html'), 'utf8');
  const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {textContent:'',innerHTML:'',disabled:false,hidden:true,
      handlers:{},addEventListener(event, fn){this.handlers[event] = fn;},showModal(){}});
    return elements.get(id);
  };
  const requests = [];
  const context = vm.createContext({
    window:{addEventListener(){}}, document:{addEventListener(){},getElementById:element},
    fetch:url => new Promise((resolve, reject) => requests.push({url,resolve:body => resolve({ok:true,json:async () => body}),reject})),
  });
  vm.runInContext(source, context);
  return {context, element, requests, load:offset => vm.runInContext(`loadTypePreview(${offset})`, context)};
}
const data = label => ({untyped_count:101,sample:[{date:'2026-10-01',description:label,amount:10,primary_tag:null,suggested_type:null,reason:'needs_review'}]});

test('a late earlier page cannot replace the latest preview or its page labels', async () => {
  const {load,requests,element} = fixture();
  const old = load(0);
  const current = load(50);
  requests[1].resolve(data('Current page'));
  await current;
  requests[0].resolve(data('Old page'));
  await old;
  assert.match(element('type-preview-list').innerHTML, /Current page/);
  assert.doesNotMatch(element('type-preview-list').innerHTML, /Old page/);
  assert.match(element('type-preview-summary').textContent, /Showing 51–51/);
});

test('a stale failed page cannot erase a successful current page', async () => {
  const {load,requests,element} = fixture();
  const old = load(0);
  const current = load(50);
  requests[1].resolve(data('Current page'));
  await current;
  requests[0].reject(new Error('Offline'));
  await old;
  assert.match(element('type-preview-summary').textContent, /Showing 51–51/);
  assert.equal(element('type-preview-retry').hidden, true);
});

test('closing the preview invalidates its pending response', async () => {
  const {load,requests,element} = fixture();
  const pending = load(0);
  element('type-preview-dialog').handlers.close?.();
  element('type-preview-summary').textContent = 'Closed';
  requests[0].resolve(data('Closed request'));
  await pending;
  assert.equal(element('type-preview-summary').textContent, 'Closed');
});

test('loading hides old rows and a failed page can retry the same offset', async () => {
  const {load,requests,element,context} = fixture();
  element('type-preview-list').textContent = 'Old rows';
  const failed = load(50);
  assert.equal(element('type-preview-list').textContent, '');
  assert.equal(element('type-preview-prev').disabled, true);
  assert.equal(element('type-preview-next').disabled, true);
  requests[0].reject(new Error('Offline'));
  await failed;
  assert.equal(element('type-preview-retry').hidden, false);
  assert.equal(element('type-preview-prev').disabled, false);
  assert.equal(element('type-preview-next').disabled, true);
  const retry = vm.runInContext('loadTypePreview(typePreviewOffset)', context);
  assert.match(requests[1].url, /offset=50$/);
  requests[1].resolve(data('Retried page'));
  await retry;
  assert.match(element('type-preview-list').innerHTML, /Retried page/);
  assert.equal(element('type-preview-retry').hidden, true);
  assert.equal(element('type-preview-prev').disabled, false);
});

test('an empty later page shows an honest empty range and allows going back', async () => {
  const {load,requests,element} = fixture();
  const pending = load(50);
  requests[0].resolve({untyped_count:0,sample:[]});
  await pending;
  assert.match(element('type-preview-summary').textContent, /Showing 0–0/);
  assert.equal(element('type-preview-prev').disabled, false);
  assert.equal(element('type-preview-next').disabled, true);
});
