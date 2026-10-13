// Exercise the shipped guide editor against deferred synthetic requests.
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
    if (!elements.has(id)) elements.set(id, {value:'',textContent:'',disabled:false,hidden:false,open:false,
      handlers:{},addEventListener(event, fn){this.handlers[event] = fn;},
      showModal(){this.open = true;},close(){this.open = false; this.handlers.close?.();}});
    return elements.get(id);
  };
  const requests = [];
  const context = vm.createContext({window:{addEventListener(){}},
    document:{addEventListener(){},getElementById:element},
    fetch:(url, options = {}) => new Promise((resolve, reject) => requests.push({url,options,reject,
      resolve:(body, status = 200) => resolve({ok:status < 400,status,json:async () => body,text:async () => JSON.stringify(body)})})),
  });
  vm.runInContext(source + '\ncurrentUser={role:"edit"}; showToast=()=>{};', context);
  return {element,requests,run:code => vm.runInContext(code, context)};
}

async function ready() {
  const f = fixture();
  const loading = f.run('openGuideDialog()');
  f.requests[0].resolve({guide:'Original instructions'});
  await loading;
  return f;
}

test('overlapping opens load once and cannot overwrite an open draft', async () => {
  const {run,requests,element} = fixture();
  const opening = run('openGuideDialog()');
  const overlap = run('openGuideDialog()');
  assert.equal(requests.length, 1);
  await overlap;
  requests[0].resolve({guide:'Original'});
  await opening;
  element('category-guide').value = 'Unsaved draft';
  const reopen = run('openGuideDialog()');
  assert.equal(requests.length, 1);
  await reopen;
  assert.equal(element('category-guide').value, 'Unsaved draft');
});

test('pending save protects the submitted draft and blocks duplicate saves and dismissal', async () => {
  const {run,requests,element} = await ready();
  element('category-guide').value = 'New instructions';
  const saving = run('saveGuide()');
  const duplicate = run('saveGuide()');
  const reopen = run('openGuideDialog()');
  assert.equal(requests.length, 2);
  await Promise.all([duplicate,reopen]);
  assert.equal(element('category-guide').readOnly, true);
  assert.equal(element('guide-close-btn').disabled, true);
  assert.equal(element('save-guide-btn').textContent, 'Saving…');
  let prevented = false;
  element('guide-dialog').handlers.cancel?.({preventDefault(){prevented = true;}});
  assert.equal(prevented, true);
  assert.deepEqual(JSON.parse(requests[1].options.body), {guide:'New instructions'});
  requests[1].resolve({ok:true});
  await saving;
  assert.equal(element('guide-dialog').open, false);
  assert.equal(element('guide-close-btn').disabled, false);
});

test('failed saves keep the draft editable and allow a corrected retry', async () => {
  const {run,requests,element} = await ready();
  element('category-guide').value = 'Draft to retain';
  const saving = run('saveGuide()');
  requests[1].reject(new Error('Offline'));
  await saving;
  assert.equal(element('guide-dialog').open, true);
  assert.equal(element('category-guide').value, 'Draft to retain');
  assert.equal(element('category-guide').readOnly, false);
  assert.equal(element('save-guide-btn').disabled, false);
  assert.equal(element('guide-error').hidden, false);
  assert.match(element('guide-error').textContent, /Could not save guide.*still here/);
  element('category-guide').value = '';
  const retry = run('saveGuide()');
  assert.equal(element('guide-error').hidden, true);
  assert.deepEqual(JSON.parse(requests[2].options.body), {guide:''});
  requests[2].resolve({ok:true});
  await retry;
  assert.equal(element('guide-dialog').open, false);
});

test('failed guide loads can be retried', async () => {
  const {run,requests,element} = fixture();
  const failed = run('openGuideDialog()');
  requests[0].reject(new Error('Offline'));
  await failed;
  const retry = run('openGuideDialog()');
  requests[1].resolve({guide:'Recovered'});
  await retry;
  assert.equal(element('category-guide').value, 'Recovered');
  assert.equal(element('guide-dialog').open, true);
});

test('read-only users cannot submit a guide even through a stale save action', async () => {
  const {run,requests,element} = await ready();
  run('currentUser={role:"read"}');
  const saving = run('saveGuide()');
  assert.equal(requests.length, 1);
  await saving;
  assert.equal(element('guide-dialog').open, true);
});
