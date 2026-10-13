// Exercise the shipped mutation handlers and totals loaders with synthetic I/O.
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
    if (!elements.has(id)) elements.set(id, {
      value:'', textContent:'', innerHTML:'', dataset:{}, style:{}, hidden:false,
      classList:{add(){}, remove(){}, toggle(){}},
      addEventListener(){}, close(){}, querySelector:() => null,
    });
    return elements.get(id);
  };
  let total = 125;
  const reads = [];
  const context = vm.createContext({
    window:{addEventListener(){}},
    document:{addEventListener(){}, getElementById:element,
      querySelectorAll:() => [], querySelector:selector => selector.includes('correction-scope') ? {value:'transaction'} : element(selector)},
    URLSearchParams, console, confirm:() => true, setTimeout, clearTimeout,
    fetch:async (url, options = {}) => {
      if (options.method && options.method !== 'GET') {
        total = 0;
        return {ok:true, json:async () => ({deleted:1, restored:1, updated:1, deleted_transactions:1})};
      }
      reads.push(url);
      const data = url.startsWith('/api/stats?') ? {total, count:total ? 1 : 0, by_tag:[]}
        : url.startsWith('/api/analytics?') ? {gross_charges:total, refunds:0, net_spend:total,
          income_credits:0, transfer_net:0, legacy_total:total, included_count:total ? 1 : 0,
          untyped_count:0, ambiguous_sign_count:0, excluded_count:total ? 0 : 1}
        : url.startsWith('/api/upload/jobs?') ? {jobs:[]} : {};
      return {ok:true, json:async () => data};
    },
  });
  vm.runInContext(source + `
    renderCharts = () => {};
    loadTags = loadTransactions = loadUploadHistory = renderTagList = loadCorrections = async () => {};
    showToast = () => {};
    currentUser = {role:'edit'};
    currentTxs = [{id:1, tags:['Food'], primary_tag:'Food', amount:125}];
    selectedIds.add(1);
  `, context);
  return {context, element, reads, changeTotal:() => { total = 0; }};
}

const actions = [
  ['excluding a category', 'toggleTagExclusion("Food", true)'],
  ['changing a category group', 'setTagGroup("Food", "Excluded")'],
  ['deleting a category', 'deleteTagFromModal("Food")'],
  ['saving a primary category', 'correctionTarget={ids:[1],bulk:false}; saveCorrection()'],
  ['clearing all categories', 'clearAllTagsFromTx(1, {disabled:false,closest:()=>null}, {stopPropagation(){}})'],
  ['adding a secondary tag under a tag filter', 'addTagToTx(1, "Other", {closest:()=>null})'],
  ['removing a secondary tag under a tag filter', 'removeTagFromTx(1, "Food", {stopPropagation(){},target:{closest:()=>null}})'],
  ['bulk tagging', 'applyBulkTag("add")'],
  ['deleting selected transactions', 'deleteSelected()'],
  ['restoring one transaction', 'restoreOne(1)'],
  ['restoring selected transactions', 'restoreSelected()'],
  ['deleting an import', 'deleteImport("synthetic.csv", 1)'],
  ['changing an import source', 'saveUploadSource("synthetic.csv", {querySelector:()=>({value:"New card"})})'],
  ['changing a transaction source', 'saveSource(1, "New card", {dataset:{},parentElement:null})'],
  ['saving a type with an active type filter', 'typeTarget={id:1,revision:0}; saveTransactionType()'],
];

for (const [name, action] of actions) {
  test(`${name} refreshes both spending summaries`, async () => {
    const {context, element, reads} = fixture();
    element('bulk-tag-input').value = 'Other';
    element('correction-tag').value = 'Excluded';
    element('type-choice').value = 'transfer';
    element('filter-type').value = 'expense';
    await vm.runInContext('Promise.all([loadStats(), loadAnalytics()])', context);
    assert.equal(element('card-total').textContent, '$125.00');
    assert.match(element('flow-metrics').innerHTML, /\$125\.00/);
    reads.length = 0;
    await vm.runInContext(action, context);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(element('card-total').textContent, '$0.00');
    assert.doesNotMatch(element('flow-metrics').innerHTML, /\$125\.00/);
    assert.match(element('flow-coverage').textContent, /0 of 0 included/);
    assert.equal(reads.filter(url => url.startsWith('/api/stats?')).length, 1);
    assert.equal(reads.filter(url => url.startsWith('/api/analytics?')).length, 1);
  });
}

test('an import finishing after dashboard reload refreshes both summaries', async () => {
  const {context, element, changeTotal} = fixture();
  await vm.runInContext('Promise.all([loadStats(), loadAnalytics()])', context);
  changeTotal();
  await vm.runInContext('uploadJobWasActive=true; loadUploadJobs()', context);
  assert.equal(element('card-total').textContent, '$0.00');
  assert.doesNotMatch(element('flow-metrics').innerHTML, /\$125\.00/);
});
