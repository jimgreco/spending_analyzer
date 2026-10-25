const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const source = '// ── State' + fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8').split('<script>\n// ── State')[1].split('</script>')[0];
function fixture() {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {value:'',textContent:'',style:{},classList:{remove(){},toggle(){}},addEventListener(){}});
    return elements.get(id);
  };
  const requests = [];
  const context = vm.createContext({URLSearchParams, requests,window:{addEventListener(){}},document:{addEventListener(){},getElementById:element,querySelectorAll(){return [];}}});
  vm.runInContext(source + '\napi=async url=>{requests.push(url); return {transactions:[],page:1,pages:4,total:101,per_page:pageSize};}; renderTable=()=>{}; renderReviewQueue=()=>{};',context);
  return {element,requests,run:code=>vm.runInContext(code,context)};
}
test('page size resets pagination and selection while retaining filters and sort', async () => {
  const {run,element,requests}=fixture();
  element('search').value='Groceries';
  await run('currentPage=3; selectedIds.add(9); sortCol="amount"; sortDir="asc"; changePageSize("25");');
  const params=new URLSearchParams(requests[0].split('?')[1]);
  assert.equal(params.get('page'),'1'); assert.equal(params.get('per_page'),'25');
  assert.equal(params.get('search'),'Groceries'); assert.equal(params.get('sort_by'),'amount');
  assert.equal(params.get('sort_dir'),'asc'); assert.equal(run('selectedIds.size'),0);
  assert.equal(element('page-info').textContent,'Page 1 of 4 · 101 transactions');
});
test('invalid or unchanged sizes do not reload', () => {
  const {run,requests}=fixture();
  run('changePageSize("100"); changePageSize("0"); changePageSize("10000"); changePageSize("abc");');
  assert.equal(requests.length,0);
});
test('review navigation uses the chosen page size for its global position', () => {
  const {run}=fixture();
  run('viewStatus="review"; pageSize=25; currentPage=3; currentTxs=[{id:1},{id:2}]; reviewTotal=70; reviewIndex=0; renderReviewQueue=data=>requests.push(data); moveReview(1);');
  assert.equal(run('requests[0].per_page'),25);
  assert.equal(run('(requests[0].page-1)*requests[0].per_page+reviewIndex+1'),52);
});
