const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),test=require('node:test');
const source='// ── State'+fs.readFileSync(path.join(__dirname,'index.html'),'utf8').split('<script>\n// ── State')[1].split('</script>')[0];
function fixture(storage=new Map()) {
 const elements=new Map();
 const element=id=>{
  if(!elements.has(id)) elements.set(id,{value:'',textContent:'',tagName:id.startsWith('filter-')&&!id.includes('date')?'SELECT':'INPUT',options:[],style:{},classList:{remove(){},toggle(){}},addEventListener(){},appendChild(o){this.options.push(o);},set innerHTML(v){this.options=[];},setAttribute(){}});
  return elements.get(id);
 };
 const context=vm.createContext({URLSearchParams,Date,window:{addEventListener(){}},document:{addEventListener(){},getElementById:element,querySelectorAll(){return [];},createElement(){return {};}},localStorage:{getItem:k=>storage.get(k),setItem:(k,v)=>storage.set(k,v)}});
 vm.runInContext(source+'\ncurrentUser={email:"jim@example.test"}; applyFilters=()=>{}; updateFilterBadges=()=>{};',context);
 return {element,storage,run:code=>vm.runInContext(code,context)};
}
test('named views round trip filters, tags and exact custom dates without restoring status or sort',()=>{
 const {run,element}=fixture();
 element('saved-view-name').value='Household';element('search').value='grocery';element('filter-date-from').value='2026-01-01';
 element('filter-source').value='Old issuer';
 run('tagFilters=new Set(["Food", "Shared"]); tagMatchMode="all"; saveFilterView(); clearFilter(); viewStatus="deleted"; sortCol="amount"; applySavedView("Household");');
 assert.equal(element('search').value,'grocery');assert.equal(element('filter-date-from').value,'2026-01-01');
 assert.equal(element('filter-source').value,'Old issuer');assert.equal(element('filter-source').options[0].textContent,'Old issuer (saved filter)');
 assert.deepEqual(Array.from(run('tagFilters')),['Food','Shared']);assert.equal(run('tagMatchMode'),'all');
 assert.equal(run('viewStatus'),'deleted');assert.equal(run('sortCol'),'amount');
});
test('relative date presets recalculate when reopened',()=>{
 const {run,element}=fixture();element('saved-view-name').value='Recent';
 run('setDatePreset("30days"); saveFilterView();');
 element('filter-date-from').value='1999-01-01';run('applySavedView("Recent")');
 assert.equal(element('filter-date-to').value,run('fmtDate(new Date())'));
 assert.notEqual(element('filter-date-from').value,'1999-01-01');assert.equal(run('activePreset'),'30days');
});
test('views are isolated per account and never applied during list rendering',()=>{
 const {run,element}=fixture();element('saved-view-name').value='Private';element('search').value='secret';run('saveFilterView(); currentUser={email:"wife@example.test"}; renderSavedViews();');
 assert.equal(run('readSavedViews().length'),0);assert.equal(element('saved-view-name').value,'');
 element('search').value='her filter';run('currentUser={email:"jim@example.test"}; renderSavedViews();');
 assert.equal(run('readSavedViews().length'),1);assert.equal(element('search').value,'her filter');
});
test('updating and deleting a named view preserve current filters',()=>{
 const {run,element}=fixture();element('saved-view-name').value='Monthly';run('saveFilterView()');
 element('search').value='updated';run('saveFilterView()');assert.equal(run('readSavedViews().length'),1);
 run('deleteFilterView()');assert.equal(run('readSavedViews().length'),0);assert.equal(element('search').value,'updated');
});
test('storage denial and malformed stored input fail visibly without changing filters',()=>{
 const {run,element,storage}=fixture();element('search').value='keep';element('saved-view-name').value='View';
 storage.set(run('savedViewsKey()'),'{bad');run('saveFilterView()');assert.match(element('saved-view-status').textContent,/Could not save/);assert.equal(element('search').value,'keep');
 storage.clear();run('localStorage.setItem=()=>{throw Error("blocked")}; saveFilterView();');assert.match(element('saved-view-status').textContent,/Could not save/);
});
test('saved view count is bounded and hostile names use text nodes',()=>{
 const {run,element}=fixture();for(let i=0;i<12;i++){element('saved-view-name').value='View '+i;run('saveFilterView()');}
 element('saved-view-name').value='Overflow';run('saveFilterView()');assert.equal(run('readSavedViews().length'),12);assert.match(element('saved-view-status').textContent,/12 maximum/);
 element('saved-view').value='View 0';run('deleteFilterView()');element('saved-view-name').value='<img onerror=alert(1)>';run('saveFilterView()');
 assert.equal(element('saved-view').options.at(-1).textContent,'<img onerror=alert(1)>');
});
