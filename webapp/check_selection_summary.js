// Check selection totals using the shipped UI and synthetic transactions.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function fixture(rows) {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  const source = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {textContent:'',innerHTML:'',checked:false,indeterminate:false,
        addEventListener(){},classList:{remove:key => classes.delete(key),
          toggle:(key, value) => value ? classes.add(key) : classes.delete(key),
          contains:key => classes.has(key)}});
    }
    return elements.get(id);
  };
  const boxes = rows.map(row => ({dataset:{id:String(row.id)},checked:false,
    closest:() => element(`row-${row.id}`)}));
  const downloads = [];
  const revoked = [];
  const blobs = [];
  const context = vm.createContext({rows,boxes,downloads,revoked,blobs,
    Blob,
    URL:{createObjectURL(blob){blobs.push(blob);return 'blob:csv';}, revokeObjectURL(url){revoked.push(url);}},
    setTimeout(fn){fn();},
    window:{addEventListener(){}},
    document:{addEventListener(){},getElementById:element,querySelectorAll:() => boxes,
      body:{appendChild(){}}, createElement:() => ({click(){downloads.push({href:this.href,name:this.download});},remove(){}})}});
  vm.runInContext(source + '\ntagsCell=typeCell=sourceTag=()=>""; renderTable(rows);', context);
  return {element,run:code => vm.runInContext(code, context)};
}

test('selected amount includes signed selected rows and excludes unchecked transactions', () => {
  const {run,element} = fixture([{id:1,amount:'12.34'},{id:2,amount:'-2.34'},{id:3,amount:'900.00'}]);
  run('boxes[0].checked=true; toggleRow(boxes[0]); boxes[1].checked=true; toggleRow(boxes[1]);');
  assert.equal(element('bulk-count').textContent, '2 transactions selected on this page');
  assert.equal(element('bulk-total').textContent, 'Selected amount: $10.00');
  assert.equal(element('bulk-bar').classList.contains('visible'), true);
  run('boxes[0].checked=false; toggleRow(boxes[0]);');
  assert.equal(element('bulk-total').textContent, 'Selected amount: -$2.34');
});

test('select all totals displayed cents without a negative zero balance', () => {
  const {run,element} = fixture([{id:1,amount:0.1},{id:2,amount:0.2},{id:3,amount:-0.3}]);
  run('toggleSelectAll(true)');
  assert.equal(element('bulk-count').textContent, '3 transactions selected on this page');
  assert.equal(element('bulk-total').textContent, 'Selected amount: $0.00');
  run('toggleSelectAll(false)');
  assert.equal(element('bulk-bar').classList.contains('visible'), false);
});

test('totals use effective corrected amounts and refresh when the transaction reloads', () => {
  const {run,element} = fixture([{id:1,amount:'-25.50',original_amount:'25.50'}]);
  run('toggleSelectAll(true)');
  assert.equal(element('bulk-total').textContent, 'Selected amount: -$25.50');
  run('renderTable([{id:1,amount:"25.50",original_amount:"25.50"}])');
  assert.equal(element('bulk-total').textContent, 'Selected amount: $25.50');
});

test('a refreshed page drops missing selected rows from the amount and bulk actions', () => {
  const {run,element} = fixture([{id:1,amount:20},{id:2,amount:30}]);
  run('toggleSelectAll(true); renderTable([{id:2,amount:30}]);');
  assert.equal(element('bulk-count').textContent, '1 transaction selected on this page');
  assert.equal(element('bulk-total').textContent, 'Selected amount: $30.00');
  assert.deepEqual(Array.from(run('selectedIds')), [2]);
  run('renderTable([])');
  assert.equal(element('bulk-total').textContent, 'Selected amount: $0.00');
  assert.equal(element('bulk-bar').classList.contains('visible'), false);
});

test('clearing selection resets the amount and hides the summary', () => {
  const {run,element} = fixture([{id:1,amount:12.5}]);
  run('toggleSelectAll(true); clearSelection();');
  assert.equal(element('bulk-total').textContent, 'Selected amount: $0.00');
  assert.equal(element('bulk-count').textContent, '0 transactions selected on this page');
  assert.equal(element('bulk-bar').classList.contains('visible'), false);
});


test('CSV exports only selected page rows, effective signed amounts and escaped fields', () => {
  const {run} = fixture([
    {id:1,date:'2026-10-15',description:'Cafe, "Family"\nLunch',amount:'-25.50',original_amount:'25.50',source:'Card',card_last4:'0123',primary_tag:'Food',tags:['Travel','Shared'],transaction_type:'refund',status:'active'},
    {id:2,amount:900,description:'Not selected'},
  ]);
  run('selectedIds.add(1)');
  assert.equal(run('selectedTransactionsCsv()'), '\ufeff"Date","Description","Amount","Source","Card last 4","Primary category","Tags","Type","Status"\r\n"2026-10-15","Cafe, ""Family""\nLunch",-25.50,"Card","0123","Food","Travel; Shared","refund","active"\r\n');
  run('renderTable([{id:3,description:"Next page",amount:2}])');
  assert.equal(run('selectedTransactionsCsv()'), '');
});

test('CSV neutralizes formulas in text while retaining signed numeric amounts', () => {
  const {run} = fixture([{id:1,date:'2026-10-15',description:'  =HYPERLINK("x")',amount:-12,source:'+cmd',primary_tag:'@SUM(A1)',tags:['-danger'],card_last4:'\t=1'}]);
  run('selectedIds.add(1)');
  const csv = run('selectedTransactionsCsv()');
  for (const text of ["'  =HYPERLINK", "'+cmd", "'@SUM", "'-danger", "'\t=1"]) assert.ok(csv.includes(text));
  assert.ok(csv.includes(',-12.00,'));
});

test('CSV download uses UTF-8 content and releases its object URL without clearing selection', async () => {
  const {run} = fixture([{id:1,description:'Crème brûlée',amount:4.5}]);
  run('exportSelectedCsv()');
  assert.equal(run('downloads.length'), 0);
  run('selectedIds.add(1); exportSelectedCsv()');
  assert.equal(run('downloads[0].name'), 'spending-selected-transactions.csv');
  assert.equal(run('downloads[0].href'), 'blob:csv');
  assert.equal(run('blobs[0].type'), 'text/csv;charset=utf-8');
  assert.ok((await run('blobs[0].text()')).includes('Crème brûlée'));
  assert.equal(run('revoked[0]'), 'blob:csv');
  assert.equal(run('selectedIds.size'), 1);
});
