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
  const context = vm.createContext({rows,boxes,
    window:{addEventListener(){}},
    document:{addEventListener(){},getElementById:element,querySelectorAll:() => boxes}});
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
