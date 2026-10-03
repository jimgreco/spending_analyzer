// Render real UI templates with hostile synthetic names; no browser, DB, or network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const inline = '// ── State' + html.split('<script>\n// ── State')[1].split('</script>')[0];
const elements = new Map();
const element = id => {
  if (!elements.has(id)) elements.set(id, {innerHTML:'', value:'', querySelector:() => null,
    addEventListener(){}});
  return elements.get(id);
};
const bad = "O');globalThis.syntheticHit=true;//";
const badEmail = "o');globalThis.syntheticHit=true;//@example.test";
const tags = [{name:bad, group_tag:null, excluded_from_spending:false, tx_count:1},
              {name:'Home', group_tag:null, excluded_from_spending:false, tx_count:1}];
const context = vm.createContext({
  window:{addEventListener(){}},
  document:{addEventListener(){}, getElementById:element},
  fetch:async url => ({ok:true, json:async () => url === '/api/tags'
    ? {tags} : {invites:[{email:badEmail,role:'read',last_seen_at:null,has_account:false}]}}),
  console,
});
context.bad = bad;
vm.runInContext(inline, context);

function handlers(markup) {
  return [...markup.matchAll(/\bon(?:click|change|keydown)="([^"]*)"/g)].map(match => match[1]);
}

function assertNamesRemainData(markup, name, expectedAttribute, surface) {
  assert(markup.includes(`${expectedAttribute}="${name}"`), `${surface}: missing escaped data attribute`);
  const actions = handlers(markup);
  assert(actions.length > 0, `${surface}: expected interactive controls`);
  for (const action of actions)
    assert(!action.includes(name), `${surface}: name interpolated into executable handler`);
}

async function main() {
  vm.runInContext('uploadsTotal=1', context);
  vm.runInContext('renderUploadHistory([{filename:bad,file_hash:"synthetic-hash",source:"Example card",card_last4:"1234",tx_new:1,tx_dupes:0,uploaded_at:"2026-10-03"}], {all_sources:["Example card"],all_cards:[],all_filenames:[bad]})',
    context);
  const upload = element('upload-history').innerHTML;
  assertNamesRemainData(upload, bad, 'data-filename', 'upload history');
  assert(upload.includes('data-file-hash="synthetic-hash"'), 'upload history: missing saved file hash');
  assert(upload.includes("reimportFile(this.closest('tr').dataset.fileHash)"), 'reimport must use saved file hash');
  for (const action of ['saveImportRename(', 'saveUploadSource(', 'saveCardLast4(', 'reimportFile(', 'deleteImport('])
    assert(upload.includes(action), `upload history: missing ${action}`);

  const chips = vm.runInContext('tagsCell({id:1,primary_tag:"Home",tags:[bad]})', context);
  assertNamesRemainData(chips, bad, 'data-tag', 'transaction secondary tag');
  assert(chips.includes('removeTagFromTx('));

  await vm.runInContext('renderTagList()', context);
  const tagModal = element('tag-list').innerHTML;
  assertNamesRemainData(tagModal, bad, 'data-name', 'tag manager');
  for (const action of ['setTagGroup(', 'toggleTagExclusion(', 'deleteTagFromModal('])
    assert(tagModal.includes(action), `tag manager: missing ${action}`);

  await vm.runInContext('renderInviteList()', context);
  const invites = element('invite-list').innerHTML;
  assertNamesRemainData(invites, badEmail, 'data-email', 'access manager');
  for (const action of ['changeInviteRole(', 'revokeInvite('])
    assert(invites.includes(action), `access manager: missing ${action}`);

  process.stdout.write('Synthetic names stayed in data attributes across upload, transaction, tag, and access controls.\n');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
