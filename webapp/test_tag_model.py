"""Synthetic categorization checks: no statements, DB, or live Claude calls."""
import importlib
import os
import json
import unittest
from unittest.mock import patch

from types import SimpleNamespace

import ai_client
from categorization import prepare_context, validate_results

with patch('dotenv.load_dotenv'), patch.dict('os.environ', {
    'ANTHROPIC_API_KEY':'synthetic-test-key', 'ANTHROPIC_MODEL':'',
    'SECRET_KEY':'synthetic-test-secret', 'LOCAL_DEV':'false',
}):
    app = importlib.import_module('app')

# ai_client reads the key per request, so keep a synthetic one for the whole run.
os.environ['ANTHROPIC_API_KEY']='synthetic-test-key'
os.environ.pop('ANTHROPIC_MODEL',None)


class FakeClaude:
    """Stands in for anthropic.Anthropic: records each request, returns one fixed reply."""
    def __init__(self, text, stop_reason='end_turn', requests=None):
        self.text, self.stop_reason = text, stop_reason
        self.requests = [] if requests is None else requests
        self.beta = SimpleNamespace(messages=SimpleNamespace(stream=self._stream))

    def _stream(self, **request):
        self.requests.append(request)
        message = SimpleNamespace(stop_reason=self.stop_reason,
                                  content=[SimpleNamespace(type='text', text=self.text)])
        return _Stream(message)

    def close(self):
        pass


class _Stream:
    def __init__(self, message):
        self.message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return self.message


def patch_claude(client=None, **kwargs):
    """Patch the SDK client that ai_client builds for every request."""
    if client is None:
        return patch.object(ai_client.anthropic, 'Anthropic', **kwargs)
    return patch.object(ai_client.anthropic, 'Anthropic', return_value=client)


def transaction(description='EXAMPLE GROCERY', amount=30, source='Example card'):
    return dict(date='2026-07-25', description=description, amount=amount, source=source)


def example(id=1, tag='Groceries', **kw):
    return dict(transaction(**kw), id=id, primary_tag=tag, manually_corrected=True,
                correction_scope='similar', correction_note='Food for home')


def decision(index=0, tag='Groceries', confidence='high', ids=None):
    return dict(index=index, primary_tag=tag, confidence=confidence,
                reason='Food for home, consistent with the supplied example.', example_ids=ids or [])


class TagModelTests(unittest.TestCase):
    def make_client(self, results, stop_reason='end_turn'):
        self.requests=[]
        return FakeClaude(json.dumps({'results':results}), stop_reason, self.requests)

    def test_model_request_contains_guide_and_manual_context(self):
        client=self.make_client([decision(ids=[1])])
        with patch_claude(client):
            result=app.assign_tags_with_ai([transaction()],['Groceries'],'Food for home.',[example()])
        self.assertEqual(result[0]['primary_tag'],'Groceries')
        self.assertFalse(result[0]['needs_review'])
        request=self.requests[0]
        self.assertEqual(request['model'],'claude-opus-5-5')
        self.assertEqual(request['system'],app.SYSTEM_PROMPT)
        self.assertEqual(request['output_config']['effort'],'low')
        self.assertEqual(request['output_config']['format']['type'],'json_schema')
        self.assertEqual(request['max_tokens'],16384)
        self.assertEqual(request['fallbacks'],'default')
        self.assertNotIn('temperature',request)
        payload=json.loads(request['messages'][0]['content'])
        self.assertEqual(payload['categorization_guide'],'Food for home.')
        self.assertEqual(payload['transactions'][0]['manual_examples'][0]['note'],'Food for home')
        self.assertEqual(payload['transactions'][0]['amount'],30)

    def test_same_description_different_amount_keeps_independent_decisions(self):
        client=self.make_client([decision(0,'Groceries'),decision(1,'Home')])
        with patch_claude(client):
            result=app.assign_tags_with_ai([transaction(amount=30),transaction(amount=300)],['Groceries','Home'])
        self.assertEqual([r['primary_tag'] for r in result],['Groceries','Home'])
        rows=json.loads(self.requests[0]['messages'][0]['content'])['transactions']
        self.assertEqual([r['amount'] for r in rows],[30,300])

    def test_automatic_and_one_time_examples_never_train(self):
        auto=example(2);auto['manually_corrected']=False
        once=example(3);once['correction_scope']='transaction'
        archived=example(4);archived['correction_archived']=True
        context=prepare_context([transaction()],[example(),auto,once,archived])[0]
        self.assertEqual([ex['id'] for ex in context['examples']],[1])

    def test_conflict_cannot_be_outvoted_by_repeats(self):
        history=[example(i) for i in range(1,40)]+[example(99,'Home')]
        context=prepare_context([transaction()],history)
        self.assertTrue(context[0]['conflicting'])
        self.assertLessEqual(len(context[0]['examples']),6)
        self.assertIn('Home',[ex['primary_tag'] for ex in context[0]['examples']])
        cited=context[0]['examples'][0]['id']
        result=validate_results({'results':[decision(ids=[cited])]},['Groceries','Home'],context)[0]
        self.assertTrue(result['needs_review'])
        self.assertIsNone(result['primary_tag'])
        self.assertIn('different categories',result['reason'])

    def test_broad_merchant_requires_explicit_applicable_noted_preference(self):
        row=transaction('AMAZON MARKETPLACE')
        ex=example(description='AMAZON MARKETPLACE');ex['correction_scope']=None
        result=validate_results({'results':[decision(ids=[1])]},['Groceries'],prepare_context([row],[ex]))[0]
        self.assertTrue(result['needs_review'])
        ex['correction_scope']='similar'
        result=validate_results({'results':[decision(ids=[1])]},['Groceries'],prepare_context([row],[ex]))[0]
        self.assertFalse(result['needs_review'])

    def test_null_manual_preference_is_evidence_and_auto_guess_cannot_override(self):
        context=prepare_context([transaction()],[example(tag=None)])
        valid=validate_results({'results':[decision(tag=None,ids=[1])]},['Groceries'],context)[0]
        self.assertFalse(valid['needs_review'])
        self.assertIsNone(valid['primary_tag'])
        invalid=validate_results({'results':[decision(ids=[1])]},['Groceries'],context)[0]
        self.assertTrue(invalid['needs_review'])

    def test_credits_do_not_learn_from_debits(self):
        self.assertEqual(prepare_context([transaction(amount=-30)],[example()])[0]['examples'],[])

    def test_bad_omitted_duplicated_results_stay_in_review(self):
        context=prepare_context([transaction()]*4,[])
        results=validate_results({'results':[decision(0,'Invented'),decision(1),decision(1),
            {**decision(2),'example_ids':[999]},decision(100)]},['Groceries'],context)
        self.assertTrue(all(r['needs_review'] for r in results))
        self.assertTrue(all(r['primary_tag'] is None for r in results))

    def test_uncertain_suggestion_is_kept_unassigned(self):
        result=validate_results({'results':[decision(confidence='medium')]},['Groceries'],prepare_context([transaction()],[]))[0]
        self.assertTrue(result['needs_review'])
        self.assertEqual(result['suggested_tag'],'Groceries')
        self.assertIsNone(result['primary_tag'])

    def test_token_limit_and_transport_failure_go_to_review(self):
        for stop_reason in ('max_tokens','refusal'):
            client=self.make_client([decision()],stop_reason=stop_reason)
            with patch_claude(client):
                result=app.assign_tags_with_ai([transaction()],['Groceries'])
            self.assertTrue(result[0]['needs_review'])
        with patch_claude(side_effect=RuntimeError('synthetic failure')):
            self.assertTrue(app.assign_tags_with_ai([transaction()],['Groceries'])[0]['needs_review'])

    def test_unavailable_key_or_no_categories_remains_reviewable(self):
        with patch.dict('os.environ',{'ANTHROPIC_API_KEY':''}):
            self.assertTrue(app.assign_tags_with_ai([transaction()],['Groceries'])[0]['needs_review'])
        self.assertIn('Create categories',app.assign_tags_with_ai([transaction()],[])[0]['reason'])


if __name__=='__main__':unittest.main()
