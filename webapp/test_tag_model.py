"""Synthetic categorization checks: no statements, DB, or live OpenAI calls."""
import importlib
import json
import unittest
from unittest.mock import patch

import httpx
from openai import OpenAI
from categorization import prepare_context, validate_results

with patch('dotenv.load_dotenv'), patch.dict('os.environ', {
    'OPENAI_API_KEY':'synthetic-test-key', 'OPENAI_MODEL':'gpt-4.1-mini',
    'OPENAI_TAG_MODEL':'', 'SECRET_KEY':'synthetic-test-secret', 'LOCAL_DEV':'false',
}):
    app = importlib.import_module('app')


def transaction(description='EXAMPLE GROCERY', amount=30, source='Example card'):
    return dict(date='2026-07-25', description=description, amount=amount, source=source)


def example(id=1, tag='Groceries', **kw):
    return dict(transaction(**kw), id=id, primary_tag=tag, manually_corrected=True,
                correction_scope='similar', correction_note='Food for home')


def decision(index=0, tag='Groceries', confidence='high', ids=None):
    return dict(index=index, primary_tag=tag, confidence=confidence,
                reason='Food for home, consistent with the supplied example.', example_ids=ids or [])


class TagModelTests(unittest.TestCase):
    def make_client(self, results, finish_reason='stop'):
        self.requests=[]
        def handle(request):
            self.requests.append(json.loads(request.content))
            return httpx.Response(200, json={
                'id':'synthetic-completion', 'object':'chat.completion', 'created':0,
                'model':'gpt-6-astra', 'choices':[{'index':0, 'finish_reason':finish_reason,
                    'message':{'role':'assistant', 'content':json.dumps({'results':results})}}]})
        client=OpenAI(api_key='synthetic-test-key', http_client=httpx.Client(transport=httpx.MockTransport(handle)))
        self.addCleanup(client.close)
        return client

    def test_model_request_contains_guide_and_manual_context(self):
        client=self.make_client([decision(ids=[1])])
        with patch.object(app,'OpenAI',return_value=client):
            result=app.assign_tags_with_gpt([transaction()],['Groceries'],'Food for home.',[example()])
        self.assertEqual(result[0]['primary_tag'],'Groceries')
        self.assertFalse(result[0]['needs_review'])
        self.assertEqual(app.OPENAI_MODEL,'gpt-4.1-mini')
        request=self.requests[0]
        self.assertEqual(request['model'],'gpt-6-astra')
        self.assertEqual(request['reasoning_effort'],'low')
        self.assertEqual(request['max_completion_tokens'],16384)
        self.assertEqual(request['response_format'],{'type':'json_object'})
        self.assertNotIn('temperature',request)
        self.assertNotIn('max_tokens',request)
        payload=json.loads(request['messages'][1]['content'])
        self.assertEqual(payload['categorization_guide'],'Food for home.')
        self.assertEqual(payload['transactions'][0]['manual_examples'][0]['note'],'Food for home')
        self.assertEqual(payload['transactions'][0]['amount'],30)

    def test_same_description_different_amount_keeps_independent_decisions(self):
        client=self.make_client([decision(0,'Groceries'),decision(1,'Home')])
        with patch.object(app,'OpenAI',return_value=client):
            result=app.assign_tags_with_gpt([transaction(amount=30),transaction(amount=300)],['Groceries','Home'])
        self.assertEqual([r['primary_tag'] for r in result],['Groceries','Home'])
        rows=json.loads(self.requests[0]['messages'][1]['content'])['transactions']
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
        client=self.make_client([decision()],finish_reason='length')
        with patch.object(app,'OpenAI',return_value=client):
            result=app.assign_tags_with_gpt([transaction()],['Groceries'])
        self.assertTrue(result[0]['needs_review'])
        with patch.object(app,'OpenAI',side_effect=RuntimeError('synthetic failure')):
            self.assertTrue(app.assign_tags_with_gpt([transaction()],['Groceries'])[0]['needs_review'])

    def test_unavailable_key_or_no_categories_remains_reviewable(self):
        with patch.object(app,'OPENAI_API_KEY',''):
            self.assertTrue(app.assign_tags_with_gpt([transaction()],['Groceries'])[0]['needs_review'])
        self.assertIn('Create categories',app.assign_tags_with_gpt([transaction()],[])[0]['reason'])


if __name__=='__main__':unittest.main()
