"""Deterministic recall quality and authority checks; no provider/network."""
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'bridges/magent'), str(Path(os.environ.get('MAGENT_TEST_HARNESS_PATH', ROOT.parent / 'Magent Harness')) / 'src')]
from memory_recall import WorkspaceMemoryService, ranked, budget, terms
from magent_harness.memory import MemoryItem, MemoryKind, MemoryScope, InMemoryMemoryStore
from magent_harness.identity import Caller


def item(number, text, kind=MemoryKind.FACT, **kwargs):
    return MemoryItem(id=f'{number:032x}', kind=kind, content=text, **kwargs)


class RecallTests(unittest.TestCase):
    def test_unicode_and_chinese_queries(self):
        cases = [('数据库配置', '数据库使用 MySQL', 'Spark worker 内存 4G'),
                 ('ＭＹＳＱＬ', 'MySQL 数据库端口 3306', '无关资料'),
                 ('café', 'Je préfère un café', 'cafe hardware'),
                 ('部署', '服务部署在 Docker 中', '财务数据分析')]
        for query, relevant, other in cases:
            with self.subTest(query=query):
                self.assertEqual([x.id for x in ranked([item(1, relevant), item(2, other, importance=5)], query)], [f'{1:032x}'])
        self.assertNotIn('据部', terms('数据 部署'))

    def test_relevance_before_importance_and_no_hidden_pool(self):
        items = [item(i + 1, 'unrelated', importance=5) for i in range(999)]
        wanted = item(1001, 'MySQL 配置', importance=1)
        self.assertEqual(ranked(items + [wanted], 'MySQL 配置'), [wanted])
        self.assertEqual(ranked([item(1, 'MySQL', importance=5), wanted], 'MySQL 配置')[0], wanted)

    def test_stopwords_and_weak_overlap_do_not_inject(self):
        self.assertEqual(ranked([item(1, 'any note', importance=5)], 'how can i do it'), [])
        self.assertEqual(ranked([item(1, 'mysql')], 'mysql docker qdrant minio redis tika ollama'), [])

    def test_budgets_deduplicate_and_skip_long_items(self):
        items = [item(1, 'x' * 500), item(2, 'MySQL port'), item(3, 'MySQL port')]
        self.assertEqual([x.id for x in budget(items, 5, 50)], [items[1].id])
        resident, situational = WorkspaceMemoryService._select([
            item(1, 'concise answers', MemoryKind.PREFERENCE),
            item(2, 'MySQL port', MemoryKind.FACT),
            item(3, 'unrelated', MemoryKind.TASK)], 'MySQL')
        self.assertEqual(len(resident), 1)
        self.assertEqual(len(situational), 1)

    def test_search_touches_and_rechecks_after_forget(self):
        caller = Caller(workspace_id='workspace', user_id='one', role='owner')
        scope = MemoryScope.for_caller(caller)
        class Store(InMemoryMemoryStore):
            def touch(self, scope, ids, now):
                super().touch(scope, ids, now)
                for mid in ids: self.forget(scope, mid)
        store = Store()
        store.add(scope, item(1, 'MySQL configuration'))
        service = WorkspaceMemoryService(store)
        self.assertEqual(service.search(caller, 'MySQL'), [])
        self.assertEqual(service.recall(caller, 'MySQL').prompt, '')

    def test_store_failure_degrades_and_search_budget(self):
        caller = Caller(workspace_id='workspace', user_id='one', role='owner')
        store = InMemoryMemoryStore()
        scope = MemoryScope.for_caller(caller)
        for i in range(20): store.add(scope, item(i+1, 'MySQL ' + str(i) + 'x' * 450))
        service = WorkspaceMemoryService(store)
        matches = service.search(caller, 'MySQL', 20)
        self.assertLessEqual(len("\n".join(f"- [{i.kind.value}] {i.content}" for i in matches)), 1600)
        self.assertTrue(all(i.use_count == 1 for i in matches))
        store.list_items = lambda *_: (_ for _ in ()).throw(RuntimeError('private'))
        self.assertEqual(service.search(caller, 'MySQL'), [])
        self.assertEqual(service.recall(caller, 'MySQL').prompt, '')

if __name__ == '__main__': unittest.main()
