import unittest
import asyncio
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from lightrag.kg.json_doc_status_impl import JsonDocStatusStorage
from lightrag.lightrag import LightRAG
from lightrag.base import BaseVectorStorage, DocStatus
from lightrag.utils import EmbeddingFunc

# Mock Embedding Func
class MockEmbeddingFunc(EmbeddingFunc):
    def __call__(self, texts: list[str]) -> list[list[float]]:
        return [[0.1] * 8 for _ in texts]
    
    @property
    def embedding_dim(self):
        return 8

# Mock Vector Storage
@dataclass
class MockVectorStorage(BaseVectorStorage):
    def __post_init__(self):
        self._data = {}

    async def soft_delete(self, ids: list[str], is_deleted: bool = True):
        for id in ids:
            if id in self._data:
                self._data[id]["isDeleted"] = is_deleted
                
    async def query(self, query, top_k, query_embedding=None):
        return []

    async def upsert(self, data):
        self._data.update(data)

    async def delete_entity(self, entity_name):
        pass

    async def delete_entity_relation(self, entity_name):
        pass

    async def get_by_id(self, id):
        return self._data.get(id)

    async def get_by_ids(self, ids):
        return [self._data.get(id) for id in ids if id in self._data]
    
    async def delete(self, ids):
        for id in ids:
            self._data.pop(id, None)

    async def get_vectors_by_ids(self, ids):
        return {id: [0.1]*8 for id in ids if id in self._data}
        
    async def is_empty(self):
        return len(self._data) == 0
        
    async def index_done_callback(self):
        pass
        
    async def drop(self):
        self._data.clear()
        return {"status": "success", "message": "data dropped"}

class TestSoftDelete(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.working_dir = self.tmp_dir

    async def asyncTearDown(self):
        shutil.rmtree(self.tmp_dir)

    async def test_json_doc_status_soft_delete_retrieval(self):
        # Setup
        storage = JsonDocStatusStorage(
            namespace="test_doc_status",
            workspace="test_ws",
            global_config={"working_dir": self.working_dir},
            embedding_func=None
        )
        await storage.initialize()
        
        # Add document
        doc_id = "doc1"
        doc_data = {
            doc_id: {
                "content_summary": "test",
                "content_length": 10,
                "file_path": "test.pdf",
                "status": "processed",
                "created_at": "now",
                "updated_at": "now",
                "chunks_list": [],
                "metadata": {},
                "error_msg": None,
                "multimodal_processed": True,
                "isDeleted": False 
            }
        }
        await storage.upsert(doc_data)
        
        # Verify retrieval
        doc = await storage.get_doc_by_file_path("test.pdf")
        self.assertIsNotNone(doc)
        self.assertEqual(doc["id"], doc_id) # Verify ID injection
        
        # Soft delete
        await storage.soft_delete([doc_id], is_deleted=True)
        
        # Verify retrieval fails by default
        doc = await storage.get_doc_by_file_path("test.pdf")
        self.assertIsNone(doc)
        
        # Verify retrieval with include_deleted=True
        doc = await storage.get_doc_by_file_path("test.pdf", include_deleted=True)
        self.assertIsNotNone(doc)
        self.assertTrue(doc["isDeleted"])
        self.assertEqual(doc["id"], doc_id)

        # Reactivate
        await storage.soft_delete([doc_id], is_deleted=False)
        
        # Verify retrieval works again
        doc = await storage.get_doc_by_file_path("test.pdf")
        self.assertIsNotNone(doc)
        self.assertFalse(doc["isDeleted"])

    async def test_lightrag_soft_delete_flow(self):
        # Setup LightRAG with mocks
        rag = LightRAG(
            working_dir=self.working_dir,
            embedding_func=MockEmbeddingFunc(),
            vector_storage_cls=MockVectorStorage,
            llm_model_func=lambda *args, **kwargs: "response"
        )
        
        # Insert a dummy document directly into storages to simulate existence
        doc_id = "doc_123"
        file_path = "document.pdf"
        file_id = "file_123"
        
        # 1. Populate storages
        await rag.doc_status.upsert({
            doc_id: {
                 "content_summary": "test",
                "content_length": 10,
                "file_path": file_path,
                "status": "processed",
                "created_at": "now",
                "updated_at": "now",
                "chunks_list": ["chunk_1"],
                "metadata": {"file_id": file_id},
                "error_msg": None,
                "isDeleted": False,
                "track_id": "track_1",
                "multimodal_processed": True
            }
        })
        
        await rag.chunks_vdb.upsert({
            "chunk_1": {
                "content": "test chunk",
                "doc_id": doc_id,
                "file_id": file_id,
                "vector": [0.1]*8,
                "isDeleted": False
            }
        })
        
        # 2. Test Soft Delete
        result = await rag.asoft_delete_by_doc_id(doc_id)
        self.assertEqual(result.status, "success")
        
        # Check status
        doc = await rag.doc_status.get_by_id(doc_id)
        self.assertTrue(doc["isDeleted"])
        
        chunk = await rag.chunks_vdb.get_by_id("chunk_1")
        self.assertTrue(chunk["isDeleted"])

        # 3. Test Reactivation
        result = await rag.aactive_by_doc_id(doc_id)
        self.assertEqual(result.status, "success")
        
        doc = await rag.doc_status.get_by_id(doc_id)
        self.assertFalse(doc["isDeleted"])
        
        chunk = await rag.chunks_vdb.get_by_id("chunk_1")
        self.assertFalse(chunk["isDeleted"])

if __name__ == "__main__":
    unittest.main()
