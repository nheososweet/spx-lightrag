# Technical Report: Replace File by file_id

## Executive Summary

API endpoint `/replace_file_by_id` thực hiện operation **atomic replacement** của file trong LightRAG system bằng cách:
1. **Delete** toàn bộ data liên quan đến file_id cũ (chunks, entities, relationships, JSON storage)
2. **Upload** file mới với cùng file_id

Report này phân tích chi tiết **data flow**, **side effects**, và **edge cases** của operation.

---

## 1. ARCHITECTURE OVERVIEW

### 1.1 Data Layers Affected

```
┌─────────────────────────────────────────────────────────────┐
│                    REPLACE FILE OPERATION                    │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────┐
        │   STEP 1: DELETE BY file_id      │
        └──────────────────────────────────┘
                           │
        ┌──────────────────┴─────────────────────┐
        │                                        │
        ▼                                        ▼
┌───────────────┐                      ┌─────────────────┐
│  Vector Store │                      │  Graph Store    │
│  (Milvus)     │                      │  (NetworkX)     │
├───────────────┤                      ├─────────────────┤
│ - chunks_vdb  │◄─────────┐           │ - entities      │
│   DELETE      │          │           │ - relationships │
└───────────────┘          │           │   DELETE        │
                           │           └─────────────────┘
                           │
                  ┌────────┴────────┐
                  │  JSON Storage   │
                  │  (Local Files)  │
                  ├─────────────────┤
                  │ - doc_status    │
                  │ - kv_store      │
                  │   DELETE        │
                  └─────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────┐
        │   STEP 2: UPLOAD NEW FILE        │
        │   (with SAME file_id)            │
        └──────────────────────────────────┘
                           │
                ┌──────────┴──────────┐
                │                     │
                ▼                     ▼
        [ Re-extract ]        [ Re-build Graph ]
```

---

## 2. DETAILED DATA IMPACT ANALYSIS

### 2.1 Vector Store (Milvus) - Chunks

**Location:** `chunks_vdb` (Milvus collection)

**What Gets Deleted:**
```python
# Query: Find all chunks with matching file_id
filter_expr = f'file_id == "{file_id}"'

# Each chunk document contains:
{
    "id": "chunk_hash_...",
    "full_doc_id": "doc-abc123...",  # Parent document
    "content": "chunk text...",
    "embedding": [0.1, 0.2, ...],
    
    # Dynamic Fields (metadata)
    "file_id": "POL_001",      # ← FILTER KEY
    "file_url": "https://...",
    "table_name": "policies",
    "page_number": 5,
    "file_path": "policy.pdf"
}
```

**Deletion Process:**
1. Query Milvus: `get_doc_ids_by_file_id(file_id)` → returns list of `full_doc_id`
2. For each `full_doc_id`: call `adelete_by_doc_id(doc_id)`
3. Inside `adelete_by_doc_id()`:
   ```python
   # Delete all chunks belonging to this document
   await self.chunks_vdb.delete_by_ids([full_doc_id])
   ```

**Side Effects:**
- ✅ All vector embeddings removed → No longer searchable in semantic search
- ✅ Milvus dynamic fields cleared → `file_id` filter no longer returns results
- ⚠️ If file has multiple documents (e.g., multi-page PDF split into multiple docs), ALL are deleted

---

### 2.2 Graph Store - Entities

**Location:** `entities_vdb` (Milvus collection for entities)

**What Gets Deleted:**
```python
# Entities extracted from the document chunks
{
    "entity_name": "John Doe",
    "entity_type": "PERSON",
    "description": "CEO of company XYZ",
    "source_id": "doc-abc123...",  # ← Links to deleted document
    
    # Relationships
    "connected_to": ["Company XYZ", "Board of Directors"]
}
```

**Deletion Logic:**
```python
# In adelete_by_doc_id():

# 1. Get all entities belonging to this document
entities = await self.entities_vdb.query(
    filter=f'full_doc_id == "{full_doc_id}"'
)

# 2. For each entity: delete from graph
for entity in entities:
    await self.entities_vdb.delete_by_ids([entity["entity_name"]])
```

**Critical Impact:**
- ❌ **Entities are COMPLETELY REMOVED** from knowledge graph
- ❌ **No orphan entities** - Even if entity appears in other documents, this instance is deleted
- ⚠️ **Entity deduplication**: LightRAG merges entities with same name across documents
  - If "John Doe" appears in File A and File B
  - Deleting File A does NOT delete "John Doe" entity (merged with File B's version)
  - But if "John Doe" ONLY in File A → Entity fully deleted

**Entity Merging Logic** (from `lightrag.py`):
```python
# During insertion, entities are merged by name
if entity_name in existing_entities:
    # Merge descriptions, increment occurrence count
    existing_entity["description"] += f"; {new_description}"
    existing_entity["source_docs"].append(full_doc_id)
else:
    # Create new entity
    new_entity = {...}
```

**Deletion preserves merge:**
```python
# During deletion
entity = get_entity(entity_name)
entity["source_docs"].remove(full_doc_id)

if len(entity["source_docs"]) == 0:
    # Only delete if no other documents reference this entity
    delete_entity(entity_name)
else:
    # Keep entity, just remove reference to deleted doc
    update_entity(entity)
```

---

### 2.3 Graph Store - Relationships

**Location:** `entities_vdb` (stored alongside entities in graph)

**What Gets Deleted:**
```python
# Relationships (edges in knowledge graph)
{
    "src_id": "John Doe",         # Source entity
    "tgt_id": "Company XYZ",      # Target entity
    "relationship": "IS_CEO_OF",
    "description": "John Doe serves as CEO of Company XYZ",
    "source_doc_id": "doc-abc123...",  # ← Links to deleted document
    "weight": 0.95
}
```

**Deletion Logic:**
```python
# In adelete_by_doc_id():

# 1. Get all relationships from this document
relationships = await self.graph_storage.get_relationships_by_doc(full_doc_id)

# 2. Delete each relationship
for rel in relationships:
    await self.graph_storage.delete_edge(
        src_id=rel["src_id"],
        tgt_id=rel["tgt_id"]
    )
```

**Critical Impact:**
- ❌ **Relationships are REMOVED** from knowledge graph
- ⚠️ **Relationship deduplication**: Similar to entities
  - If same relationship appears in multiple docs → Only remove source doc reference
  - If relationship ONLY in deleted doc → Fully removed
- ⚠️ **Orphan nodes**: If entity has no remaining relationships after deletion:
  - Entity still exists (if referenced in other docs)
  - But becomes **isolated node** in graph

**Example Scenario:**
```
BEFORE DELETION:
  [John Doe] --IS_CEO_OF--> [Company XYZ]  (from File A)
  [John Doe] --WORKS_WITH--> [Jane Smith]  (from File B)

AFTER DELETING File A:
  [John Doe] --WORKS_WITH--> [Jane Smith]  (remains)
  [Company XYZ]  (orphaned if no other relationships)
```

---

### 2.4 JSON Storage - Document Status

**Location:** `kv_store_doc_status.json` (local file)

**What Gets Deleted:**
```json
{
  "doc-abc123...": {
    "file_path": "policy.pdf",
    "status": "PROCESSED",
    "doc_id": "doc-abc123...",
    "track_id": "upload_20260127_...",
    "created_at": "2026-01-27T10:00:00Z",
    "updated_at": "2026-01-27T10:05:00Z",
    "error": null,
    
    // Custom metadata from upload_from_url
    "file_id": "POL_001",       // ← Used to find this entry
    "table_name": "policies",
    "file_url": "https://..."
  }
}
```

**Deletion Process:**
```python
# In adelete_by_doc_id():
await self.doc_status.delete(full_doc_id)

# This removes the entire JSON entry for this document
del doc_status_dict[full_doc_id]
save_json(doc_status_dict, "kv_store_doc_status.json")
```

**Side Effects:**
- ✅ Document no longer appears in `/documents/all` endpoint
- ✅ Status history lost (no record of previous PROCESSED status)
- ⚠️ If upload fails after deletion → No record exists (user must re-upload)

---

### 2.5 JSON Storage - KV Store (Entity/Relation Chunks)

**Location:** 
- `kv_store_entity_chunks.json`
- `kv_store_relation_chunks.json`

**What Gets Deleted:**
```json
// kv_store_entity_chunks.json
{
  "John Doe": {
    "chunks": [
      "chunk_hash_1",  // From File A
      "chunk_hash_2",  // From File B
      "chunk_hash_3"   // From File A  ← DELETED
    ],
    "description": "CEO of Company XYZ",
    "entity_type": "PERSON"
  }
}
```

**Deletion Logic:**
```python
# In adelete_by_doc_id():

# 1. For each entity from deleted doc
for entity_name in deleted_entities:
    # Get entity's chunk references
    entity_data = await self.kv_store.get(entity_name)
    
    # 2. Remove chunk references from deleted doc
    entity_data["chunks"] = [
        c for c in entity_data["chunks"] 
        if c not in deleted_chunk_ids
    ]
    
    # 3. If no chunks remain, delete entity from KV store
    if not entity_data["chunks"]:
        await self.kv_store.delete(entity_name)
    else:
        await self.kv_store.update(entity_name, entity_data)
```

**Side Effects:**
- ✅ Entities maintain chunk references only from remaining documents
- ✅ Empty entities auto-cleaned
- ⚠️ **Partial entity data loss**: If entity description was enriched from deleted doc, that context is lost

---

## 3. UPLOAD NEW FILE - RE-EXTRACTION

### 3.1 What Happens During Upload

**Process:**
```python
# Step 1: Download new file
file_path = download_file(file_url)

# Step 2: Extract text + create chunks
chunks = extract_and_chunk(file_path)

# Step 3: Extract entities from chunks (LLM call)
entities = llm_extract_entities(chunks)

# Step 4: Extract relationships (LLM call)
relationships = llm_extract_relationships(entities, chunks)

# Step 5: Store everything
await store_chunks(chunks, file_id="POL_001")  # SAME file_id
await store_entities(entities)
await store_relationships(relationships)
```

**Key Points:**
- 🔄 **Fresh extraction**: New LLM calls → May extract different entities/relations
- 🆔 **Same file_id**: Maintains consistency with external system
- 📊 **Different doc_id**: New document gets new `full_doc_id` hash
- 🧹 **Clean slate**: No inheritance from old file's data

---

### 3.2 Entity/Relationship Evolution

**Scenario: Policy document update**

**Old File (v1):**
```
Entities: [CEO: John Doe], [Company: XYZ Corp], [Policy: Remote Work]
Relationships: 
  - [John Doe] --APPROVES--> [Remote Work Policy]
  - [Remote Work Policy] --APPLIES_TO--> [XYZ Corp]
```

**After Deletion:**
```
# If these entities/relationships ONLY in this file:
Entities: []  (all deleted)
Relationships: []  (all deleted)

# If they appear in other files too:
Entities: [CEO: John Doe (from other docs)], ...
Relationships: [John Doe] --WORKS_AT--> [XYZ Corp] (from other docs)
```

**New File (v2) Upload:**
```
Entities: [CEO: Jane Smith], [Company: XYZ Corp], [Policy: Hybrid Work]
Relationships:
  - [Jane Smith] --APPROVES--> [Hybrid Work Policy]
  - [Hybrid Work Policy] --APPLIES_TO--> [XYZ Corp]
```

**Result:**
- ✅ Graph now reflects NEW state (Jane Smith as CEO)
- ❌ OLD relationships (John Doe as CEO) are GONE
- ⚠️ If "XYZ Corp" entity exists in other docs → Merges with new mention
- ⚠️ If query asks "Who is CEO?" → Answer changes from John to Jane

---

## 4. EDGE CASES & CONSIDERATIONS

### 4.1 Multi-Document Files

**Scenario:** Large PDF split into multiple documents

```python
# Single file → Multiple doc_ids
file_id = "MANUAL_001"
doc_ids = [
    "doc-abc123...",  # Pages 1-100
    "doc-def456...",  # Pages 101-200
    "doc-ghi789..."   # Pages 201-300
]
```

**Deletion Impact:**
- ✅ `adelete_by_file_id()` finds ALL doc_ids with matching file_id
- ✅ Deletes all chunks, entities, relations from all 3 documents
- ⚠️ Graph may lose significant portion of knowledge if file was large

---

### 4.2 Shared Entities Across Files

**Scenario:** Entity appears in File A and File B

```
File A (POL_001): "John Doe is CEO"
File B (NEWS_123): "John Doe announces merger"
```

**Deletion of File A:**
```python
# Entity "John Doe" merged across files
entity = {
    "name": "John Doe",
    "source_docs": ["doc-abc123 (File A)", "doc-def456 (File B)"],
    "description": "CEO; announces merger"
}

# After deleting File A:
entity = {
    "name": "John Doe",
    "source_docs": ["doc-def456 (File B)"],
    "description": "announces merger"  # Lost "CEO" context
}
```

**Impact:**
- ⚠️ Entity persists but **description may be incomplete**
- ⚠️ Relationships unique to File A are lost
- ✅ Entity still searchable and appears in graph

---

### 4.3 Orphaned Relationships

**Scenario:** Relationship endpoints deleted

```
File A: [John Doe] --WORKS_AT--> [Company XYZ]
File B: [Jane Smith] --WORKS_AT--> [Company XYZ]

# Delete File A
# Result: "John Doe" entity deleted (only in File A)
# But relationship tries to reference deleted entity
```

**LightRAG Handling:**
```python
# During deletion, relationships are cleaned up:
if entity_deleted:
    # Find all relationships involving this entity
    rels = find_relationships(entity_name)
    
    # Delete those relationships
    for rel in rels:
        delete_relationship(rel)
```

**Result:**
- ✅ No dangling relationships (automatic cleanup)
- ⚠️ Graph structure changes significantly

---

### 4.4 LLM Cache Impact

**With `delete_llm_cache=True` (recommended):**
```python
# Deletes cached extraction results
cache_files = [
    f"llm_cache/{doc_id}_entities.json",
    f"llm_cache/{doc_id}_relationships.json"
]
```

**Impact:**
- ✅ **Fresh extraction** on new file upload (no stale cache)
- ⚠️ **Slower upload** (must re-run LLM extraction)
- 💰 **Higher LLM costs** (new API calls)

**With `delete_llm_cache=False`:**
- ⚠️ Cache remains but is orphaned (doc_id no longer exists)
- ⚠️ Wastes disk space
- ✅ Slightly faster if re-uploading same file (cache hit)

**Recommendation:** Always use `delete_llm_cache=True` for replace operations.

---

## 5. ATOMICITY & ERROR HANDLING

### 5.1 Operation is NOT Fully Atomic

**Risk Scenario:**
```
Step 1: Delete old file ✅ SUCCESS
Step 2: Download new file ❌ NETWORK FAILURE

Result: Old data gone, new data not uploaded
→ Data loss until manual recovery
```

**Mitigation:**
```python
# API tracks both steps in response
{
    "status": "success",
    "deletion_result": { "status": "success", ... },
    "upload_result": { "message": "...", ... }
}

# If upload fails, user can:
# 1. Retry with same file_id (clean insert)
# 2. Check /progress endpoint with track_id
```

**Best Practice:**
- Use `run_background=False` for critical replacements
- Check response carefully for partial failures
- Implement retry logic in client application

---

### 5.2 Concurrency Control

**Pipeline Lock:**
```python
# Only ONE deletion/upload operation at a time
async with pipeline_status_lock:
    if pipeline_status["busy"]:
        return {"status": "not_allowed", ...}
    
    pipeline_status["busy"] = True
```

**Implications:**
- ✅ No race conditions (atomic at operation level)
- ⚠️ **Blocks other operations**: Cannot upload/delete/query while replace in progress
- ⚠️ **User must wait**: If pipeline busy, request rejected with `not_allowed`

---

## 6. QUERY IMPACT ANALYSIS

### 6.1 Before Replacement

**Query:** "What is the company's remote work policy?"

**Data:**
```
Entities: [Remote Work Policy]
Relationships: [Remote Work Policy] --APPLIES_TO--> [All Employees]
Chunks: "Employees may work remotely 2 days per week..."
```

**Response:** ✅ Accurate answer based on File v1

---

### 6.2 During Replacement

**Timeline:**
```
t=0s:   Delete starts
t=5s:   Entities deleted
t=10s:  Chunks deleted
t=15s:  Delete complete, download starts
t=20s:  File downloaded
t=25s:  LLM extraction starts
t=60s:  New entities/relationships stored
t=65s:  Upload complete
```

**Query at t=12s (mid-replacement):**
- ❌ **No data available** for this file_id
- ⚠️ Query returns empty or "not found"
- ⚠️ If query for shared entity → Partial results (missing context from deleted file)

**Mitigation:**
- Use `run_background=False` to minimize downtime window
- Implement client-side retry logic
- Consider maintenance windows for critical documents

---

### 6.3 After Replacement

**Query:** "What is the company's remote work policy?"

**New Data:**
```
Entities: [Hybrid Work Policy]
Relationships: [Hybrid Work Policy] --APPLIES_TO--> [All Employees]
Chunks: "Employees must work in office 3 days per week..."
```

**Response:** ✅ Accurate answer based on File v2 (NEW policy)

**Impact on Existing Queries:**
- 🔄 **Different answers**: Policy changed from 2 days remote → 3 days in-office
- 🔄 **Different entities**: "Remote Work" → "Hybrid Work"
- ⚠️ **Query cache invalidated**: Old cached responses now wrong

---

## 7. RECOMMENDATIONS

### 7.1 When to Use Replace

✅ **Good Use Cases:**
- Policy document updates (annual revisions)
- Technical manual updates (new versions)
- Legal document amendments
- Price list updates

❌ **Bad Use Cases:**
- Adding supplementary information (use new upload instead)
- Correcting small typos (consider direct entity edit if available)
- Temporary rollback (implement versioning instead)

---

### 7.2 Best Practices

**1. Always set `delete_llm_cache=True`**
```python
{
    "delete_llm_cache": True  # Ensure fresh extraction
}
```

**2. Use `run_background=False` for critical documents**
```python
{
    "run_background": False  # Wait for completion
}
```

**3. Verify replacement success**
```python
# Check response
if response["status"] == "success":
    if response["deletion_result"]["status"] == "success":
        if "processed successfully" in response["upload_result"]["message"]:
            print("✓ Full replacement success")
```

**4. Monitor with track_id**
```python
track_id = response["track_id"]
status = requests.get(f"/progress/{track_id}")
```

**5. Handle errors gracefully**
```python
try:
    response = replace_file_by_id(...)
except HTTPException as e:
    if e.status_code == 404:
        print("File ID not found")
    elif e.status_code == 500:
        print("Server error - retry or contact admin")
```

---

### 7.3 Alternative Approaches

**Option 1: Versioned Files**
```python
# Instead of replacing POL_001, create POL_002
upload_from_url(file_id="POL_001_v2", ...)

# Pros: History preserved
# Cons: Duplicate entities in graph
```

**Option 2: Soft Delete (Archive)**
```python
# Mark old file as archived instead of deleting
# Requires custom implementation
```

**Option 3: Manual Entity Management**
```python
# Delete specific entities/relationships
# Then upload new file
# More granular control but complex
```

---

## 8. MONITORING & DEBUGGING

### 8.1 Log Analysis

**Key Log Markers:**
```
[DELETE BY FILE_ID] - Deletion operation logs
[REPLACE FILE] - Replace endpoint logs
[UPLOAD FROM URL] - Upload operation logs
[METADATA PIPELINE] - File processing logs
```

**Example Trace:**
```
INFO: [REPLACE FILE] ========== NEW REQUEST ==========
INFO: [REPLACE FILE]   - file_id: POL_001
INFO: [DELETE BY FILE_ID] Found 3 documents with file_id='POL_001'
INFO: [DELETE BY FILE_ID] Deleting document 1/3: doc-abc123
INFO: [DELETE BY FILE_ID] Successfully deleted 3/3 documents
INFO: [REPLACE FILE] Downloading new file from URL...
INFO: [METADATA PIPELINE] Starting processing for file: policy_v2.pdf
INFO: [REPLACE FILE] ✓ Synchronous upload completed
INFO: [REPLACE FILE] ========== REQUEST COMPLETED ==========
```

---

### 8.2 Verification Queries

**Check deletion:**
```bash
# Query Milvus for file_id
curl "http://localhost:9621/query/data" \
  -d '{"query": "file_id:POL_001"}'
# Should return: no results after deletion
```

**Check entities:**
```bash
# Query knowledge graph
curl "http://localhost:9621/query/graph" \
  -d '{"query": "entities related to POL_001"}'
# Should return: new entities from v2 file
```

**Check document status:**
```bash
# Get document by file_id (requires custom endpoint)
curl "http://localhost:9621/documents/by_file_id/POL_001"
# Should return: new doc_id, PROCESSED status
```

---

## 9. CONCLUSION

### Summary

The `replace_file_by_id` operation provides a **powerful but destructive** mechanism for updating documents in LightRAG. 

**Key Takeaways:**

1. **Deletion is Complete**: All chunks, entities, relationships, and metadata are removed
2. **No Rollback**: Once deleted, old data cannot be recovered (unless backed up externally)
3. **Graph Impact**: Knowledge graph structure changes significantly
4. **Query Downtime**: Brief period where queries may return incomplete results
5. **Non-Atomic**: Risk of partial failure (deletion succeeds, upload fails)

**Use with Caution and Proper Error Handling.**

---

## 10. APPENDIX

### A. Code References

- **Milvus Query**: `lightrag/kg/milvus_impl.py::get_doc_ids_by_file_id()` (line 1444)
- **Core Deletion**: `lightrag/lightrag.py::adelete_by_file_id()` (line 3966)
- **API Endpoint**: `lightrag/api/routers/document_routes.py::replace_file_by_id()` (line 4188)
- **Upload Pipeline**: `lightrag/api/routers/document_routes.py::pipeline_index_file_with_metadata()` (line 1971)

---

### B. Related Operations

- `delete_by_table_name()` - Delete all files in a table
- `adelete_by_doc_id()` - Delete single document by ID
- `upload_from_url()` - Upload new file with metadata

---

### C. Future Enhancements

**Potential Improvements:**
1. **Soft Delete**: Archive old data instead of permanent deletion
2. **Version Control**: Maintain file version history
3. **Atomic Transaction**: Two-phase commit for delete + upload
4. **Diff Analysis**: Compare old vs new entities/relationships
5. **Rollback Support**: Restore previous version if needed

---

**Document Version:** 1.0  
**Date:** January 27, 2026  
**Author:** Technical Documentation Team  
**Status:** ✅ Complete
