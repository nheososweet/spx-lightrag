# Phân Tích Kỹ Thuật Các API Chính - LightRAG System

## Tổng Quan

Document này phân tích chi tiết **5 API endpoint quan trọng nhất** trong LightRAG system, bao gồm data flow, storage operations, và side effects của từng operation.

**Các API được phân tích:**
1. `POST /documents/upload_from_url` - Upload file từ URL với metadata
2. `DELETE /documents/delete_by_table_name` - Xóa documents theo table_name
3. `DELETE /documents/delete_by_file_id` - Xóa documents theo file_id (NEW)
4. `POST /query/data` - Query dữ liệu với structured response
5. `POST /documents/replace_file_by_id` - Replace file theo file_id (NEW)

---

# 1. API: POST /documents/upload_from_url

## 1.1 Mục Đích & Use Cases

**Mô tả:**  
Upload file từ URL và xử lý với custom metadata (file_url, table_name, file_id). Metadata này được lưu vào Milvus Dynamic Fields để sau này có thể filter và delete theo các trường này.

**Use Cases:**
- Integration với external platform (CMS, DMS, Google Drive)
- Bulk upload từ file storage system
- Scheduled document updates từ remote sources
- Multi-tenant document management (isolate bằng table_name)

---

## 1.2 Request Structure

**Endpoint:** `POST /documents/upload_from_url`

**Request Body:**
```json
{
    "file_url": "https://example.com/documents/policy.pdf",
    "table_name": "company_policies",
    "file_id": "POL_001",
    "run_background": false
}
```

**Parameters:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file_url` | string | ✅ | HTTP/HTTPS URL của file cần download. Phải accessible publicly hoặc có authentication |
| `table_name` | string | ✅ | Table name để categorize document. Dùng để filter/delete sau này |
| `file_id` | string | ✅ | Unique identifier của file trong external system. Must be unique per file |
| `run_background` | boolean | ❌ | `true` (default): chạy background; `false`: đợi hoàn tất mới return |

**Validation Rules:**
```python
# file_url validation
- Must start with http:// or https://
- Cannot be empty
- Must be valid URL format

# table_name & file_id validation  
- Cannot be empty string
- Must contain printable characters
- Spaces are stripped
```

---

## 1.3 Workflow Chi Tiết

### **Phase 1: Pre-processing (Validation & Download)**

```
┌─────────────────────────────────────────────────────────────┐
│              STEP 1: VALIDATION & EXTRACTION                 │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 1.1 Extract filename from URL                                │
│     - Parse URL: https://example.com/docs/policy.pdf         │
│     - Extracted: "policy.pdf"                                 │
│                                                               │
│ 1.2 Sanitize filename                                         │
│     - Remove special chars: policy_2024 (v1).pdf → policy... │
│     - Check conflicts with existing files                     │
│     - Generate safe filename: policy_2024_v1.pdf             │
│                                                               │
│ 1.3 Validate file type                                       │
│     - Check extension against supported types:               │
│       .txt, .pdf, .docx, .md, .html, .csv, .json, etc.      │
│     - Return 400 if unsupported                              │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 2: DUPLICATE CHECK                          │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2.1 Check doc_status storage                                 │
│     Query: await rag.doc_status.get_doc_by_file_path(        │
│         safe_filename                                         │
│     )                                                         │
│     → If exists: Return 200 with status="duplicated"         │
│                                                               │
│ 2.2 Check file system                                        │
│     Path: {input_dir}/policy_2024_v1.pdf                     │
│     → If exists: Return 200 with status="duplicated"         │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 3: DOWNLOAD FILE                            │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 3.1 Create HTTP session (aiohttp.ClientSession)              │
│     - Timeout: 300 seconds (5 minutes)                       │
│     - Streaming download: iter_chunked(8192 bytes)           │
│                                                               │
│ 3.2 Write to disk                                            │
│     Path: {input_dir}/policy_2024_v1.pdf                     │
│     Mode: Binary write (async)                               │
│     Progress: Log every 100 chunks (~800KB)                  │
│                                                               │
│ 3.3 Verify download                                          │
│     - Check file size > 0                                    │
│     - Log total bytes downloaded                             │
└──────────────────────────────────────────────────────────────┘
```

### **Phase 2: Processing (Chunking & Extraction)**

```
┌─────────────────────────────────────────────────────────────┐
│         STEP 4: METADATA INJECTION PIPELINE                  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ Call: pipeline_index_file_with_metadata(                     │
│     rag, file_path, track_id, custom_metadata                │
│ )                                                             │
│                                                               │
│ custom_metadata = {                                           │
│     "file_url": "https://example.com/policy.pdf",            │
│     "table_name": "company_policies",                        │
│     "file_id": "POL_001"                                     │
│ }                                                             │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 4.1: TEXT EXTRACTION                        │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 4.1.1 File type detection                                    │
│       - PDF: Use PyPDF2 / pdfplumber                         │
│       - DOCX: Convert to PDF first (docx2pdf)                │
│       - TXT/MD: Direct read                                  │
│                                                               │
│ 4.1.2 Page extraction (for PDF/DOCX)                        │
│       - Extract text per page                                │
│       - Track page numbers                                   │
│       - Preserve formatting metadata                         │
│                                                               │
│       Example output:                                         │
│       pages_info = [                                          │
│           {"page_num": 1, "text": "Page 1 content..."},      │
│           {"page_num": 2, "text": "Page 2 content..."}       │
│       ]                                                       │
│                                                               │
│ 4.1.3 Full text assembly                                     │
│       full_text = "\n\n".join([p["text"] for p in pages])   │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 4.2: PAGE EXTRACTION & TRACKING             │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 4.2.1 Convert DOCX to PDF (if needed)                        │
│                                                               │
│       if file_ext in [".docx", ".doc"]:                       │
│           # Convert to PDF for real page extraction          │
│           converted_pdf_path = _convert_docx_to_pdf(file)    │
│           file_path = converted_pdf_path                     │
│           is_pdf = True                                       │
│                                                               │
│       Methods tried in order:                                 │
│       1. docx2pdf (Windows với MS Word) - Best quality       │
│       2. pypandoc (cross-platform) - Good quality            │
│       → Original DOCX sẽ bị xóa sau khi process xong         │
│                                                               │
│ 4.2.2 Extract text với page position tracking                │
│                                                               │
│       if is_pdf:  # PDF hoặc DOCX đã convert                 │
│           # REAL PAGE EXTRACTION                             │
│           full_text, pages_info = _extract_pdf_pypdf(        │
│               file_bytes,                                     │
│               return_pages_info=True                         │
│           )                                                   │
│                                                               │
│           # pages_info structure:                            │
│           pages_info = [                                      │
│               {                                               │
│                   "page": 1,                                  │
│                   "char_start": 0,                           │
│                   "char_end": 1523                           │
│               },                                              │
│               {                                               │
│                   "page": 2,                                  │
│                   "char_start": 1523,                        │
│                   "char_end": 3104                           │
│               },                                              │
│               ...                                             │
│           ]                                                   │
│                                                               │
│       else:  # PPTX, XLSX, etc.                              │
│           # VIRTUAL PAGE CREATION                            │
│           full_text, pages_info = _extract_text_with_virtual_pages(│
│               file_bytes, file_ext                           │
│           )                                                   │
│           # Tạo virtual pages (2000 chars/page)             │
│                                                               │
│ 4.2.3 Save page tracking data                                │
│                                                               │
│       pages_info_for_tracking = pages_info                   │
│       full_text_for_tracking = full_text                     │
│       # Sẽ dùng để assign page numbers vào chunks            │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 4.3: CHUNKING WITH METADATA                 │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 4.3.1 Save original chunking function                        │
│       original_chunking_func = rag.chunking_func             │
│                                                               │
│ 4.3.2 Create wrapper function                                │
│                                                               │
│       def chunking_with_metadata(                            │
│           tokenizer, content, chunk_size=1200, ...           │
│       ):                                                      │
│           # Call original chunker                            │
│           chunks = original_chunking_func(                   │
│               tokenizer, content, ...                        │
│           )                                                   │
│                                                               │
│           # Inject metadata + page numbers into each chunk   │
│           for chunk in chunks:                               │
│               # Basic metadata                               │
│               chunk["file_url"] = "https://..."              │
│               chunk["table_name"] = "policies"               │
│               chunk["file_id"] = "POL_001"                   │
│                                                               │
│               # PAGE NUMBER ASSIGNMENT (KEY LOGIC)           │
│               if pages_info_for_tracking:                    │
│                   from lightrag.utils_pdf import \           │
│                       assign_page_to_chunk                   │
│                                                               │
│                   chunk_text = chunk["content"]              │
│                   start_page, end_page = \                   │
│                       assign_page_to_chunk(                  │
│                           chunk_text,                        │
│                           full_text_for_tracking,            │
│                           pages_info_for_tracking            │
│                       )                                       │
│                                                               │
│                   chunk["start_page"] = start_page           │
│                   chunk["end_page"] = end_page               │
│                                                               │
│           return chunks                                       │
│                                                               │
│       # How assign_page_to_chunk works:                      │
│       # 1. Find chunk_text position in full_text             │
│       #    chunk_start = full_text.find(chunk_text)          │
│       #    chunk_end = chunk_start + len(chunk_text)         │
│       #                                                       │
│       # 2. Match with pages_info to find pages               │
│       #    for page in pages_info:                           │
│       #        if char_start <= chunk_start < char_end:      │
│       #            start_page = page["page"]                 │
│       #        if char_start < chunk_end <= char_end:        │
│       #            end_page = page["page"]                   │
│       #                                                       │
│       # Example:                                              │
│       # - Chunk ở page 1: start_page=1, end_page=1          │
│       # - Chunk span pages 2-3: start_page=2, end_page=3    │
│       # - Chunk không tìm thấy: start_page=0, end_page=0    │
│                                                               │
│ 4.3.3 Temporarily replace chunking function                  │
│       rag.chunking_func = chunking_with_metadata             │
└──────────────────────────────────────────────────────────────┘
                           │
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 4.4: STANDARD PIPELINE PROCESSING           │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 4.4.1 Call pipeline_enqueue_file()                           │
│       → Extract text from file                               │
│       → Create chunks using chunking_with_metadata()         │
│       → Chunks now có đầy đủ metadata including pages        │
│                                                               │
│ 4.4.2 Call apipeline_process_enqueue_documents()             │
│       → Process queued documents with LLM                    │
│       → Store chunks với metadata vào Milvus                 │
│                                                               │
│ 4.4.3 Restore original chunking function                     │
│       rag.chunking_func = original_chunking_func             │
│       → Ensure không ảnh hưởng uploads khác                  │
│                                                               │
│ 4.4.4 Cleanup files                                          │
│       - Delete converted PDF (nếu DOCX đã convert)           │
│       - Delete original DOCX file (sau khi process xong)     │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              STEP 4.5: LLM EXTRACTION                         │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 4.5.1 Entity extraction (LLM call)                           │
│       Prompt: "Extract named entities from this text..."     │
│       Model: GPT-4 / Claude / Gemini (configured)            │
│                                                               │
│       Output example:                                         │
│       entities = [                                            │
│           {                                                   │
│               "entity_name": "Remote Work Policy",           │
│               "entity_type": "POLICY",                       │
│               "description": "Company policy allowing..."    │
│           },                                                  │
│           {                                                   │
│               "entity_name": "John Doe",                     │
│               "entity_type": "PERSON",                       │
│               "description": "CEO who approved policy"       │
│           }                                                   │
│       ]                                                       │
│                                                               │
│ 4.5.2 Relationship extraction (LLM call)                     │
│       Prompt: "Extract relationships between entities..."    │
│                                                               │
│       Output example:                                         │
│       relationships = [                                       │
│           {                                                   │
│               "src_id": "John Doe",                          │
│               "tgt_id": "Remote Work Policy",                │
│               "relationship": "APPROVED",                    │
│               "description": "John Doe approved policy in...",│
│               "weight": 0.95                                  │
│           }                                                   │
│       ]                                                       │
│                                                               │
│ 4.5.3 Cache LLM results (optional)                          │
│       Save to: llm_cache/{doc_id}_entities.json              │
│                llm_cache/{doc_id}_relationships.json         │
└──────────────────────────────────────────────────────────────┘
```

### **Phase 3: Storage (Milvus + Graph + JSON)**

```
┌─────────────────────────────────────────────────────────────┐
│              STEP 5: STORAGE OPERATIONS                      │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
┌────────────────────┐             ┌────────────────────┐
│  MILVUS STORAGE    │             │  GRAPH STORAGE     │
└────────────────────┘             └────────────────────┘
        │                                     │
        ▼                                     ▼
┌──────────────────────────────────────────────────────────────┐
│ 5.1 Store Chunks (chunks_vdb)                                │
│                                                               │
│ For each chunk:                                               │
│   chunk_data = {                                              │
│       "id": "chunk_hash_abc123",                             │
│       "full_doc_id": "doc-xyz789",                           │
│       "content": "Text content of chunk...",                 │
│       "embedding": [0.1, 0.2, 0.3, ...],  # 768 dims         │
│                                                               │
│       # Dynamic Fields (METADATA) ← KEY FEATURE              │
│       "file_url": "https://example.com/policy.pdf",          │
│       "table_name": "company_policies",                      │
│       "file_id": "POL_001",                                  │
│       "start_page": 5,          # Page where chunk starts   │
│       "end_page": 6,            # Page where chunk ends     │
│       "file_path": "policy_2024_v1.pdf"                      │
│   }                                                           │
│                                                               │
│   await milvus_client.insert(                                │
│       collection_name="chunks",                              │
│       data=[chunk_data]                                      │
│   )                                                           │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│ 5.2 Store Entities (entities_vdb)                            │
│                                                               │
│ For each entity:                                              │
│   entity_data = {                                             │
│       "entity_name": "Remote Work Policy",                   │
│       "entity_type": "POLICY",                               │
│       "description": "Policy allowing remote work...",       │
│       "source_id": "doc-xyz789",                             │
│       "embedding": [0.4, 0.5, ...],                          │
│                                                               │
│       # OPTIONAL: Dynamic fields for filtering              │
│       "file_id": "POL_001",                                  │
│       "table_name": "company_policies"                       │
│   }                                                           │
│                                                               │
│   # Entity deduplication logic                               │
│   existing = await entities_vdb.get(entity_name)             │
│   if existing:                                                │
│       # Merge descriptions                                   │
│       existing["description"] += f"; {new_description}"      │
│       existing["source_docs"].append("doc-xyz789")           │
│       await entities_vdb.update(entity_name, existing)       │
│   else:                                                       │
│       await entities_vdb.insert(entity_data)                 │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│ 5.3 Store Relationships (graph_storage)                      │
│                                                               │
│ For each relationship:                                        │
│   rel_data = {                                                │
│       "src_id": "John Doe",                                  │
│       "tgt_id": "Remote Work Policy",                        │
│       "relationship": "APPROVED",                            │
│       "description": "John Doe approved...",                 │
│       "weight": 0.95,                                         │
│       "source_id": "doc-xyz789",                             │
│       "keywords": "approved, policy, remote"                 │
│   }                                                           │
│                                                               │
│   # Relationship deduplication                               │
│   await graph_storage.add_edge(                              │
│       src=rel_data["src_id"],                                │
│       tgt=rel_data["tgt_id"],                                │
│       edge_data=rel_data                                     │
│   )                                                           │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│ 5.4 Store Document Status (JSON)                             │
│                                                               │
│ kv_store_doc_status.json:                                    │
│ {                                                             │
│   "doc-xyz789": {                                             │
│       "file_path": "policy_2024_v1.pdf",                     │
│       "status": "PROCESSED",                                  │
│       "doc_id": "doc-xyz789",                                │
│       "track_id": "upload_url_20260127_123456",              │
│       "created_at": "2026-01-27T10:00:00Z",                  │
│       "updated_at": "2026-01-27T10:05:23Z",                  │
│       "error": null,                                          │
│                                                               │
│       # Custom metadata (queryable)                          │
│       "file_url": "https://example.com/policy.pdf",          │
│       "table_name": "company_policies",                      │
│       "file_id": "POL_001"                                   │
│   }                                                           │
│ }                                                             │
│                                                               │
│ → Enables querying: "Get all docs with table_name=X"        │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────Example Chunk trong Milvus                               │
│                                                               │
│ {                                                             │
│   "id": "chunk_abc123",                                       │
│   "full_doc_id": "doc-xyz789",                               │
│   "content": "CHI TIẾT CÁC ĐẠI TRỤ (HASHIRA)...",           │
│   "embedding": [0.1, 0.2, ...],                              │
│                                                               │
│   // Dynamic Fields với Page Info                           │
│   "file_url": "https://s3.../cab4fa00-e0bb-41bb.docx",      │
│   "table_name": "3479aed0-f28e-49a8-85af-75ebaf2da5d9",     │
│   "file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",        │
│   "start_page": 1,    // ← Chunk bắt đầu từ page 1         │
│   "end_page": 2,      // ← Chunk kết thúc ở page 2         │
│   "file_path": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2.pdf"   │
│ }                                                             │
│                                                               │
│ → Page tracking cho phép:                                    │
│   - Filter chunks by page: start_page >= 5 && end_page <= 10│
│   - Display page references trong UI                        │
│   - Jump to exact page trong PDF viewer                      │
│ → Ensures không ảnh hưởng uploads khác                       │
└──────────────────────────────────────────────────────────────┘
```

---

## 1.4 Response Structure

### **Success Response (run_background=true)**

```json
{
    "status": "success",
    "message": "File 'policy_2024_v1.pdf' downloaded from URL and queued for processing with custom metadata.",
    "track_id": "upload_url_20260127_123456"
}
```

**Track Progress:**
```bash
GET /progress/{track_id}
→ Returns processing status (PENDING, PROCESSING, PROCESSED, FAILED)
```

### **Success Response (run_background=false)**

```json
{
    "status": "success",
    "message": "File 'policy_2024_v1.pdf' downloaded from URL and processed successfully with custom metadata.",
    "track_id": "upload_url_20260127_123456"
}
```

### **Duplicate Response**

```json
{
    "status": "duplicated",
    "message": "File 'policy_2024_v1.pdf' already exists in document storage (Status: PROCESSED).",
    "track_id": "upload_url_20260115_987654"
}
```

### **Error Responses**

**Invalid URL (400):**
```json
{
    "detail": "file_url must be a valid HTTP/HTTPS URL"
}
```

**Unsupported File Type (400):**
```json
{
    "detail": "Unsupported file type. Supported types: ['.txt', '.pdf', '.docx', ...]"
}
```

**Download Failed (500):**
```json
{
    "detail": "Failed to download file. HTTP 404: Not Found"
}
```

**Processing Failed (500):**
```json
{
    "detail": "File processing failed: LLM extraction error"
}
```

---

## 1.5 Storage Impact Chi Tiết

### **Milvus Collections**

**chunks_vdb:**
```
Collection: workspace_chunks
Schema:
  - id (primary key)
  - full_doc_id (indexed)
  - embedding (vector: 768 dims)
  - content (text)
  - Dynamic Fields: ← QUA     - Source URL của file
      * table_name (string)   - ← Enable delete_by_table_name()
      * file_id (string)      - ← Enable delete_by_file_id()
      * start_page (int)      - ← Page bắt đầu của chunk
      * end_page (int)        - ← Page kết thúc của chunk
      * file_path (string)    - Path to local file

Index: IVF_FLAT / HNSW on embedding

Note về Pages:
- PDF/DOCX: Real pages từ document structure
- PPTX/XLSX/TXT: Virtual pages (2000 chars/page)
- start_page = end_page → Chunk nằm trong 1 page
- start_page < end_page → Chunk span nhiều pages
- start_page = end_page = 0 → Không tìm thấy page info
Index: IVF_FLAT / HNSW on embedding
```

**entities_vdb:**
```
Collection: workspace_entities
Schema:
  - entity_name (primary key)
  - entity_type
  - description
  - source_id
  - embedding (vector)

Index: IVF_FLAT on embedding
```

### **Graph Storage (NetworkX)**

```python
# In-memory graph structure
graph = nx.DiGraph()

# Nodes (entities)
graph.add_node(
    "Remote Work Policy",
    entity_type="POLICY",
    description="...",
    source_docs=["doc-xyz789"]
)

# Edges (relationships)
graph.add_edge(
    "John Doe",
    "Remote Work Policy",
    relationship="APPROVED",
    weight=0.95,
    source_id="doc-xyz789"
)
```

### **JSON Storage**

**Files Created/Updated:**
1. `kv_store_doc_status.json` - Document status tracking
2. `kv_store_full_docs.json` - Full document text
3. `kv_store_text_chunks.json` - Chunked text
4. `kv_store_entity_chunks.json` - Entity→chunks mapping
5. `kv_store_relation_chunks.json` - Relationship→chunks mapping

---

## 1.9 Page Tracking Logic Chi Tiết

### **Tại Sao Cần Page Tracking?**

Page tracking cho phép system ghi nhớ **chunk nào nằm ở page nào** trong document gốc. Điều này quan trọng vì:

1. **Reference Accuracy:** User query → System return answer với reference "Page 5-7"
2. **Navigation:** Click vào chunk → Jump đến exact page trong PDF viewer
3. **Filtering:** Query chỉ trong pages 10-20 của document
4. **Audit Trail:** Track thông tin được extract từ pages nào

### **Workflow Chi Tiết**

```
┌─────────────────────────────────────────────────────────────┐
│              PHASE 1: FILE TYPE DETECTION                    │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
   .docx/.doc                            .pdf file
        │                                     │
        ▼                                     │
┌──────────────────┐                          │
│ Convert to PDF   │                          │
│ _convert_docx_   │                          │
│    to_pdf()      │                          │
└──────────────────┘                          │
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              PHASE 2: PAGE EXTRACTION                        │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
   PDF/DOCX                          PPTX/XLSX/TXT/...
 (Real Pages)                        (Virtual Pages)
        │                                     │
        ▼                                     ▼
┌────────────────────┐              ┌────────────────────┐
│ _extract_pdf_pypdf │              │ _extract_text_with │
│ (return_pages_info │              │   _virtual_pages   │
│      =True)        │              └────────────────────┘
└────────────────────┘                        │
        │                                     │
        ▼                                     ▼
   Extract real                      Create virtual pages
   pages from PDF                    (2000 chars per page)
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                           ▼
                    Return tuple:
              (full_text, pages_info)
```

### **pages_info Structure**

```python
# Example cho PDF 3 pages
pages_info = [
    {
        "page": 1,          # Page number (1-indexed)
        "char_start": 0,    # Vị trí bắt đầu trong full_text
        "char_end": 1523    # Vị trí kết thúc (exclusive)
    },
    {
        "page": 2,
        "char_start": 1523,  # Bắt đầu ngay sau page 1
        "char_end": 3104
    },
    {
        "page": 3,
        "char_start": 3104,
        "char_end": 4500
    }
]

# full_text structure:
# chars [0:1523]    → Page 1 content
# chars [1523:3104] → Page 2 content  
# chars [3104:4500] → Page 3 content
```

### **Chunk → Page Assignment Algorithm**

```python
def assign_page_to_chunk(chunk_text, full_text, pages_info):
    """
    Tìm chunk trong full_text và map với pages_info
    để xác định start_page và end_page
    """
    
    # Step 1: Find chunk position in full_text
    chunk_start = full_text.find(chunk_text)
    
    if chunk_start == -1:
        # Fuzzy matching với first 100 chars
        chunk_preview = chunk_text[:100]
        chunk_start = full_text.find(chunk_preview)
        if chunk_start == -1:
            return 0, 0  # Not found
        chunk_end = chunk_start + len(chunk_preview)
    else:
        chunk_end = chunk_start + len(chunk_text)
    
    # Step 2: Find start_page
    start_page = 0
    for page_info in pages_info:
        if page_info["char_start"] <= chunk_start < page_info["char_end"]:
            start_page = page_info["page"]
            break
    
    # Step 3: Find end_page
    end_page = 0
    for page_info in pages_info:
        if page_info["char_start"] < chunk_end <= page_info["char_end"]:
            end_page = page_info["page"]
            break
    
    # Step 4: Fallbacks
    if start_page == 0:
        start_page = pages_info[0]["page"]
    if end_page == 0:
        end_page = pages_info[-1]["page"]
    if end_page < start_page:
        end_page = start_page
    
    return start_page, end_page
```

### **Examples**

**Example 1: Chunk nằm hoàn toàn trong 1 page**
```python
# Page 1: chars [0:1523]
# Chunk: chars [100:500]

chunk_start = 100  # → Falls in page 1
chunk_end = 500    # → Falls in page 1

→ start_page = 1, end_page = 1
```

**Example 2: Chunk span nhiều pages**
```python
# Page 1: chars [0:1523]
# Page 2: chars [1523:3104]
# Chunk: chars [1400:2000]

chunk_start = 1400  # → Falls in page 1 (0 <= 1400 < 1523)
chunk_end = 2000    # → Falls in page 2 (1523 < 2000 <= 3104)

→ start_page = 1, end_page = 2
```

**Example 3: Chunk ở page boundary**
```python
# Page 1: chars [0:1523]
# Page 2: chars [1523:3104]
# Chunk: chars [1523:2000]

chunk_start = 1523  # → Exactly at boundary
                    # → Assigned to page 2 (next page)
chunk_end = 2000    # → Falls in page 2

→ start_page = 2, end_page = 2
```

**Example 4: Chunk không tìm thấy (fuzzy match fail)**
```python
chunk_text = "Some text not in document"
chunk_start = -1  # find() returns -1

→ start_page = 0, end_page = 0
```

### **Virtual Pages vs Real Pages**

**Real Pages (PDF/DOCX):**
```
✓ Exact page numbers from document structure
✓ Match với PDF viewer page numbers
✓ Accurate cho document navigation
✓ Best for user experience

File types: .pdf, .docx, .doc (sau khi convert)
```

**Virtual Pages (PPTX/XLSX/TXT/...):**
```
✓ Logical pages created by system (2000 chars/page)
✓ Consistent chunking across non-paginated formats
✓ Still provide page references for filtering
⚠ Page numbers không match với original file structure

File types: .pptx, .xlsx, .txt, .md, .html, etc.

Algorithm:
  page_size = 2000 chars
  virtual_page_num = (char_position // page_size) + 1
  
  Example:
    char 0-1999    → Virtual page 1
    char 2000-3999 → Virtual page 2
    char 4000-5999 → Virtual page 3
```

### **DOCX → PDF Conversion Logic**

**Tại sao convert?**
- DOCX không có native page structure trong text extraction
- Convert sang PDF → Extract real page boundaries
- User experience tốt hơn với real page numbers

**Conversion methods (priority order):**

1. **docx2pdf (Windows với MS Word):**
```python
# Best quality, preserves formatting
from docx2pdf import convert
convert("document.docx", "document.pdf")
```

2. **pypandoc (cross-platform với Pandoc):**
```python
# Good quality, widely available
import pypandoc
pypandoc.convert_file(
    "document.docx",
    "pdf",
    outputfile="document.pdf"
)
```

**Cleanup sau conversion:**
```python
# After successful processing:
1. Delete converted PDF (temporary file)
2. Delete original DOCX (processed rồi)

# On error:
   Cleanup cả 2 files ngay lập tức
```

### **Metadata Injection Flow**

```
┌─────────────────────────────────────────────────────────────┐
│              ORIGINAL CHUNKING FUNCTION                      │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
               Input: full_text (string)
                           │
                           ▼
                  Split vào chunks
                           │
                           ▼
               Output: List[Dict] chunks
                  [
                      {"content": "text...", ...},
                      {"content": "text...", ...}
                  ]
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              WRAPPER WITH METADATA INJECTION                 │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
             For each chunk in chunks:
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
  Basic Metadata                      Page Assignment
        │                                     │
        ▼                                     ▼
  chunk["file_url"] = ...         start, end = assign_page_to_chunk(
  chunk["table_name"] = ...           chunk["content"],
  chunk["file_id"] = ...              full_text_for_tracking,
                                      pages_info_for_tracking
                                  )
                                  chunk["start_page"] = start
                                  chunk["end_page"] = end
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                           ▼
              Output: Enriched chunks
                  [
                      {
                          "content": "text...",
                          "file_url": "...",
                          "table_name": "...",
                          "file_id": "...",
                          "start_page": 1,
                          "end_page": 2
                      },
                      ...
                  ]
                           │
                           ▼
               Store vào Milvus Dynamic Fields
```

### **Query với Page Filtering**

```python
# Example: Query chunks trong pages 5-10
filter_expr = "start_page >= 5 AND end_page <= 10"

results = milvus_client.query(
    collection_name="workspace_chunks",
    filter=filter_expr,
    output_fields=["content", "start_page", "end_page"]
)

# Return:
# [
#     {"content": "...", "start_page": 5, "end_page": 6},
#     {"content": "...", "start_page": 7, "end_page": 7},
#     {"content": "...", "start_page": 9, "end_page": 10}
# ]
```

### **Edge Cases**

**Case 1: Empty page**
```python
# Page có content rỗng hoặc chỉ có whitespace
pages_info = [
    {"page": 1, "char_start": 0, "char_end": 100},
    {"page": 2, "char_start": 100, "char_end": 100},  # Empty!
    {"page": 3, "char_start": 100, "char_end": 200}
]

# Chunks có thể skip page 2 nếu không có content
```

**Case 2: Chunk lớn hơn 1 page**
```python
# Chunk: 3000 chars
# Page size: 1500 chars/page

→ start_page = 1, end_page = 2  # Span 2 pages
```

**Case 3: Multi-line breaks**
```python
# PDF extraction thêm \n sau mỗi page
full_text = "Page 1 content\nPage 2 content\nPage 3 content"

# Chunk có thể include \n characters
# → assign_page_to_chunk vẫn tính đúng vì dựa vào char position
```

**Case 4: Special characters trong chunk**
```python
# Chunk contains: "Policy 2024 – Overview"
# full_text contains: "Policy 2024 - Overview"  # Different dash!

→ find() fails → Fuzzy match với first 100 chars
→ Vẫn assign được pages (partial match)
```

---

## 1.10 Best Practices for Page Tracking

### **✅ DO:**

1. **Convert DOCX to PDF trước khi process:**
```python
# System tự động convert, nhưng ensure dependencies installed
pip install docx2pdf  # Windows
pip install pypandoc  # Cross-platform
```

2. **Use page filters trong query:**
```python
# Query specific section của document
filter_expr = "file_id == 'DOC_001' AND start_page >= 10 AND end_page <= 20"
```

3. **Display page references trong UI:**
```typescript
// Frontend
<Reference>
  Source: {chunk.file_path}
  Pages: {chunk.start_page}-{chunk.end_page}
</Reference>
```

4. **Enable PDF viewer navigation:**
```typescript
// Click reference → Open PDF at specific page
openPDF(chunk.file_url, chunk.start_page)
```

### **❌ DON'T:**

1. **Rely on page numbers cho non-PDF formats:**
```python
# ❌ Bad: Assume .xlsx has real pages
chunk["start_page"]  # → Virtual page, không match Excel

# ✅ Good: Check file type
if file_ext in [".pdf", ".docx"]:
    # Real pages
else:
    # Virtual pages (informational only)
```

2. **Filter by exact page match cho span chunks:**
```python
# ❌ Bad: Miss chunks spanning multiple pages
filter = "start_page == 5 AND end_page == 5"

# ✅ Good: Use range query
filter = "start_page <= 5 AND end_page >= 5"
```

3. **Forget cleanup converted files:**
```python
# ❌ Bad: Keep all converted PDFs
# → Disk space explosion

# ✅ Good: Auto cleanup in finally block
finally:
    if converted_pdf_path:
        converted_pdf_path.unlink()
```

---

## 1.11 Performance Considerations cho Page Tracking

### **Bottlenecks**

1. **Download Speed:** Network-dependent (300s timeout)
2. **LLM Extraction:** Slowest step (~30-60s per document)
   - Entity extraction: ~20s
   - Relationship extraction: ~20s
3. **Milvus Insertion:** Fast (~1-2s per 100 chunks)

### **Optimization Strategies**

**1. Use `run_background=true` for large files:**
```json
{
    "run_background": true
}
// Returns immediately with track_id
```

**2. Enable LLM caching:**
```python
enable_llm_cache=True  # In server config
# Cache reused if same doc_id + content
```

**3. Batch uploads:**
```bash
# Upload multiple files in parallel
for url in urls:
    curl -X POST /upload_from_url -d "{...}" &
done
wait
```

### **Resource Usage**

| Operation | CPU | Memory | Disk I/O | Network |
|-----------|-----|--------|----------|---------|
| Download | Low | Low | Medium | High |
| Text Extract | Medium | Medium | High | None |
| LLM Call | Low* | Low | None | High |
| Chunking | Medium | Medium | Low | None |
| Embedding | High** | Medium | None | High*** |
| Milvus Insert | Low | Medium | High | None |

*If using local LLM: High CPU/GPU  
**If using local embedding model  
***If using API embedding service

---

## 1.7 Error Handling & Recovery

### **Failure Scenarios**

**Scenario 1: Download fails mid-way**
```
Status: Partial file written to disk
Recovery:
  1. Automatic retry (3 attempts with exponential backoff)
  2. If all fail → Return 500 error
  3. Partial file auto-deleted
  4. User can retry request
```

**Scenario 2: LLM extraction fails**
```
Status: File downloaded, chunks created, but no entities
Recovery:
  1. doc_status set to FAILED with error message
  2. Chunks still stored (searchable by content)
  3. User can:
     - Check /documents/all?status=FAILED
     - Trigger reprocess: POST /documents/reprocess_failed
```

**Scenario 3: Milvus connection lost**
```
Status: Data extracted but not stored
Recovery:
  1. Exception caught, transaction rolled back
  2. doc_status remains PENDING
  3. Auto-retry on next pipeline run
  4. Or manual: POST /documents/reprocess_failed
```

### **Monitoring**

**Check upload status:**
```bash
# By track_id
GET /progress/{track_id}

# By file_path
GET /documents/all?file_path=policy_2024_v1.pdf

# Failed uploads
GET /documents/all?status=FAILED
```

---

## 1.8 Best Practices

### **✅ DO:**

1. **Always validate URLs trước khi gọi API:**
```javascript
// Client-side validation
const isValidUrl = (url) => {
    try {
        new URL(url);
        return url.startsWith('http');
    } catch {
        return false;
    }
};
```

2. **Sử dụng unique file_id:**
```python
# Good: UUID hoặc timestamp-based
file_id = f"DOC_{uuid.uuid4()}"
file_id = f"DOC_{datetime.now().isoformat()}"

# Bad: Non-unique
file_id = "document_1"  # ← Conflict với doc khác
```

3. **Set run_background=false cho critical documents:**
```json
{
    "run_background": false,
    // Wait để ensure upload success trước khi proceed
}
```

4. **Monitor với track_id:**
```python
# Upload
response = requests.post("/upload_from_url", json={...})
track_id = response.json()["track_id"]

# Poll status
while True:
    status = requests.get(f"/progress/{track_id}").json()
    if status["status"] in ["PROCESSED", "FAILED"]:
        break
    time.sleep(5)
```

### **❌ DON'T:**

1. **Upload sensitive files qua public URL:**
```python
# ❌ Bad: API có thể log URL
file_url = "https://drive.google.com/file/d/SECRET_TOKEN/view"

# ✅ Good: Use signed URLs với expiration
file_url = "https://storage.com/file.pdf?token=ABC&expires=3600"
```

2. **Reuse file_id cho different files:**
```python
# ❌ Bad: Replace unintentionally
upload(file_id="DOC_001", url="report_jan.pdf")
upload(file_id="DOC_001", url="report_feb.pdf")  # ← Duplicate!

# ✅ Good: Unique IDs hoặc intentional replace
upload(file_id="DOC_001_JAN", ...)
upload(file_id="DOC_001_FEB", ...)
```

3. **Upload quá large files (>100MB) synchronously:**
```python
# ❌ Bad: Timeout risk
upload(..., run_background=False)  # May timeout nếu file lớn

# ✅ Good: Background cho large files
upload(..., run_background=True)
```

---

# 2. API: DELETE /documents/delete_by_table_name

## 2.1 Mục Đích & Use Cases

**Mô tả:**  
Xóa tất cả documents có chunks matching `table_name` trong Milvus dynamic fields. Đây là batch deletion operation để clean up entire category of documents.

**Use Cases:**
- Xóa toàn bộ documents của một tenant (multi-tenant system)
- Clean up expired documents (e.g., "temp_uploads" table)
- Reset một category trước khi bulk re-import
- Compliance: Delete data sau retention period

---

## 2.2 Request Structure

**Endpoint:** `DELETE /documents/delete_by_table_name`

**Request Body:**
```json
{
    "table_name": "company_policies",
    "delete_file": false,
    "delete_llm_cache": true,
    "run_background": true
}
```

**Parameters:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `table_name` | string | ✅ | - | Table name để filter documents. Must match exactly (case-sensitive) |
| `delete_file` | boolean | ❌ | false | Có xóa physical files trong input directory không |
| `delete_llm_cache` | boolean | ❌ | false | Có xóa cached LLM extraction results không |
| `run_background` | boolean | ❌ | true | Background processing mode |

---

## 2.3 Workflow Chi Tiết

### **Phase 1: Query & Discovery**

```
┌─────────────────────────────────────────────────────────────┐
│           STEP 1: QUERY MILVUS FOR MATCHING DOCS             │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 1.1 Build Milvus filter expression                           │
│                                                               │
│     filter_expr = f'table_name == "{table_name}"'            │
│     # Example: 'table_name == "company_policies"'            │
│                                                               │
│ 1.2 Query chunks collection với iterator                    │
│                                                               │
│     iterator = milvus_client.query_iterator(                 │
│         collection_name="workspace_chunks",                  │
│         filter=filter_expr,                                  │
│         batch_size=1000,                                     │
│         output_fields=["full_doc_id"]                        │
│     )                                                         │
│                                                               │
│ 1.3 Collect unique doc_ids                                   │
│                                                               │
│     doc_ids = set()  # Use set để auto-deduplicate          │
│     while True:                                              │
│         batch = iterator.next()                              │
│         if not batch: break                                  │
│                                                               │
│         for item in batch:                                   │
│             doc_ids.add(item["full_doc_id"])                 │
│                                                               │
│     # Example result:                                         │
│     # doc_ids = {                                             │
│     #     "doc-abc123",                                       │
│     #     "doc-def456",                                       │
│     #     "doc-ghi789"                                        │
│     # }                                                       │
│                                                               │
│ 1.4 Return early if no documents found                      │
│                                                               │
│     if len(doc_ids) == 0:                                    │
│         return {                                              │
│             "status": "not_found",                           │
│             "message": "No documents with table_name=..."    │
│         }                                                     │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
                  Found: 3 documents
```

### **Phase 2: Pipeline Lock & Batch Deletion**

```
┌─────────────────────────────────────────────────────────────┐
│           STEP 2: ACQUIRE PIPELINE LOCK                      │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2.1 Check pipeline status                                    │
│                                                               │
│     async with pipeline_status_lock:                         │
│         if pipeline_status["busy"]:                          │
│             return {                                          │
│                 "status": "not_allowed",                     │
│                 "message": "Pipeline busy"                   │
│             }                                                 │
│                                                               │
│ 2.2 Set pipeline to busy                                     │
│                                                               │
│     pipeline_status.update({                                 │
│         "busy": True,                                        │
│         "job_name": "Deleting 3 Documents by table_name",   │
│         "docs": 3,                                           │
│         "cur_batch": 0,                                      │
│         "latest_message": "Starting deletion...",            │
│         "history_messages": [...]                            │
│     })                                                        │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│           STEP 3: DELETE EACH DOCUMENT                       │
└─────────────────────────────────────────────────────────────┘
                           │
         ┌─────────────────┴────────────────┐
         │                                  │
         ▼                                  ▼
    Document 1/3                       Document 2/3 ...
         │                                  
         ▼                                  
┌──────────────────────────────────────────────────────────────┐
│ 3.1 Call adelete_by_doc_id(doc_id)                          │
│                                                               │
│     → Xem Section 1.3 để hiểu chi tiết adelete_by_doc_id    │
│                                                               │
│     Summary of deletion steps:                               │
│     ✓ Delete chunks from Milvus (chunks_vdb)                │
│     ✓ Delete entities from Milvus (entities_vdb)            │
│     ✓ Delete relationships from graph (graph_storage)       │
│     ✓ Delete doc_status entry (JSON)                        │
│     ✓ Delete KV store entries                               │
│     ✓ Delete LLM cache (if delete_llm_cache=true)           │
│     ✓ Delete physical file (if delete_file=true)            │
│                                                               │
│ 3.2 Track result                                             │
│                                                               │
│     if deletion_success:                                     │
│         deleted_docs.append(doc_id)                          │
│     else:                                                    │
│         failed_docs.append({                                 │
│             "doc_id": doc_id,                                │
│             "error": error_message                           │
│         })                                                    │
│                                                               │
│ 3.3 Update pipeline progress                                │
│                                                               │
│     pipeline_status["cur_batch"] = i                         │
│     pipeline_status["latest_message"] = f"Deleted {i}/3"    │
│     pipeline_status["history_messages"].append(...)          │
│                                                               │
│ 3.4 Check for cancellation                                  │
│                                                               │
│     if pipeline_status["cancellation_requested"]:           │
│         # Stop deletion, mark remaining as failed           │
│         break                                                │
└──────────────────────────────────────────────────────────────┘
```

### **Phase 3: Cleanup & Response**

```
┌─────────────────────────────────────────────────────────────┐
│           STEP 4: FINALIZATION                               │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 4.1 Release pipeline lock                                    │
│                                                               │
│     finally:                                                  │
│         async with pipeline_status_lock:                     │
│             pipeline_status["busy"] = False                  │
│             pipeline_status["latest_message"] = "Complete"   │
│                                                               │
│ 4.2 Determine overall status                                │
│                                                               │
│     if len(deleted_docs) == total_docs:                      │
│         status = "success"                                   │
│     elif len(deleted_docs) > 0:                              │
│         status = "partial_success"                           │
│     else:                                                    │
│         status = "failure"                                   │
│                                                               │
│ 4.3 Return response                                          │
│                                                               │
│     return {                                                  │
│         "status": status,                                    │
│         "table_name": "company_policies",                    │
│         "total_docs": 3,                                     │
│         "deleted_docs": ["doc-abc123", "doc-def456", ...],  │
│         "failed_docs": [],                                   │
│         "message": "Successfully deleted 3/3 documents"      │
│     }                                                         │
└──────────────────────────────────────────────────────────────┘
```

---

## 2.4 Response Examples

### **Success Response**

```json
{
    "status": "success",
    "table_name": "company_policies",
    "total_docs": 15,
    "deleted_docs": [
        "doc-abc123",
        "doc-def456",
        "doc-ghi789",
        "..."
    ],
    "failed_docs": [],
    "message": "Successfully deleted all 15 documents with table_name='company_policies'"
}
```

### **Partial Success Response**

```json
{
    "status": "partial_success",
    "table_name": "temp_uploads",
    "total_docs": 10,
    "deleted_docs": [
        "doc-aaa111",
        "doc-bbb222",
        "doc-ccc333",
        "..."
    ],
    "failed_docs": [
        {
            "doc_id": "doc-ddd444",
            "error": "Milvus connection timeout"
        },
        {
            "doc_id": "doc-eee555",
            "error": "Graph data corrupted"
        }
    ],
    "message": "Partially deleted documents with table_name='temp_uploads': 8/10 succeeded, 2 failed"
}
```

### **Not Found Response**

```json
{
    "status": "not_found",
    "table_name": "non_existent_table",
    "total_docs": 0,
    "deleted_docs": [],
    "failed_docs": [],
    "message": "No documents found with table_name='non_existent_table'"
}
```

### **Pipeline Busy Response**

```json
{
    "status": "not_allowed",
    "table_name": "company_policies",
    "total_docs": 0,
    "deleted_docs": [],
    "failed_docs": [],
    "message": "Cannot delete documents while pipeline is busy"
}
```

---

## 2.5 Impact Trên Storage Layers

### **Milvus Collections**

**chunks_vdb:**
```python
# Before deletion (table_name="policies")
Query: SELECT full_doc_id FROM chunks WHERE table_name="policies"
Result: [doc-1, doc-2, doc-3]

# After deletion
Query: SELECT full_doc_id FROM chunks WHERE table_name="policies"
Result: []  # Empty

# Other tables không bị ảnh hưởng
Query: SELECT full_doc_id FROM chunks WHERE table_name="reports"
Result: [doc-10, doc-11, ...]  # Vẫn còn
```

**entities_vdb:**
```python
# Entities chỉ bị xóa nếu KHÔNG còn trong documents khác

# Scenario 1: Entity chỉ trong documents của table này
Entity: "Policy 2024" (only in table="policies")
→ DELETED completely

# Scenario 2: Entity còn trong documents khác
Entity: "John Doe" (in table="policies" AND table="reports")
→ NOT DELETED
→ But references to "policies" docs removed from source_docs list
```

**relationships:**
```python
# Tương tự entities

# Relationship chỉ trong deleted docs
[Policy 2024] --APPLIES_TO--> [Employees]
(only in table="policies")
→ DELETED

# Relationship còn trong docs khác  
[John Doe] --WORKS_AT--> [Company XYZ]
(in table="policies" AND table="org_chart")
→ KEPT (but weight may be adjusted)
```

### **JSON Storage**

**kv_store_doc_status.json:**
```json
// Before
{
    "doc-1": {"table_name": "policies", ...},
    "doc-2": {"table_name": "policies", ...},
    "doc-10": {"table_name": "reports", ...}
}

// After deletion (table_name="policies")
{
    "doc-10": {"table_name": "reports", ...}
}
// doc-1, doc-2 entries REMOVED
```

---

## 2.6 Performance & Scalability

### **Performance Metrics**

| Documents | Query Time | Deletion Time | Total Time |
|-----------|------------|---------------|------------|
| 10 | ~1s | ~10s | ~11s |
| 100 | ~3s | ~100s | ~103s |
| 1,000 | ~10s | ~1000s | ~17 mins |
| 10,000 | ~30s | ~10000s | ~3 hours |

**Bottleneck:** Sequential deletion of documents (1 doc at a time)

### **Optimization Tips**

**1. Run in background:**
```json
{
    "run_background": true
    // Returns immediately, process async
}
```

**2. Monitor progress:**
```bash
GET /progress/pipeline
# Returns:
{
    "busy": true,
    "cur_batch": 500,
    "batchs": 1000,
    "latest_message": "Deleting document 500/1000"
}
```

**3. Cancel if needed:**
```bash
POST /pipeline/cancel
# Stops deletion after current document
```

---

## 2.7 Safety Considerations

### **⚠️ WARNINGS:**

1. **Operation KHÔNG THỂ ROLLBACK:**
```
Once deleted, data cannot be recovered
→ Ensure backup trước khi delete large batches
```

2. **Shared entities có thể mất context:**
```
Entity "John Doe" appears in:
  - table="policies" (context: "CEO")
  - table="reports" (context: "Author")

Delete table="policies"
→ "John Doe" mất context "CEO"
→ Chỉ còn context "Author"
```

3. **Graph structure thay đổi:**
```
Before: [A] --r1--> [B] --r2--> [C]
Delete docs containing r1
After: [A]    [B] --r2--> [C]
→ Path từ A đến C bị broken
```

### **✅ Best Practices:**

**1. Preview trước khi delete:**
```bash
# Query để xem sẽ delete bao nhiêu docs
POST /query/data
{
    "query": "",
    "filter": {"table_name": "temp_uploads"}
}
# Count results
```

**2. Delete từng bước (tiệm tiến):**
```python
# Thay vì delete all ngay
delete_by_table_name("old_data")  # 10,000 docs

# Delete theo batches nhỏ
tables = ["old_data_2020", "old_data_2021", ...]
for table in tables:
    delete_by_table_name(table)  # 1,000 docs each
    time.sleep(60)  # Cool down
```

**3. Keep audit log:**
```python
# Before deletion
docs = get_documents_by_table_name("policies")
save_to_backup(docs)

# Delete
result = delete_by_table_name("policies")

# Log
audit_log.append({
    "action": "delete_by_table_name",
    "table": "policies",
    "deleted_count": result["total_docs"],
    "timestamp": now(),
    "user": current_user
})
```

---

Tiếp tục với các API còn lại... 

*[Document sẽ được bổ sung với 3 API còn lại: delete_by_file_id, query/data, và replace_file_by_id trong phần tiếp theo]*

---

# 3. API: DELETE /documents/delete_by_file_id

## 3.1 Mục Đích & Use Cases

**Mô tả:**  
Xóa tất cả documents có chunks matching `file_id` trong Milvus dynamic fields. Tương tự delete_by_table_name nhưng filter theo file_id thay vì table_name.

**Use Cases:**
- **Replace file:** Delete old version trước khi upload new version với same file_id
- **Remove specific file:** Delete by unique identifier (không cần biết filename)
- **Cleanup after error:** Remove partial uploads nếu processing bị fail giữa chừng
- **Data privacy:** Delete user's uploaded file on request

---

## 3.2 Request Structure

**Endpoint:** `DELETE /documents/delete_by_file_id`

**Request Body:**
```json
{
    "file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "delete_file": false,
    "delete_llm_cache": true,
    "run_in_background": false
}
```

**Parameters:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `file_id` | string | ✅ | - | File ID để filter documents. Must match exactly |
| `delete_file` | boolean | ❌ | false | Có xóa physical file không (use with caution - file có thể không exist nếu upload từ URL) |
| `delete_llm_cache` | boolean | ❌ | false | Có xóa cached LLM results không |
| `run_in_background` | boolean | ❌ | true | Background processing mode |

---

## 3.3 Workflow Chi Tiết

Workflow hoàn toàn giống `delete_by_table_name`, chỉ khác filter expression:

```python
# delete_by_table_name:
filter_expr = f'table_name == "{table_name}"'

# delete_by_file_id:
filter_expr = f'file_id == "{file_id}"'
```

### **Key Steps:**

1. **Query Milvus:** `milvus.get_doc_ids_by_file_id(file_id)`
2. **Acquire Pipeline Lock:** Prevent concurrent operations
3. **Delete Each Document:** Call `adelete_by_doc_id()` per doc
4. **Track Progress:** Update `pipeline_status` for monitoring
5. **Return Results:** List of deleted/failed documents

**Xem Section 2.3 để hiểu chi tiết workflow (identical logic)**

---

## 3.4 Differences từ delete_by_table_name

| Aspect | delete_by_table_name | delete_by_file_id |
|--------|---------------------|-------------------|
| **Filter Field** | `table_name` | `file_id` |
| **Typical Usage** | Bulk category deletion | Single file deletion |
| **Expected Count** | Multiple documents (10-1000s) | Usually 1 document (1 file) |
| **Use Case** | Tenant cleanup, category reset | File replacement, privacy delete |
| **Performance** | Slower (nhiều docs) | Faster (1-few docs) |

---

## 3.5 Response Examples

### **Success Response**

```json
{
    "status": "success",
    "file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "total_docs": 1,
    "deleted_docs": [
        "doc-xyz789"
    ],
    "failed_docs": [],
    "message": "Successfully deleted all 1 documents with file_id='cab4fa00-e0bb-41bb-8b2b-45a5c46041d2'"
}
```

### **Not Found Response**

```json
{
    "status": "not_found",
    "file_id": "non_existent_file_id",
    "total_docs": 0,
    "deleted_docs": [],
    "failed_docs": [],
    "message": "No documents found with file_id='non_existent_file_id'"
}
```

---

## 3.6 Best Practices

### **✅ DO:**

**1. Use với replace_file_by_id API:**
```python
# Correct replace workflow
1. delete_by_file_id(file_id="FILE_001", delete_llm_cache=True)
2. upload_from_url(file_id="FILE_001", url="new_version.pdf")
```

**2. Set run_in_background=false cho immediate operations:**
```json
{
    "file_id": "urgent_delete",
    "run_in_background": false
    // Wait để confirm deletion trước khi proceed
}
```

**3. Query trước để verify:**
```bash
# Check có bao nhiêu documents sẽ bị delete
POST /query/data
{
    "query": "",
    "filter": {"file_id": "FILE_001"}
}
```

### **❌ DON'T:**

**1. Set delete_file=true nếu uploaded từ URL:**
```python
# ❌ Bad: File không exist locally
delete_by_file_id(
    file_id="from_url_file",
    delete_file=True  # File doesn't exist!
)

# ✅ Good: Only delete data
delete_by_file_id(
    file_id="from_url_file",
    delete_file=False
)
```

**2. Assume file_id unique across workspaces:**
```python
# ❌ Bad: Same file_id in different workspaces
workspace_a.delete_by_file_id("FILE_001")  # OK
workspace_b.delete_by_file_id("FILE_001")  # Different file!

# ✅ Good: Include workspace in file_id
file_id = f"{workspace_id}_{file_id}"
```

---

# 4. API: POST /query/data

## 4.1 Mục Đích & Use Cases

**Mô tả:**  
Query dữ liệu từ RAG system và return **structured data** (entities, relationships, chunks, references) thay vì generated answer. Khác với `/query` endpoint return text response, `/query/data` return raw retrieval data.

**Use Cases:**
- **Frontend custom rendering:** Display entities và relationships trong graph visualization
- **Data analysis:** Extract structured knowledge graph data
- **RAG pipeline integration:** Get intermediate retrieval data cho custom processing
- **Citation system:** Build references list với exact source tracking
- **Debug/Audit:** Inspect retrieval results trước khi LLM generation

---

## 4.2 Request Structure

**Endpoint:** `POST /query/data`

**Request Body:**
```json
{
    "query": "CHI TIẾT CÁC ĐẠI TRỤ",
    "mode": "local",
    "top_k": 10,
    "chunk_top_k": 5,
    "hl_keywords": ["Hashira", "Demon Slayer"],
    "ll_keywords": ["swordsmanship", "breathing techniques"],
    "include_references": true
}
```

**Parameters:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `query` | string | ✅ | - | Query text (min 3 chars) |
| `mode` | string | ❌ | "mix" | Query mode: "local", "global", "hybrid", "naive", "mix", "bypass" |
| `top_k` | int | ❌ | 20 | Number of entities (local) hoặc relationships (global) to retrieve |
| `chunk_top_k` | int | ❌ | 5 | Number of chunks to retrieve from vector search |
| `hl_keywords` | list[str] | ❌ | [] | High-level keywords (empty → LLM generate) |
| `ll_keywords` | list[str] | ❌ | [] | Low-level keywords (empty → LLM generate) |
| `include_references` | bool | ❌ | true | Include reference list in response |
| `max_entity_tokens` | int | ❌ | null | Max tokens for entity context |
| `max_relation_tokens` | int | ❌ | null | Max tokens for relationship context |
| `max_total_tokens` | int | ❌ | null | Max total tokens budget |

**Note về keywords:**
- Nếu `hl_keywords` và `ll_keywords` empty → LLM sẽ generate từ query
- Nếu provided → Use trực tiếp (skip LLM keyword extraction step)

---

## 4.3 Workflow Chi Tiết

### **Phase 1: Keyword Extraction**

```
┌─────────────────────────────────────────────────────────────┐
│              STEP 1: KEYWORD GENERATION                      │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
  Keywords Provided?                  Keywords Empty?
        │                                     │
        ▼                                     ▼
    Use Directly                    Call LLM to Extract
        │                                     │
        │                                     ▼
        │                         ┌─────────────────────┐
        │                         │ Prompt Template:    │
        │                         │ "Given query: X     │
        │                         │  Extract:           │
        │                         │  - High-level terms │
        │                         │  - Low-level terms" │
        │                         └─────────────────────┘
        │                                     │
        │                                     ▼
        │                         LLM returns JSON:
        │                         {
        │                           "high_level_keywords": ["A", "B"],
        │                           "low_level_keywords": ["x", "y"]
        │                         }
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                           ▼
               Store keywords for retrieval
```

**Example:**

```python
# Query: "CHI TIẾT CÁC ĐẠI TRỤ"

# If hl_keywords = [], ll_keywords = []
# → LLM generates:
hl_keywords = ["Hashira", "Dai Tru", "Demon Slayer"]
ll_keywords = ["Flame Pillar", "Water Pillar", "breathing techniques"]

# If provided:
hl_keywords = ["Hashira", "Pillars"]  # Use these
ll_keywords = ["swordsmanship"]       # Use these
```

---

### **Phase 2: Data Retrieval by Mode**

```
┌─────────────────────────────────────────────────────────────┐
│              MODE SELECTION LOGIC                            │
└─────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
    mode="local"                        mode="global"
        │                                     │
        ▼                                     ▼
┌────────────────────┐              ┌────────────────────┐
│ ENTITY-FOCUSED     │              │ RELATIONSHIP-FOCUSED│
│ RETRIEVAL          │              │ RETRIEVAL           │
└────────────────────┘              └────────────────────┘
        │                                     │
        ▼                                     ▼
  Retrieve entities               Retrieve relationships
  matching keywords               matching keywords
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                           ▼
                   Retrieve related chunks
                           │
                           ▼
                    Build references
```

---

### **Mode: LOCAL (Entity-Focused)**

```
┌─────────────────────────────────────────────────────────────┐
│              STEP 2A: LOCAL MODE RETRIEVAL                   │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2A.1 Search entities_vdb                                     │
│                                                               │
│      # Embed high-level keywords                             │
│      keyword_embedding = embed_model(hl_keywords)            │
│                                                               │
│      # Vector search in entities collection                  │
│      entities = milvus.search(                               │
│          collection="workspace_entities",                    │
│          query_vector=keyword_embedding,                     │
│          top_k=top_k,  # Default: 20                         │
│          output_fields=["entity_name", "entity_type",        │
│                        "description", "source_id"]           │
│      )                                                        │
│                                                               │
│      Example results:                                         │
│      [                                                        │
│          {                                                    │
│              "entity_name": "Rengoku Kyojuro",               │
│              "entity_type": "PERSON",                        │
│              "description": "Flame Pillar, passionate...",   │
│              "source_id": "doc-abc123"                       │
│          },                                                   │
│          {                                                    │
│              "entity_name": "Tomioka Giyu",                  │
│              "entity_type": "PERSON",                        │
│              "description": "Water Pillar, calm...",         │
│              "source_id": "doc-abc123"                       │
│          },                                                   │
│          ...                                                  │
│      ]                                                        │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2A.2 Get relationships involving these entities              │
│                                                               │
│      # For each entity, find relationships                   │
│      relationships = []                                       │
│      for entity in entities:                                 │
│          # Query graph storage                               │
│          rels = graph_storage.get_edges(                     │
│              node=entity["entity_name"]                      │
│          )                                                    │
│          relationships.extend(rels)                          │
│                                                               │
│      # Deduplicate                                           │
│      relationships = unique(relationships)                   │
│                                                               │
│      Example:                                                 │
│      [                                                        │
│          {                                                    │
│              "src_id": "Rengoku Kyojuro",                    │
│              "tgt_id": "Flame Breathing",                    │
│              "description": "Master of Flame Breathing",     │
│              "weight": 0.95                                   │
│          },                                                   │
│          ...                                                  │
│      ]                                                        │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2A.3 Retrieve related chunks                                 │
│                                                               │
│      # Embed low-level keywords                              │
│      ll_embedding = embed_model(ll_keywords)                 │
│                                                               │
│      # Vector search in chunks collection                    │
│      chunks = milvus.search(                                 │
│          collection="workspace_chunks",                      │
│          query_vector=ll_embedding,                          │
│          top_k=chunk_top_k,  # Default: 5                    │
│          output_fields=["content", "full_doc_id",            │
│                        "file_path", "start_page", "end_page"]│
│      )                                                        │
│                                                               │
│      # Optional: Rerank chunks if rerank_model enabled       │
│      if enable_rerank:                                       │
│          chunks = rerank_model.rerank(query, chunks)         │
│          chunks = chunks[:chunk_top_k]  # Keep top-k         │
└──────────────────────────────────────────────────────────────┘
```

---

### **Mode: GLOBAL (Relationship-Focused)**

```
┌─────────────────────────────────────────────────────────────┐
│              STEP 2B: GLOBAL MODE RETRIEVAL                  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2B.1 Search relationships directly                           │
│                                                               │
│      # Embed high-level keywords                             │
│      keyword_embedding = embed_model(hl_keywords)            │
│                                                               │
│      # Query graph storage for relationships                 │
│      # (Graph edges có embeddings của descriptions)          │
│      relationships = graph_storage.search_edges(             │
│          query_vector=keyword_embedding,                     │
│          top_k=top_k  # Default: 20                          │
│      )                                                        │
│                                                               │
│      # Relationships sorted by weight/relevance              │
│      Example:                                                 │
│      [                                                        │
│          {                                                    │
│              "src_id": "Demon Slayer Corps",                 │
│              "tgt_id": "Hashira",                            │
│              "description": "Hashira are top warriors...",   │
│              "weight": 0.98                                   │
│          },                                                   │
│          ...                                                  │
│      ]                                                        │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2B.2 Get entities involved in relationships                  │
│                                                               │
│      entities = []                                            │
│      for rel in relationships:                               │
│          # Get source entity                                 │
│          src = entities_vdb.get(rel["src_id"])               │
│          entities.append(src)                                │
│                                                               │
│          # Get target entity                                 │
│          tgt = entities_vdb.get(rel["tgt_id"])               │
│          entities.append(tgt)                                │
│                                                               │
│      # Deduplicate                                           │
│      entities = unique(entities)                             │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2B.3 Retrieve related chunks (same as local mode)           │
└──────────────────────────────────────────────────────────────┘
```

---

### **Phase 3: Reference Building**

```
┌─────────────────────────────────────────────────────────────┐
│              STEP 3: BUILD REFERENCES                        │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 3.1 Extract file_paths from all sources                      │
│                                                               │
│     all_file_paths = set()                                   │
│                                                               │
│     # From entities                                          │
│     for entity in entities:                                  │
│         all_file_paths.add(entity["file_path"])              │
│                                                               │
│     # From relationships                                     │
│     for rel in relationships:                                │
│         all_file_paths.add(rel["file_path"])                 │
│                                                               │
│     # From chunks                                            │
│     for chunk in chunks:                                     │
│         all_file_paths.add(chunk["file_path"])               │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 3.2 Generate reference IDs                                   │
│                                                               │
│     references = []                                           │
│     file_to_ref_id = {}                                       │
│                                                               │
│     for idx, file_path in enumerate(sorted(all_file_paths)): │
│         ref_id = str(idx + 1)  # "1", "2", "3", ...          │
│         file_to_ref_id[file_path] = ref_id                   │
│         references.append({                                   │
│             "reference_id": ref_id,                          │
│             "file_path": file_path                           │
│         })                                                    │
│                                                               │
│     Example:                                                  │
│     references = [                                            │
│         {"reference_id": "1", "file_path": "hashira.pdf"},   │
│         {"reference_id": "2", "file_path": "demons.pdf"}     │
│     ]                                                         │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 3.3 Inject reference_id into data items                     │
│                                                               │
│     # Add reference_id to entities                           │
│     for entity in entities:                                  │
│         entity["reference_id"] = \                           │
│             file_to_ref_id[entity["file_path"]]              │
│                                                               │
│     # Add reference_id to relationships                      │
│     for rel in relationships:                                │
│         rel["reference_id"] = \                              │
│             file_to_ref_id[rel["file_path"]]                 │
│                                                               │
│     # Add reference_id to chunks                             │
│     for chunk in chunks:                                     │
│         chunk["reference_id"] = \                            │
│             file_to_ref_id[chunk["file_path"]]               │
└──────────────────────────────────────────────────────────────┘
```

---

## 4.4 Response Structure

### **Complete Response Example**

```json
{
    "status": "success",
    "message": "Query executed successfully",
    "data": {
        "entities": [
            {
                "entity_name": "Rengoku Kyojuro",
                "entity_type": "PERSON",
                "description": "Flame Pillar, passionate and righteous warrior",
                "source_id": "chunk-abc123",
                "file_path": "hashira_details.pdf",
                "reference_id": "1"
            },
            {
                "entity_name": "Tomioka Giyu",
                "entity_type": "PERSON",
                "description": "Water Pillar, calm and introverted",
                "source_id": "chunk-def456",
                "file_path": "hashira_details.pdf",
                "reference_id": "1"
            }
        ],
        "relationships": [
            {
                "src_id": "Rengoku Kyojuro",
                "tgt_id": "Flame Breathing",
                "description": "Master of Flame Breathing techniques",
                "keywords": "master, breathing, techniques",
                "weight": 0.95,
                "source_id": "chunk-abc123",
                "file_path": "hashira_details.pdf",
                "reference_id": "1"
            },
            {
                "src_id": "Tomioka Giyu",
                "tgt_id": "Water Breathing",
                "description": "Practitioner of Water Breathing",
                "keywords": "practitioner, breathing, water",
                "weight": 0.92,
                "source_id": "chunk-def456",
                "file_path": "hashira_details.pdf",
                "reference_id": "1"
            }
        ],
        "chunks": [
            {
                "content": "CHI TIẾT CÁC ĐẠI TRỤ (HASHIRA) – KIMETSU NO YAIBA\nViêm Trụ – Rengoku Kyojuro\nTính cách: Nhiệt huyết, chính trực...",
                "file_path": "hashira_details.pdf",
                "chunk_id": "chunk-abc123",
                "reference_id": "1",
                "start_page": 1,
                "end_page": 2
            }
        ],
        "references": [
            {
                "reference_id": "1",
                "file_path": "hashira_details.pdf"
            }
        ]
    },
    "metadata": {
        "query_mode": "local",
        "keywords": {
            "high_level": ["Hashira", "Dai Tru", "Pillars"],
            "low_level": ["breathing", "techniques", "swordsmanship"]
        },
        "processing_info": {
            "total_entities_found": 20,
            "total_relations_found": 15,
            "entities_after_truncation": 2,
            "relations_after_truncation": 2,
            "final_chunks_count": 1
        }
    }
}
```

---

## 4.5 Use Cases Chi Tiết

### **Use Case 1: Graph Visualization**

```typescript
// Frontend: Render knowledge graph
const response = await fetch('/query/data', {
    method: 'POST',
    body: JSON.stringify({
        query: "Hashira relationships",
        mode: "local",
        top_k: 20
    })
});

const { data } = await response.json();

// Build graph nodes and edges
const nodes = data.entities.map(e => ({
    id: e.entity_name,
    label: e.entity_name,
    type: e.entity_type,
    description: e.description
}));

const edges = data.relationships.map(r => ({
    source: r.src_id,
    target: r.tgt_id,
    label: r.description,
    weight: r.weight
}));

// Render với D3.js, Cytoscape, etc.
renderGraph({ nodes, edges });
```

### **Use Case 2: Custom Citation System**

```typescript
// Display answer với inline citations
function formatChunkWithCitation(chunk) {
    return `
        <div class="chunk">
            <p>${chunk.content}</p>
            <cite>
                <a href="#ref-${chunk.reference_id}">
                    [${chunk.reference_id}]
                </a>
                Pages: ${chunk.start_page}-${chunk.end_page}
            </cite>
        </div>
    `;
}

// Display references section
function formatReferences(references) {
    return references.map(ref => `
        <li id="ref-${ref.reference_id}">
            [${ref.reference_id}] ${ref.file_path}
        </li>
    `).join('');
}
```

### **Use Case 3: Multi-Source Analysis**

```python
# Analyze coverage across multiple documents
response = query_data(
    query="Hashira training methods",
    mode="hybrid"
)

# Group by file
by_file = defaultdict(lambda: {
    "entities": [],
    "relationships": [],
    "chunks": []
})

for entity in response["data"]["entities"]:
    by_file[entity["file_path"]]["entities"].append(entity)

for rel in response["data"]["relationships"]:
    by_file[rel["file_path"]]["relationships"].append(rel)

for chunk in response["data"]["chunks"]:
    by_file[chunk["file_path"]]["chunks"].append(chunk)

# Analyze
for file, content in by_file.items():
    print(f"File: {file}")
    print(f"  Entities: {len(content['entities'])}")
    print(f"  Relationships: {len(content['relationships'])}")
    print(f"  Chunks: {len(content['chunks'])}")
```

---

## 4.6 Query Modes Comparison

| Mode | Focus | Top-K Meaning | Use When |
|------|-------|---------------|----------|
| **local** | Entities | Top-K entities | Query về specific entities, people, concepts |
| **global** | Relationships | Top-K relationships | Query về connections, patterns, broad analysis |
| **hybrid** | Both | Mixed | Query complex combining entities + relationships |
| **naive** | Chunks only | Top-K chunks | Simple vector search, no graph |
| **mix** | Auto-select | Dynamic | Let system choose best mode |
| **bypass** | Full context | All data | Use entire knowledge base (no filtering) |

**Example Queries per Mode:**

```python
# LOCAL: Entity-focused
query_data(
    query="Who is Rengoku Kyojuro?",
    mode="local"  # → Focus on Rengoku entity
)

# GLOBAL: Relationship-focused
query_data(
    query="How are Hashira connected to each other?",
    mode="global"  # → Focus on relationships between entities
)

# HYBRID: Complex query
query_data(
    query="What are the breathing techniques and who uses them?",
    mode="hybrid"  # → Both entities (techniques) and relationships (who uses)
)

# NAIVE: Simple search
query_data(
    query="Find chunks about flame pillar",
    mode="naive"  # → Just vector search chunks, no graph
)
```

---

## 4.7 Performance & Token Management

### **Token Budget System**

```python
# Unified token control (optional parameters)
query_data(
    query="...",
    max_entity_tokens=5000,     # Limit entity context
    max_relation_tokens=3000,   # Limit relationship context
    max_total_tokens=15000      # Hard cap for all context
)

# System will:
# 1. Retrieve all relevant data
# 2. Truncate entities to fit max_entity_tokens
# 3. Truncate relationships to fit max_relation_tokens
# 4. Ensure total < max_total_tokens
```

**Truncation Algorithm:**

```
┌─────────────────────────────────────────────────────────────┐
│              TOKEN TRUNCATION WORKFLOW                       │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 1. Count tokens for each component                           │
│                                                               │
│    entity_tokens = sum([count_tokens(e.description)          │
│                         for e in entities])                  │
│    relation_tokens = sum([count_tokens(r.description)        │
│                           for r in relationships])           │
│    chunk_tokens = sum([count_tokens(c.content)               │
│                        for c in chunks])                     │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 2. Apply individual limits                                   │
│                                                               │
│    if entity_tokens > max_entity_tokens:                     │
│        # Truncate lowest-scored entities                     │
│        entities = truncate_to_token_limit(                   │
│            entities, max_entity_tokens                       │
│        )                                                      │
│                                                               │
│    if relation_tokens > max_relation_tokens:                 │
│        relationships = truncate_to_token_limit(              │
│            relationships, max_relation_tokens                │
│        )                                                      │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ 3. Apply total limit                                         │
│                                                               │
│    total = entity_tokens + relation_tokens + chunk_tokens    │
│                                                               │
│    if total > max_total_tokens:                              │
│        # Proportional reduction                              │
│        ratio = max_total_tokens / total                      │
│        entity_budget = entity_tokens * ratio                 │
│        relation_budget = relation_tokens * ratio             │
│        chunk_budget = chunk_tokens * ratio                   │
│                                                               │
│        # Truncate each to budget                             │
│        entities = truncate(entities, entity_budget)          │
│        relationships = truncate(relationships, relation_budget)│
│        chunks = truncate(chunks, chunk_budget)               │
└──────────────────────────────────────────────────────────────┘
```

### **Performance Metrics**

| Components | Avg Latency | Notes |
|-----------|-------------|-------|
| Keyword extraction | ~0.5s | LLM call (skip if keywords provided) |
| Entity search | ~0.1s | Milvus vector search |
| Relationship search | ~0.2s | Graph traversal |
| Chunk search | ~0.1s | Milvus vector search |
| Reranking | ~0.3s | If enabled |
| Reference building | ~0.01s | Local processing |
| **Total (local mode)** | **~0.5-1s** | Without LLM keyword extraction |
| **Total (with LLM)** | **~1-1.5s** | With LLM keyword extraction |

---

# 5. API: POST /documents/replace_file_by_id

## 5.1 Mục Đích & Use Cases

**Mô tả:**  
Replace một file existing bằng cách xóa old version (với old file_id) và upload new version với **NEW file_id** (được generate bởi S3). Đây là compound operation kết hợp `delete_by_file_id` + `upload_from_url` nhưng với **hai file_id khác nhau**.

**Quan trọng:** Mỗi lần upload file lên S3, platform sẽ generate UUID mới làm file_id. Do đó API này thực chất là:
1. DELETE toàn bộ data liên quan đến old file_id
2. UPLOAD file mới với new file_id (extract từ S3 URL path)

**Use Cases:**
- **Document updates:** User sửa nội dung file và re-upload lên platform
- **S3 file versioning:** S3 tạo file mới với UUID khác, cần replace old data
- **Content refresh:** Replace stale data với current information from new S3 file
- **Error correction:** Fix file uploaded nhầm và upload file đúng (khác file_id)

---

## 5.2 Request Structure

**Endpoint:** `POST /documents/replace_file_by_id`

**Request Body:**
```json
{
    "old_file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "file_id": "c7a24784-9402-4bdd-98a7-d03a9a53e7cd",
    "file_url": "https://s3.../documents/report_v2.pdf",
    "table_name": "3479aed0-f28e-49a8-85af-75ebaf2da5d9",
    "run_background": false,
    "delete_llm_cache": true
}
```

**Parameters:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `old_file_id` | string | ✅ | - | **OLD file_id** to delete. Toàn bộ data liên quan sẽ bị xóa |
| `file_id` | string | ✅ | - | **NEW file_id** for upload (provided by frontend/S3) |
| `file_url` | string | ✅ | - | URL của new file to download and process |
| `table_name` | string | ✅ | - | Table name for categorizing new document |
| `run_background` | boolean | ❌ | true | Process in background (true) or wait for completion (false) |
| `delete_llm_cache` | boolean | ❌ | true | Delete LLM cache từ old file để ensure fresh processing |

**Quan trọng:**
- Frontend/S3 cung cấp `file_id` mới trong request body (không extract từ URL)
- `old_file_id` và `file_id` phải khác nhau
- API sẽ kiểm tra duplicate trước khi xóa old data

---

## 5.3 Workflow Chi Tiết

### **Three-Phase Operation**

```
┌─────────────────────────────────────────────────────────────┐
│              PHASE 0: CHECK DUPLICATE (CRITICAL!)           │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 0: Kiểm tra NEW file_id/file_url đã tồn tại chưa?  │
│                                                               │
│     Check 1: NEW file_id exists?                            │
│       doc_ids = await chunks_vdb.get_doc_ids_by_file_id(    │
│           file_id="c7a24784-9402-..."  # NEW file_id        │
│       )                                                       │
│       if doc_ids:                                            │
│           return HTTP 400: "NEW file_id already exists"     │
│                                                               │
│     Check 2: NEW file_url exists?                           │
│       doc_ids = await chunks_vdb.get_doc_ids_by_file_url(   │
│           file_url="https://s3.../report_v2.pdf"            │
│       )                                                       │
│       if doc_ids:                                            │
│           return HTTP 400: "file_url already exists"        │
│                                                               │
│     Result: No duplicate → Safe to proceed                 │
└──────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              PHASE 1: DELETE OLD VERSION                     │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 1: Call adelete_by_file_id()                           │
│                                                               │
│     result = await rag.adelete_by_file_id(                   │
│         file_id=old_file_id,  # Use OLD file_id from request│
│         delete_file=False,     # Don't delete physical file │
│         delete_llm_cache=delete_llm_cache  # Configurable   │
│     )                                                         │
│                                                               │
│     Operations:                                               │
│     ✓ Query Milvus for doc_ids with this old_file_id         │
│     ✓ Acquire pipeline lock                                  │
│     ✓ Delete each document:                                  │
│       - Remove chunks from Milvus                            │
│       - Remove entities from Milvus (if orphaned)            │
│       - Remove relationships from graph (if orphaned)        │
│       - Remove doc_status entries                            │
│       - Remove KV store entries                              │
│       - Delete LLM cache (if delete_llm_cache=true)          │
│     ✓ Release pipeline lock                                  │
│                                                               │
│     Possible outcomes:                                        │
│     - "success": All deleted                                 │
│     - "partial_success": Some failed                         │
│     - "not_found": No documents with this old_file_id        │
│     - "not_allowed": Pipeline busy                           │
│     - "failure": All failed                                  │
└──────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
   Deletion Success                    Deletion Failed
        │                                     │
        ▼                                     ▼
   Proceed to Phase 2              Return error immediately
```

**Critical Decision Point:**

```python
# After delete_by_file_id:
if delete_result["status"] not in ["success", "not_found"]:
    # Deletion failed or partial
    # → STOP, don't upload new file
    return {
        "status": "deletion_failed",
        "message": f"Old file deletion failed: {delete_result['message']}",
        "deletion_result": delete_result
    }

# Only proceed if:
# - status == "success" (deleted successfully)
# - status == "not_found" (no old file, proceed with upload)
```

---

### **Phase 2: Upload New Version**

```
┌─────────────────────────────────────────────────────────────┐
│              PHASE 2: UPLOAD NEW VERSION                     │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 2: Standard upload_from_url workflow                    │
│                                                               │
│     → Xem Section 1 để hiểu full workflow                    │
│                                                               │
│     Key operations:                                           │
│     1. Extract filename from URL                              │
│     2. Validate file type                                     │
│     3. Download file from file_url                            │
│     4. Extract text với page tracking                        │
│     5. Convert DOCX → PDF (if needed)                        │
│     6. Chunk with metadata injection:                        │
│        chunk["file_id"] = file_id  ← NEW file_id from body! │
│        chunk["table_name"] = table_name                      │
│        chunk["file_url"] = file_url                          │
│        chunk["start_page"] = X                               │
│        chunk["end_page"] = Y                                 │
│     7. LLM extraction (entities + relationships)             │
│     8. Store in Milvus + Graph + JSON                        │
│                                                               │
│     Result:                                                   │
│     - New chunks với NEW file_id (from request body)         │
│     - New entities/relationships extracted từ new content    │
│     - doc_status updated với new document (new file_id)      │
└──────────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
   Upload Success                      Upload Failed
        │                                     │
        ▼                                     ▼
   Return success                      Return error
   (file replaced)                     (old file deleted,
                                        new file failed)
```

---

## 5.4 Atomicity Concerns & Duplicate Protection

### **⚠️ CRITICAL: Operation is NOT Atomic**

```
┌─────────────────────────────────────────────────────────────┐
│              ATOMICITY PROBLEM                               │
└─────────────────────────────────────────────────────────────┘

Timeline:
  T0: Old file exists với old_file_id="cab4fa00-..."
  T1: ✓ STEP 0: Check duplicate - NEW file_id/file_url không tồn tại
  T2: ✓ STEP 1: delete_by_file_id("cab4fa00-...") SUCCESS
  T3: ✗ STEP 2: upload_from_url(file_id="c7a24784-...") FAILS
  T4: Result: Old data DELETED, new data NOT uploaded!

Data Loss Scenario:
  - Old data deleted
  - New data not uploaded
  - old_file_id data effectively missing
  - User queries return empty results for old_file_id
```

### **✅ Built-in Duplicate Protection (STEP 0)**

API đã có duplicate check tự động:

```python
# STEP 0: Check BEFORE deleting
existing_docs = await chunks_vdb.get_doc_ids_by_file_id(new_file_id)
if existing_docs:
    return HTTP 400: "NEW file_id already exists"

existing_docs_url = await chunks_vdb.get_doc_ids_by_file_url(new_file_url)
if existing_docs_url:
    return HTTP 400: "file_url already exists"

# Only proceed to STEP 1 (delete) if no duplicate
```

**Benefits:**
- ✅ Phát hiện duplicate TRƯỚC KHI xóa old data
- ✅ Bảo vệ old data khỏi bị xóa nhầm
- ✅ Frontend nhận HTTP 400 error rõ ràng

### **Additional Risk Mitigation Strategies**

**Strategy 1: Pre-validation (Already Built-in)**

```python
# API đã tự động validate!
# Không cần implement thêm ở client
response = await replace_file_by_id({
    "old_file_id": "cab4fa00-...",
    "file_id": "c7a24784-...",  # Will be checked for duplicate
    "file_url": "https://...",   # Will be checked for duplicate
    ...
})
    # Step 1: Validate new URL is accessible
    try:
        response = requests.head(new_url, timeout=5)
        if response.status_code != 200:
            return {"error": "New file URL not accessible"}
    except:
        return {"error": "Cannot reach new file URL"}
    
    # Step 2: Check file type supported
    filename = extract_filename(new_url)
    if not is_supported_file(filename):
        return {"error": "New file type not supported"}
    
    # Step 3: Now safe to proceed
    result = replace_file_by_id(file_id, new_url, ...)
    return result
```

**Strategy 2: Backup before delete**

```python
# Keep old data in backup before deletion
def replace_with_backup(file_id, new_url):
    # Step 1: Backup old data
    old_data = export_data_by_file_id(file_id)
    save_to_backup(file_id, old_data)
    
    # Step 2: Replace
    result = replace_file_by_id(file_id, new_url, ...)
    
    # Step 3: On failure, restore
    if result["status"] != "success":
        restore_from_backup(file_id, old_data)
        return {"status": "rollback", "message": "Restored old data"}
    
    return result
```

**Strategy 3: Use temporary file_id**

```python
# Upload với temporary ID first, then swap
def atomic_replace(file_id, new_url):
    temp_file_id = f"{file_id}_TEMP_{timestamp()}"
    
    # Step 1: Upload new file với temp ID
    upload_result = upload_from_url(
        file_id=temp_file_id,
        file_url=new_url,
        ...
    )
    
    if upload_result["status"] != "success":
        return {"error": "Upload failed, old file intact"}
    
    # Step 2: Delete old file
    delete_by_file_id(file_id)
    
    # Step 3: Rename temp ID → real ID
    update_file_id_in_chunks(temp_file_id, file_id)
    
    return {"status": "success"}
```

---

## 5.5 Response Examples

### **Success Response**

```json
{
    "status": "success",
    "message": "Successfully deleted old file_id='cab4fa00-e0bb...' and uploaded new file with file_id='c7a24784-9402...'",
    "old_file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "new_file_id": "c7a24784-9402-4bdd-98a7-d03a9a53e7cd",
    "deletion_result": {
        "status": "success",
        "total_docs": 1,
        "deleted_docs": ["doc-old-xyz"]
    },
    "upload_result": {
        "status": "success",
        "track_id": "upload_url_20260127_153000_abc"
    }
}
```

### **Deletion Failed Response**

```json
{
    "status": "deletion_failed",
    "message": "Failed to delete old file. Replace operation aborted.",
    "file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "deletion_result": {
        "status": "partial_success",
        "total_docs": 2,
        "deleted_docs": ["doc-1"],
        "failed_docs": [
            {"doc_id": "doc-2", "error": "Milvus connection timeout"}
        ]
    },
    "upload_result": null
}
```

### **Upload Failed Response**

```json
{
    "status": "upload_failed",
    "message": "Old file deleted but new file upload failed. DATA LOSS WARNING!",
    "file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "deletion_result": {
        "status": "success",
        "total_docs": 1,
        "deleted_docs": ["doc-old-xyz"]
    },
    "upload_result": {
        "status": "failure",
        "error": "Download failed: HTTP 404"
    }
}
```

---

## 5.6 Best Practices

### **✅ DO:**

**1. Track file_id changes in frontend:**
```typescript
// Store mapping between old and new file_ids
const result = await replaceFileById({
    file_id: oldFileId,  // "cab4fa00-..."
    file_url: newS3Url   // Contains "c7a24784-..."
});

if (result.status === 'success') {
    // Update frontend state với NEW file_id
    updateDocumentMapping({
        oldFileId: result.old_file_id,
        newFileId: result.new_file_id,
        timestamp: new Date()
    });
    
    // Update UI queries để use new file_id
    queryByFileId(result.new_file_id);
}
```

**2. Extract new file_id từ S3 URL before calling:**
```typescript
// Validate S3 URL and extract new file_id
function extractFileIdFromS3Url(url: string): string {
    // https://.../documents/c7a24784-9402-4bdd-98a7-d03a9a53e7cd.pdf
    const filename = url.split('/').pop();  // "c7a24784-...pdf"
    const fileId = filename.split('.')[0]; // "c7a24784-..."
    
    // Validate UUID format
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(fileId)) {
        throw new Error('Invalid file_id format in S3 URL');
    }
    
    return fileId;
}
```

**3. Use run_background=false cho critical operations:**
```json
{
    "file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
    "run_background": false
    // Wait để ensure success và get new_file_id immediately
}
```

**4. Always set delete_llm_cache=true:**
```json
{
    "delete_llm_cache": true
    // Ensure new file gets fresh LLM extraction
    // Old cached entities không contaminate new version
}
```

**5. Handle atomicity failure gracefully:**
```typescript
const result = await replaceFileById(oldFileId, newS3Url);

if (result.status === 'upload_failed') {
    // Old file deleted, new upload failed
    // → Re-upload old file from backup
    await uploadFromUrl({
        file_id: extractFileIdFromS3Url(oldBackupUrl),
        file_url: oldBackupUrl
    });
    
    alert('Replace failed, old version restored');
}
```

### **❌ DON'T:**

**1. Replace without checking file existence:**
```python
# ❌ Bad: May delete wrong file
replace_file_by_id(
    file_id="guess_file_id_123"  # Không chắc có tồn tại
)

# ✅ Good: Query first
docs = query_by_file_id("guess_file_id_123")
if not docs:
    return "File not found"
replace_file_by_id(...)
```

**2. Use same URL for replace:**
```python
# ❌ Bad: No change, waste resources
replace_file_by_id(
    file_id="FILE_001",
    file_url="https://.../same_file.pdf"  # Same as old URL!
)

# ✅ Good: Check if URL changed
if new_url == old_url:
    return "No change needed"
```

**3. Replace trong production without backup:**
```python
# ❌ Bad: Data loss risk
replace_file_by_id(file_id, new_url)

# ✅ Good: Backup first
old_data = export_by_file_id(file_id)
save_backup(file_id, old_data, timestamp)
replace_file_by_id(file_id, new_url)
```

---

## 5.7 Comparison với Manual Replace

### **Manual Approach:**

```python
# Step 1: Delete old file
await delete_by_file_id(
    file_id="cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",  # Old ID
    delete_llm_cache=True
)

# Wait...

# Step 2: Extract new file_id from S3 URL
new_file_id = extract_from_url(
    "https://s3.../c7a24784-9402-4bdd-98a7-d03a9a53e7cd.pdf"
)  # → "c7a24784-9402-4bdd-98a7-d03a9a53e7cd"

# Step 3: Upload new file với NEW file_id
await upload_from_url(
    file_id=new_file_id,  # NEW ID (khác old ID)
    file_url="https://s3.../c7a24784-9402-4bdd-98a7-d03a9a53e7cd.pdf",
    table_name="policies"
)
```

**Problems:**
- ❌ Three separate operations (delete, extract, upload)
- ❌ Gap period without data
- ❌ Manual error handling
- ❌ Client must track old_file_id → new_file_id mapping
- ❌ Risk of losing file_id tracking

### **replace_file_by_id Approach:**

```python
# Single call handles both delete + upload
result = await replace_file_by_id(
    file_id="cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",  # Old ID to delete
    file_url="https://s3.../c7a24784-9402-4bdd-98a7-d03a9a53e7cd.pdf",  # Contains NEW ID
    table_name="policies",
    delete_llm_cache=True
)

# Response includes both old and new file_ids
print(result.old_file_id)  # "cab4fa00-..."
print(result.new_file_id)  # "c7a24784-..."
```

**Benefits:**
- ✅ Single API call (delete + extract + upload)
- ✅ Automatic new_file_id extraction from S3 URL
- ✅ Consistent error handling
- ✅ Returns both old_file_id and new_file_id for tracking
- ✅ Shorter gap period
- ✅ Built-in validation

---

## 5.8 Request Body Structure Details

### **Field Descriptions**

**1. old_file_id (Required)**
```json
"old_file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2"
```
- OLD file_id cần xóa khỏi hệ thống
- Phải tồn tại trong database (có chunks với file_id này)
- Nếu không tồn tại → HTTP 404 error

**2. file_id (Required)**  
```json
"file_id": "c7a24784-9402-4bdd-98a7-d03a9a53e7cd"
```
- NEW file_id cho file mới upload
- Được generate bởi frontend/S3 platform
- **PHẢI KHÁC old_file_id**
- API sẽ check duplicate TRƯỚC KHI xóa old data

**3. file_url (Required)**
```json
"file_url": "https://s3.../documents/report_v2.pdf"
```
- URL để download file mới
- Phải là HTTP/HTTPS URL hợp lệ
- API sẽ check duplicate URL TRƯỚC KHI xóa old data

**4. table_name (Required)**
```json
"table_name": "3479aed0-f28e-49a8-85af-75ebaf2da5d9"
```
- Category/tenant identifier cho file mới
- Có thể giữ nguyên hoặc đổi so với old file
- Dùng để filter/query sau này

**5. delete_llm_cache (Optional, default=true)**
```json
"delete_llm_cache": true
```
- `true`: Xóa LLM extraction cache của old file → Fresh extraction cho new file
- `false`: Giữ lại cache (không khuyến khích khi replace)

**6. run_background (Optional, default=true)**
```json
"run_background": false
```
- `true`: Delete + Upload chạy background, API return ngay
- `false`: Đợi cả 2 operations hoàn tất mới return

### **Complete Request Example**

```json
{
  "old_file_id": "cab4fa00-e0bb-41bb-8b2b-45a5c46041d2",
  "file_id": "c7a24784-9402-4bdd-98a7-d03a9a53e7cd",
  "file_url": "https://svisor-dev.s3.ap-southeast-1.amazonaws.com/workspaces/e769c17b-7414-4f98-8482-f6c6710a50a4/99cedf93-6444-4192-864b-4b4c87dbd7e8/documents/c7a24784-9402-4bdd-98a7-d03a9a53e7cd.pdf",
  "table_name": "3479aed0-f28e-49a8-85af-75ebaf2da5d9",
  "run_background": false,
  "delete_llm_cache": true
}
```

### **Validation Rules**

| Field | Validation | Error Message |
|-------|------------|---------------|
| `old_file_id` | Must exist in database | HTTP 404: "No documents found with old_file_id" |
| `file_id` | Must NOT exist in database | HTTP 400: "NEW file_id already exists" |
| `file_url` | Must NOT exist in database | HTTP 400: "file_url already exists" |
| `file_url` | Must start with http/https | HTTP 400: "file_url must be HTTP/HTTPS" |
| All strings | Cannot be empty | HTTP 400: "Field cannot be empty" |

---

# 6. API Summary & Comparison Table

## 6.1 Complete API Overview

| API | Method | Purpose | Key Parameters | Atomicity |
|-----|--------|---------|----------------|-----------|
| **upload_from_url** | POST | Upload file từ URL với metadata | file_url, file_id, table_name, run_background | ✅ Atomic |
| **delete_by_table_name** | DELETE | Xóa tất cả docs trong table | table_name, delete_llm_cache | ⚠️ Sequential delete |
| **delete_by_file_id** | DELETE | Xóa docs theo file_id | file_id, delete_llm_cache | ⚠️ Sequential delete |
| **query/data** | POST | Query structured data | query, mode, top_k | N/A (read-only) |
| **replace_file_by_id** | POST | Delete old file_id + Upload với NEW file_id | old file_id, S3 URL (chứa new file_id) | ❌ NOT Atomic |

---

## 6.2 Performance Comparison

| API | Typical Latency | Bottleneck | Can Background? |
|-----|----------------|------------|-----------------|
| upload_from_url | 30-60s | LLM extraction, download | ✅ Yes |
| delete_by_table_name | 10s-hours | Sequential doc deletion | ✅ Yes |
| delete_by_file_id | 1-10s | Sequential doc deletion | ✅ Yes |
| query/data | 0.5-1.5s | Vector search, LLM keywords | ❌ No |
| replace_file_by_id | 30-70s | Delete + Upload combined | ✅ Yes |

---

## 6.3 Data Impact Matrix

| API | Chunks | Entities | Relationships | doc_status | LLM Cache |
|-----|--------|----------|---------------|------------|-----------|
| upload_from_url | ➕ Add | ➕ Add/Merge | ➕ Add/Merge | ➕ Add | ➕ Create |
| delete_by_table_name | ➖ Delete | ⚠️ Delete if orphaned | ⚠️ Delete if orphaned | ➖ Delete | ➖ Delete (optional) |
| delete_by_file_id | ➖ Delete | ⚠️ Delete if orphaned | ⚠️ Delete if orphaned | ➖ Delete | ➖ Delete (optional) |
| query/data | ✅ Read | ✅ Read | ✅ Read | ✅ Read | N/A |
| replace_file_by_id | ➖➕ Replace | ⚠️➕ Replace | ⚠️➕ Replace | ➖➕ Replace | ➖➕ Replace |

**Legend:**
- ➕ Add new data
- ➖ Delete data
- ⚠️ Conditional delete (only if không còn trong docs khác)
- ✅ Read-only

---

## 6.4 Use Case Recommendations

| Scenario | Recommended API | Why? |
|----------|----------------|------|
| Upload new document | upload_from_url | Direct upload với metadata |
| Update existing file | replace_file_by_id | Delete old data + Upload new data in one call |
| S3 file versioning | replace_file_by_id | S3 generates new UUID, need to replace old file_id |
| Remove category | delete_by_table_name | Bulk deletion by category |
| Remove specific file | delete_by_file_id | Precise deletion by ID |
| Display knowledge graph | query/data | Returns structured entities+relationships |
| Build citation system | query/data | Provides references list |
| Tenant cleanup | delete_by_table_name | Remove all tenant data |

---

**Document Version:** 2.0 (Complete)  
**Last Updated:** January 27, 2026  
**Coverage:** All 5 core APIs với full technical details  
**Language:** Vietnamese (technical terms in English)