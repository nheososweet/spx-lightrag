"""
Embedding API Proxy - Convert OpenAI format to custom embedding format
Chuyển đổi từ format OpenAI sang format endpoint embedding của bạn
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import httpx
import asyncio

app = FastAPI(title="Embedding Proxy")

# Config endpoint thực tế của bạn
ACTUAL_EMBEDDING_HOST = "http://1.34.114.64:62712"


class EmbeddingRequest(BaseModel):
    """OpenAI format request"""
    input: List[str] | str
    model: Optional[str] = None
    encoding_format: Optional[str] = None


class EmbeddingResponse(BaseModel):
    """OpenAI format response"""
    object: str = "list"
    data: List[dict]
    model: str
    usage: dict


@app.post("/embeddings", response_model=EmbeddingResponse)
async def embeddings(request: EmbeddingRequest):
    """
    Proxy endpoint that converts OpenAI format to custom format
    Nhận request OpenAI format, forward sang endpoint của bạn
    """
    
    # Convert input to list if it's a string
    texts = request.input if isinstance(request.input, list) else [request.input]
    
    if not texts:
        raise HTTPException(status_code=400, detail="Input cannot be empty")
    
    embeddings_list = []
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Gửi từng text đến endpoint của bạn
            for text in texts:
                response = await client.post(
                    f"{ACTUAL_EMBEDDING_HOST}/embeddings",
                    json={"text": text}  # Format của bạn
                )
                
                if response.status_code != 200:
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Embedding API error: {response.text}"
                    )
                
                data = response.json()
                embedding = data.get("embedding", [])
                
                embeddings_list.append({
                    "object": "embedding",
                    "embedding": embedding,
                    "index": len(embeddings_list)
                })
    
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to connect to embedding service: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing embeddings: {str(e)}"
        )
    
    return EmbeddingResponse(
        data=embeddings_list,
        model=request.model or "custom-embedding-model",
        usage={
            "prompt_tokens": sum(len(t.split()) for t in texts),
            "total_tokens": sum(len(t.split()) for t in texts)
        }
    )


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Embedding Proxy on http://localhost:8000")
    print(f"📡 Forwarding to: {ACTUAL_EMBEDDING_HOST}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
