"""
Custom embedding function for self-hosted embedding APIs.

This module provides a flexible embedding function that works with custom/self-hosted
embedding services that don't follow the OpenAI API format.

Supported API format:
- Request: POST to {base_url}{endpoint}
- Body: {"texts": ["text1", "text2", ...]}
- Response: {"embeddings": [[...], [...], ...], "model": "...", "dimension": ...}

Configuration via environment variables:
- EMBEDDING_BINDING=custom
- EMBEDDING_BINDING_HOST=http://your-host:port
- EMBEDDING_CUSTOM_ENDPOINT=/embed (default: /embed)
- EMBEDDING_DIM=768
- EMBEDDING_MODEL=your-model-name (optional, for logging)
"""

import os
from typing import Any

import aiohttp
import numpy as np

from lightrag.utils import logger
from lightrag.llm.openai import wrap_embedding_func_with_attrs


@wrap_embedding_func_with_attrs(
    embedding_dim=768, max_token_size=8192, model_name="custom-embedding-model"
)
async def custom_embed(
    texts: list[str],
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    endpoint: str | None = None,
    embedding_dim: int | None = None,
    max_token_size: int | None = None,
    timeout: int | None = None,
    token_tracker: Any | None = None,
    **kwargs,
) -> np.ndarray:
    """Generate embeddings using a custom/self-hosted embedding API.

    This function is designed to work with embedding APIs that use a different
    format than OpenAI's standard API.

    Expected API format:
        Request:
            POST {base_url}{endpoint}
            Content-Type: application/json
            Body: {"texts": ["text1", "text2", ...]}

        Response:
            {
                "model": "model-name",
                "dimension": 768,
                "embeddings": [[0.1, 0.2, ...], [0.3, 0.4, ...]]
            }

    Args:
        texts: List of texts to embed.
        model: Model name (optional, mainly for logging).
        base_url: Base URL of the embedding API (e.g., http://localhost:8080).
        api_key: API key for authentication (optional, sent as Bearer token if provided).
        endpoint: API endpoint path (default: /embed).
        embedding_dim: Expected embedding dimension (for validation).
        max_token_size: Maximum tokens per text (not used for custom APIs).
        timeout: Request timeout in seconds.
        token_tracker: Token usage tracker (not applicable for custom APIs).

    Returns:
        A numpy array of embeddings, shape (len(texts), embedding_dim).

    Raises:
        ValueError: If required parameters are missing or response format is invalid.
        aiohttp.ClientError: If there's a connection error with the API.
    """
    # Get configuration from environment variables with fallbacks
    base_url = (
        base_url
        or os.getenv("EMBEDDING_BINDING_HOST")
        or os.getenv("EMBEDDING_CUSTOM_HOST")
    )
    api_key = (
        api_key
        or os.getenv("EMBEDDING_BINDING_API_KEY")
        or os.getenv("EMBEDDING_CUSTOM_API_KEY")
    )
    endpoint = (
        endpoint
        or os.getenv("EMBEDDING_CUSTOM_ENDPOINT")
        or "/embed"
    )
    timeout = timeout or int(os.getenv("EMBEDDING_TIMEOUT", "180"))
    model = model or os.getenv("EMBEDDING_MODEL", "custom-embedding-model")

    if not base_url:
        raise ValueError(
            "base_url is required for custom embedding. "
            "Set EMBEDDING_BINDING_HOST or EMBEDDING_CUSTOM_HOST environment variable."
        )

    # Ensure endpoint starts with /
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint

    # Build full URL
    # Remove trailing slash from base_url if present
    base_url = base_url.rstrip("/")
    full_url = f"{base_url}{endpoint}"

    # Prepare headers
    headers = {
        "Content-Type": "application/json",
    }
    if api_key and api_key != "dummy_key":
        headers["Authorization"] = f"Bearer {api_key}"

    # Prepare request body
    request_body = {
        "texts": texts,
    }

    logger.debug(f"Custom embedding request to {full_url} with {len(texts)} texts")
    logger.debug(f"Custom embedding request body preview: texts[0]={texts[0][:100] if texts and texts[0] else 'empty'}...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                full_url,
                json=request_body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Custom embedding API error: status={response.status}, response={error_text[:500]}")
                    logger.error(f"Request was: url={full_url}, texts_count={len(texts)}, first_text_len={len(texts[0]) if texts else 0}")
                    raise ValueError(
                        f"Custom embedding API error {response.status}: {error_text}"
                    )

                result = await response.json()

                # Extract embeddings from response
                # Support multiple response formats
                embeddings = None

                # Format 1: {"embeddings": [[...], [...]]}
                if "embeddings" in result:
                    embeddings = result["embeddings"]
                # Format 2: {"data": [{"embedding": [...]}, ...]} (OpenAI-like)
                elif "data" in result:
                    embeddings = [item["embedding"] for item in result["data"]]
                # Format 3: Direct array [[...], [...]]
                elif isinstance(result, list):
                    embeddings = result
                else:
                    raise ValueError(
                        f"Invalid response format from custom embedding API. "
                        f"Expected 'embeddings' key or 'data' array. Got: {list(result.keys()) if isinstance(result, dict) else type(result)}"
                    )

                if not embeddings:
                    raise ValueError("Empty embeddings returned from custom embedding API")

                # Convert to numpy array
                embeddings_array = np.array(embeddings, dtype=np.float32)

                # Validate dimensions if specified
                if embedding_dim is not None and embeddings_array.shape[1] != embedding_dim:
                    logger.warning(
                        f"Embedding dimension mismatch: expected {embedding_dim}, "
                        f"got {embeddings_array.shape[1]}"
                    )

                # Validate count matches input
                if len(embeddings_array) != len(texts):
                    raise ValueError(
                        f"Embedding count mismatch: expected {len(texts)}, "
                        f"got {len(embeddings_array)}"
                    )

                logger.debug(
                    f"Custom embedding completed: {len(texts)} texts -> "
                    f"shape {embeddings_array.shape}"
                )

                return embeddings_array

    except aiohttp.ClientError as e:
        raise ValueError(f"Connection error to custom embedding API at {full_url}: {e}")
    except Exception as e:
        if "Custom embedding" in str(e) or "Connection error" in str(e):
            raise
        raise ValueError(f"Error calling custom embedding API: {e}")
