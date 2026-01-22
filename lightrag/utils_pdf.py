"""
PDF utilities for extracting text with page number tracking.

This module provides functions to:
- Extract text from PDF files with page position information
- Assign page numbers to text chunks based on their content position
"""

from pathlib import Path
from typing import List, Dict, Tuple, Union
from io import BytesIO


def extract_text_with_pages(
    pdf_source: Union[Path, bytes, BytesIO], password: str = None
) -> Tuple[str, List[Dict[str, int]]]:
    """Extract text from PDF with page number tracking.

    Args:
        pdf_source: Path to PDF file, bytes content, or BytesIO object
        password: Optional password for encrypted PDFs

    Returns:
        Tuple containing:
        - full_text (str): Complete concatenated text from all pages
        - pages_info (List[Dict]): List of dicts with page metadata:
            {
                'page': int,           # Page number (1-indexed)
                'char_start': int,     # Start position in full_text (0-indexed)
                'char_end': int        # End position in full_text (exclusive)
            }

    Raises:
        Exception: If PDF is encrypted and password is incorrect or missing
    """
    from pypdf import PdfReader

    # Handle different input types
    if isinstance(pdf_source, Path):
        reader = PdfReader(str(pdf_source))
    elif isinstance(pdf_source, bytes):
        reader = PdfReader(BytesIO(pdf_source))
    elif isinstance(pdf_source, BytesIO):
        reader = PdfReader(pdf_source)
    else:
        raise ValueError(
            f"pdf_source must be Path, bytes, or BytesIO, got {type(pdf_source)}"
        )

    # Handle encryption
    if reader.is_encrypted:
        if not password:
            raise Exception("PDF is encrypted but no password provided")
        decrypt_result = reader.decrypt(password)
        if decrypt_result == 0:
            raise Exception("Incorrect PDF password")

    # Extract text and track page positions
    full_text = ""
    pages_info = []
    current_position = 0

    for page_num, page in enumerate(reader.pages, start=1):
        page_text = page.extract_text()
        # Add newline after each page for separation
        page_text_with_newline = page_text + "\n"

        char_start = current_position
        char_end = current_position + len(page_text_with_newline)

        pages_info.append(
            {"page": page_num, "char_start": char_start, "char_end": char_end}
        )

        full_text += page_text_with_newline
        current_position = char_end

    return full_text, pages_info


def assign_page_to_chunk(
    chunk_text: str, full_text: str, pages_info: List[Dict[str, int]]
) -> Tuple[int, int]:
    """Assign start_page and end_page to a text chunk.

    This function finds where the chunk appears in the full text and determines
    which pages it spans across.

    Args:
        chunk_text: The text content of the chunk
        full_text: The complete document text
        pages_info: List of page information dicts from extract_text_with_pages()

    Returns:
        Tuple of (start_page, end_page):
        - start_page: Page number where chunk starts (1-indexed)
        - end_page: Page number where chunk ends (1-indexed)
        - Returns (0, 0) if chunk is not found in full_text

    Examples:
        >>> # Chunk within single page
        >>> assign_page_to_chunk("text on page 1", full_text, pages_info)
        (1, 1)

        >>> # Chunk spanning multiple pages
        >>> assign_page_to_chunk("text from page 1 to 3", full_text, pages_info)
        (1, 3)
    """
    if not pages_info:
        return 0, 0

    # Find chunk position in full text
    chunk_start = full_text.find(chunk_text)
    if chunk_start == -1:
        # Chunk not found - try fuzzy matching with first 100 chars
        chunk_preview = chunk_text[:100]
        chunk_start = full_text.find(chunk_preview)
        if chunk_start == -1:
            # Still not found, return default
            return 0, 0
        # Use preview length for fuzzy match
        chunk_end = chunk_start + len(chunk_preview)
    else:
        chunk_end = chunk_start + len(chunk_text)

    # Find start page
    start_page = 0
    for page_info in pages_info:
        if page_info["char_start"] <= chunk_start < page_info["char_end"]:
            start_page = page_info["page"]
            break

    # If chunk_start is exactly at page boundary (char_end), assign to next page
    if start_page == 0:
        for page_info in pages_info:
            if chunk_start == page_info["char_end"] and page_info["page"] < len(
                pages_info
            ):
                start_page = page_info["page"] + 1
                break

    # Find end page
    end_page = 0
    for page_info in pages_info:
        if page_info["char_start"] < chunk_end <= page_info["char_end"]:
            end_page = page_info["page"]
            break

    # If chunk_end is exactly at page boundary (char_end), use that page
    if end_page == 0:
        for page_info in pages_info:
            if chunk_end == page_info["char_end"]:
                end_page = page_info["page"]
                break

    # Fallback: if still not found, use last page
    if start_page == 0:
        start_page = pages_info[0]["page"]
    if end_page == 0:
        end_page = pages_info[-1]["page"]

    # Ensure end_page >= start_page
    if end_page < start_page:
        end_page = start_page

    return start_page, end_page


def extract_text_simple(
    pdf_source: Union[Path, bytes, BytesIO], password: str = None
) -> str:
    """Extract text from PDF without page tracking (legacy/simple mode).

    This is a convenience wrapper that returns only the text content,
    matching the original behavior of _extract_pdf_pypdf.

    Args:
        pdf_source: Path to PDF file, bytes content, or BytesIO object
        password: Optional password for encrypted PDFs

    Returns:
        str: Extracted text content from all pages
    """
    full_text, _ = extract_text_with_pages(pdf_source, password)
    return full_text
