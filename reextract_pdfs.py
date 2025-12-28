"""
Enhanced PDF Re-extraction Script
Uses pymupdf with OCR and better table extraction for CSRD reports.
"""
import logging
from pathlib import Path
import json
import re
import pymupdf
import pymupdf4llm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Directories
RAW_DIR = Path("e:/AA_IMpact/csrd_extractor/data/raw")
PROCESSED_DIR = Path("e:/AA_IMpact/csrd_extractor/data/processed")

# Banks to process
BANKS = [
    {"name": "aib", "pdf": "aib_2024.pdf", "year": 2024},
    {"name": "bbva", "pdf": "bbva_2024.pdf", "year": 2024},
    {"name": "bpce", "pdf": "bpce_2024.pdf", "year": 2024},
]


def extract_text_with_layout(pdf_path: Path) -> list:
    """
    Extract text from PDF with enhanced layout preservation.
    Uses pymupdf4llm for better table structure.
    """
    logger.info(f"Extracting with layout: {pdf_path}")
    
    # Use pymupdf4llm with table settings
    try:
        md_chunks = pymupdf4llm.to_markdown(
            str(pdf_path),
            page_chunks=True,
            write_images=False,
            show_progress=True,
            # Enhanced table detection
            table_strategy="lines_strict",  # Better table detection
        )
        return md_chunks
    except Exception as e:
        logger.error(f"pymupdf4llm failed: {e}")
        return []


def extract_tables_direct(pdf_path: Path) -> dict:
    """
    Directly extract tables using pymupdf's find_tables() method.
    Returns dict: page_num -> list of tables
    """
    logger.info(f"Extracting tables directly from: {pdf_path}")
    tables_by_page = {}
    
    doc = pymupdf.open(pdf_path)
    
    for page_num, page in enumerate(doc, 1):
        try:
            tables = page.find_tables()
            if tables.tables:
                page_tables = []
                for table in tables:
                    # Convert table to markdown format
                    df_data = table.extract()
                    if df_data and len(df_data) > 0:
                        md_table = convert_to_markdown_table(df_data)
                        if md_table:
                            page_tables.append(md_table)
                
                if page_tables:
                    tables_by_page[page_num] = page_tables
                    logger.info(f"  Page {page_num}: Found {len(page_tables)} tables")
        except Exception as e:
            logger.debug(f"  Page {page_num}: Table extraction error: {e}")
    
    doc.close()
    return tables_by_page


def convert_to_markdown_table(table_data: list) -> str:
    """Convert extracted table data to Markdown format."""
    if not table_data or len(table_data) < 2:
        return ""
    
    # Clean cells
    clean_data = []
    for row in table_data:
        clean_row = []
        for cell in row:
            if cell is None:
                clean_row.append("")
            else:
                # Clean cell content
                text = str(cell).strip()
                text = text.replace("|", "/")  # Escape pipes
                text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
                clean_row.append(text)
        clean_data.append(clean_row)
    
    # Build markdown table
    lines = []
    
    # Header row
    header = clean_data[0]
    lines.append("| " + " | ".join(header) + " |")
    
    # Separator
    lines.append("| " + " | ".join(["---"] * len(header)) + " |")
    
    # Data rows
    for row in clean_data[1:]:
        # Pad row if needed
        while len(row) < len(header):
            row.append("")
        lines.append("| " + " | ".join(row[:len(header)]) + " |")
    
    return "\n".join(lines)


def merge_content_with_tables(md_chunks: list, tables_by_page: dict) -> list:
    """
    Merge the markdown chunks with directly extracted tables.
    Appends tables at the end of each page's content.
    """
    for chunk in md_chunks:
        page_num = chunk.get("metadata", {}).get("page", 0) + 1
        
        if page_num in tables_by_page:
            # Append extracted tables to the page content
            extra_content = "\n\n**[Extracted Tables]**\n\n"
            for i, table in enumerate(tables_by_page[page_num], 1):
                extra_content += f"\n**Table {i}:**\n{table}\n"
            
            chunk["text"] = chunk.get("text", "") + extra_content
    
    return md_chunks


def extract_numbers_from_page(pdf_path: Path, page_num: int) -> list:
    """
    Extract all numbers from a specific page using raw text extraction.
    Useful for finding data that might be in graphics.
    """
    doc = pymupdf.open(pdf_path)
    page = doc[page_num - 1]  # 0-indexed
    
    # Get raw text
    text = page.get_text("text")
    
    # Find all numbers (with optional comma/dot formatting)
    numbers = re.findall(r'[\d,]+\.?\d*', text)
    
    doc.close()
    return numbers


def save_processed(bank_name: str, md_chunks: list, pdf_filename: str, year: int):
    """Save processed content to disk."""
    output_dir = PROCESSED_DIR / bank_name.lower()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    pages_dir = output_dir / "pages"
    pages_dir.mkdir(exist_ok=True)
    
    # Save pages
    full_text_parts = []
    pages_with_tables = []
    
    for chunk in md_chunks:
        page_num = chunk.get("metadata", {}).get("page", 0) + 1
        content = chunk.get("text", "")
        
        # Save page
        page_path = pages_dir / f"page_{page_num:04d}.md"
        with open(page_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        full_text_parts.append(content)
        
        # Check for tables
        if "|" in content and "---" in content:
            pages_with_tables.append(page_num)
    
    # Save full text
    full_text_path = output_dir / "full_text.md"
    with open(full_text_path, "w", encoding="utf-8") as f:
        f.write("\n\n---PAGE BREAK---\n\n".join(full_text_parts))
    
    # Save metadata
    metadata = {
        "filename": pdf_filename,
        "bank_name": bank_name.upper(),
        "report_year": year,
        "total_pages": len(md_chunks),
        "pages_with_tables": pages_with_tables,
        "extraction_method": "enhanced_with_tables"
    }
    metadata_path = output_dir / "metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Saved {len(md_chunks)} pages for {bank_name}")
    logger.info(f"  Pages with tables: {len(pages_with_tables)}")


def process_bank(bank_info: dict):
    """Process a single bank's PDF."""
    bank_name = bank_info["name"]
    pdf_filename = bank_info["pdf"]
    year = bank_info["year"]
    
    pdf_path = RAW_DIR / pdf_filename
    
    if not pdf_path.exists():
        logger.error(f"PDF not found: {pdf_path}")
        return
    
    logger.info(f"\n{'='*60}")
    logger.info(f"PROCESSING: {bank_name.upper()}")
    logger.info(f"{'='*60}")
    
    # Step 1: Extract with layout
    md_chunks = extract_text_with_layout(pdf_path)
    
    if not md_chunks:
        logger.error(f"Failed to extract content from {pdf_filename}")
        return
    
    # Step 2: Extract tables directly
    tables_by_page = extract_tables_direct(pdf_path)
    
    # Step 3: Merge content
    if tables_by_page:
        logger.info(f"Merging {len(tables_by_page)} pages with extra tables")
        md_chunks = merge_content_with_tables(md_chunks, tables_by_page)
    
    # Step 4: Save
    save_processed(bank_name, md_chunks, pdf_filename, year)
    
    logger.info(f"Completed: {bank_name}")


def main():
    """Main entry point."""
    logger.info("="*60)
    logger.info("ENHANCED PDF RE-EXTRACTION")
    logger.info("="*60)
    
    for bank in BANKS:
        try:
            process_bank(bank)
        except Exception as e:
            logger.error(f"Failed to process {bank['name']}: {e}")
            import traceback
            traceback.print_exc()
    
    logger.info("\n" + "="*60)
    logger.info("RE-EXTRACTION COMPLETE")
    logger.info("="*60)


if __name__ == "__main__":
    main()
