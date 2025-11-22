#!/usr/bin/env python3
"""
Improved Crawl4AI Service for Railway
Properly handles JavaScript rendering and various scraping strategies
"""
import asyncio
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import uvicorn

try:
    from crawl4ai import AsyncWebCrawler
    from crawl4ai.extraction_strategy import JsonCssExtractionStrategy, LLMExtractionStrategy
except ImportError:
    print("❌ crawl4ai not installed")
    sys.exit(1)


app = FastAPI(
    title="Crawl4AI Service - Enhanced",
    description="Advanced web scraping with JS support",
    version="2.0.0"
)


class ScrapeRequest(BaseModel):
    """Enhanced request model for scraping"""
    url: HttpUrl
    # Basic options
    word_count_threshold: int = 10
    excluded_tags: list[str] = ["form", "nav", "footer"]
    remove_overlay_elements: bool = True
    
    # JavaScript handling
    wait_for: Optional[str] = None  # CSS selector or JS expression to wait for
    wait_timeout: int = 10000  # ms to wait for element
    js_code: Optional[str] = None  # JS to execute before scraping
    
    # Content options
    process_iframes: bool = False
    bypass_cache: bool = True
    
    # Strategy options
    strategy: str = "markdown"  # "markdown", "json_css", "llm"
    css_schema: Optional[Dict[str, Any]] = None  # For JSON CSS extraction
    
    # Page interaction
    actions: Optional[List[Dict[str, str]]] = None  # List of actions to perform
    

class CrawlRequest(BaseModel):
    """Request for multi-page crawling"""
    url: HttpUrl
    max_pages: int = 5
    max_depth: int = 1
    include_external: bool = False


class ScrapeResponse(BaseModel):
    """Response model for scraping"""
    success: bool
    url: str
    content_length: int
    title: Optional[str]
    markdown: Optional[str]
    extracted_content: Optional[Any] = None  # For structured extraction
    links: Optional[List[str]] = None
    error: Optional[str]
    timestamp: str
    strategy_used: str


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Crawl4AI Service - Enhanced",
        "version": "2.0.0",
        "endpoints": {
            "/scrape": "POST - Advanced scraping with JS support",
            "/crawl": "POST - Multi-page crawling",
            "/extract": "POST - Structured data extraction",
            "/health": "GET - Health check"
        },
        "features": [
            "JavaScript rendering",
            "Wait for elements",
            "Custom JS execution",
            "Page interactions",
            "Multiple extraction strategies",
            "Multi-page crawling"
        ]
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_url(request: ScrapeRequest):
    """
    Enhanced scraping with JavaScript support
    """
    print(f"🔍 Scraping: {request.url}")
    print(f"   Strategy: {request.strategy}")
    if request.wait_for:
        print(f"   Waiting for: {request.wait_for}")
    if request.js_code:
        print(f"   Executing JS: {request.js_code[:50]}...")
    
    try:
        async with AsyncWebCrawler(
            verbose=True,
            headless=True,
            browser_type="chromium"
        ) as crawler:
            
            # Build crawl parameters
            crawl_params = {
                "url": str(request.url),
                "bypass_cache": request.bypass_cache,
                "word_count_threshold": request.word_count_threshold,
                "excluded_tags": request.excluded_tags,
                "remove_overlay_elements": request.remove_overlay_elements,
                "process_iframes": request.process_iframes,
            }
            
            # Add JavaScript options if provided
            if request.wait_for:
                crawl_params["wait_for"] = request.wait_for
                crawl_params["timeout"] = request.wait_timeout
            
            if request.js_code:
                crawl_params["js_code"] = request.js_code
            
            # Handle page actions (click, scroll, etc.)
            if request.actions:
                for action in request.actions:
                    action_type = action.get("type")
                    if action_type == "click":
                        crawl_params["js_code"] = f"""
                        document.querySelector('{action.get("selector")}')?.click();
                        """
                    elif action_type == "scroll":
                        crawl_params["js_code"] = "window.scrollTo(0, document.body.scrollHeight);"
                    elif action_type == "wait":
                        await asyncio.sleep(action.get("duration", 1))
            
            # Apply extraction strategy
            if request.strategy == "json_css" and request.css_schema:
                extraction_strategy = JsonCssExtractionStrategy(
                    schema=request.css_schema,
                    verbose=True
                )
                crawl_params["extraction_strategy"] = extraction_strategy
            
            # Execute crawl
            result = await crawler.arun(**crawl_params)
            
            # Extract links from the page
            links = []
            if result.links:
                links = [link.get("href", "") for link in result.links[:100]]
            
            response = ScrapeResponse(
                success=result.success,
                url=result.url,
                content_length=len(result.markdown) if result.markdown else 0,
                title=result.metadata.get("title") if hasattr(result, 'metadata') else None,
                markdown=result.markdown,
                extracted_content=result.extracted_content if hasattr(result, 'extracted_content') else None,
                links=links,
                error=None,
                timestamp=datetime.now().isoformat(),
                strategy_used=request.strategy
            )
            
            print(f"✅ Success: {response.content_length:,} chars, {len(links)} links")
            return response
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return ScrapeResponse(
            success=False,
            url=str(request.url),
            content_length=0,
            title=None,
            markdown=None,
            error=str(e),
            timestamp=datetime.now().isoformat(),
            strategy_used=request.strategy
        )


@app.post("/crawl")
async def crawl_site(request: CrawlRequest):
    """
    Multi-page crawling (similar to Firecrawl's crawl endpoint)
    """
    print(f"🕷️ Crawling: {request.url} (max {request.max_pages} pages)")
    
    pages = []
    visited_urls = set()
    to_visit = [str(request.url)]
    
    async with AsyncWebCrawler(verbose=True, headless=True) as crawler:
        while to_visit and len(pages) < request.max_pages:
            current_url = to_visit.pop(0)
            
            if current_url in visited_urls:
                continue
            
            visited_urls.add(current_url)
            
            try:
                result = await crawler.arun(
                    url=current_url,
                    bypass_cache=True,
                    word_count_threshold=10
                )
                
                if result.success:
                    # Add page to results
                    pages.append({
                        "url": current_url,
                        "title": result.metadata.get("title") if hasattr(result, 'metadata') else "",
                        "content": result.markdown[:10000] if result.markdown else "",
                        "links": []
                    })
                    
                    # Extract and queue new links
                    if result.links and len(pages) < request.max_pages:
                        for link in result.links[:20]:  # Limit links per page
                            href = link.get("href", "")
                            if href and href.startswith("http"):
                                if not request.include_external:
                                    # Check if same domain
                                    from urllib.parse import urlparse
                                    if urlparse(href).netloc == urlparse(str(request.url)).netloc:
                                        if href not in visited_urls:
                                            to_visit.append(href)
                                else:
                                    if href not in visited_urls:
                                        to_visit.append(href)
                
            except Exception as e:
                print(f"Error crawling {current_url}: {e}")
                continue
    
    return {
        "success": len(pages) > 0,
        "pages": pages,
        "total_pages": len(pages),
        "timestamp": datetime.now().isoformat()
    }


@app.post("/extract")
async def extract_structured_data(
    url: HttpUrl,
    schema: Dict[str, Any],
    strategy: str = "json_css"
):
    """
    Extract structured data using CSS selectors (like Firecrawl's extract)
    """
    print(f"📊 Extracting structured data from: {url}")
    
    try:
        async with AsyncWebCrawler(verbose=True, headless=True) as crawler:
            
            if strategy == "json_css":
                extraction_strategy = JsonCssExtractionStrategy(
                    schema=schema,
                    verbose=True
                )
            else:
                return {"success": False, "error": "Unsupported strategy"}
            
            result = await crawler.arun(
                url=str(url),
                bypass_cache=True,
                extraction_strategy=extraction_strategy
            )
            
            if result.success and result.extracted_content:
                return {
                    "success": True,
                    "data": json.loads(result.extracted_content),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "success": False,
                    "error": "Extraction failed",
                    "timestamp": datetime.now().isoformat()
                }
                
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/test/jobs")
async def test_job_scraping():
    """
    Test job board scraping with proper JS handling
    """
    print("🧪 Testing job board scraping...")
    
    # Test Ashby job board
    request = ScrapeRequest(
        url="https://jobs.ashbyhq.com/claylabs/",
        wait_for="window.__appData",  # Wait for Ashby data
        js_code="return window.__appData",  # Extract the data
        strategy="markdown"
    )
    
    result = await scrape_url(request)
    
    # Try to extract job count
    if result.markdown:
        import re
        job_patterns = re.findall(r'(?:Engineer|Developer|Manager|Designer)', result.markdown)
        print(f"Found approximately {len(job_patterns)} job-related terms")
    
    return result


if __name__ == "__main__":
    print("🚀 Starting Enhanced Crawl4AI Service...")
    print("   Port: 8000")
    print("   Features: JS rendering, wait conditions, multi-page crawling")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )