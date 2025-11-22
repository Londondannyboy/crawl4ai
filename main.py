#!/usr/bin/env python3
"""
Crawl4AI Service for Railway - Simplified Working Version
Focus on getting basic functionality working first
"""
from __future__ import annotations
import asyncio
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import re

from fastapi import FastAPI
from pydantic import BaseModel, Field
import uvicorn

# Import Crawl4AI
try:
    from crawl4ai import AsyncWebCrawler
    print("✅ Crawl4AI imported successfully")
except ImportError as e:
    print(f"❌ Failed to import crawl4ai: {e}")
    sys.exit(1)

# Check if Playwright is available
try:
    import playwright
    print("✅ Playwright imported successfully")
except ImportError as e:
    print(f"❌ Failed to import playwright: {e}")
    sys.exit(1)


app = FastAPI(
    title="Crawl4AI Service",
    description="Web scraping service",
    version="4.0.0"
)


class ScrapeRequest(BaseModel):
    """Simple scrape request"""
    url: str
    word_count_threshold: int = 10
    excluded_tags: List[str] = Field(default_factory=list)
    remove_overlay_elements: bool = True


class CrawlRequest(BaseModel):
    """Simple crawl request"""
    url: str
    max_pages: int = 1


@app.get("/")
async def root():
    return {
        "service": "Crawl4AI Service",
        "version": "4.0.0",
        "status": "running",
        "endpoints": {
            "/scrape": "POST - Scrape a URL",
            "/crawl": "POST - Crawl pages (Quest worker compatibility)",
            "/health": "GET - Health check"
        }
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/scrape")
async def scrape_url(request: ScrapeRequest):
    """
    Simple scraping endpoint
    For Ashby boards, we'll extract from the HTML source
    """
    print(f"🔍 Scraping: {request.url}")
    
    try:
        # Create a basic crawler
        async with AsyncWebCrawler(verbose=True) as crawler:
            # Just get the page
            result = await crawler.arun(
                url=request.url,
                bypass_cache=True,
                word_count_threshold=request.word_count_threshold
            )
            
            # Get content
            markdown = ""
            
            # Special handling for Ashby boards
            if "ashbyhq.com" in request.url and hasattr(result, 'html'):
                print("   Detected Ashby board - extracting jobs from HTML")
                html = result.html
                
                # Look for __appData in the HTML
                match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                if match:
                    try:
                        app_data = json.loads(match.group(1))
                        jobs = app_data.get('jobBoard', {}).get('jobPostings', [])
                        
                        # Build markdown content
                        lines = ["# Job Openings\n"]
                        job_count = 0
                        
                        for job in jobs:
                            if job.get('isListed'):
                                job_count += 1
                                lines.append(f"\n## {job.get('title', 'Untitled')}")
                                lines.append(f"**Department:** {job.get('departmentName', 'N/A')}")
                                lines.append(f"**Location:** {job.get('locationName', 'N/A')}")
                                lines.append(f"**Type:** {job.get('employmentType', 'N/A')}")
                                lines.append(f"**Workplace:** {job.get('workplaceType', 'N/A')}")
                                lines.append(f"**Posted:** {job.get('publishedDate', 'N/A')}")
                        
                        markdown = "\n".join(lines)
                        print(f"   ✅ Extracted {job_count} jobs from Ashby board")
                    except Exception as e:
                        print(f"   ❌ Failed to parse Ashby data: {e}")
                        markdown = result.markdown if hasattr(result, 'markdown') else ""
                else:
                    print("   No __appData found, using regular markdown")
                    markdown = result.markdown if hasattr(result, 'markdown') else ""
            else:
                # For non-Ashby sites, just use the markdown
                markdown = result.markdown if hasattr(result, 'markdown') else ""
            
            return {
                "success": bool(markdown),
                "url": request.url,
                "content_length": len(markdown),
                "title": result.metadata.get("title", "") if hasattr(result, 'metadata') else "",
                "markdown": markdown,
                "error": None,
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "url": request.url,
            "content_length": 0,
            "title": None,
            "markdown": None,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.post("/crawl")
async def crawl_site(request: CrawlRequest):
    """
    Quest worker compatibility endpoint
    Returns the expected format
    """
    print(f"🕷️ Crawling: {request.url} (Quest worker endpoint)")
    
    try:
        async with AsyncWebCrawler(verbose=True) as crawler:
            result = await crawler.arun(
                url=request.url,
                bypass_cache=True,
                word_count_threshold=10
            )
            
            # Get content - special handling for Ashby
            content = ""
            title = ""
            
            if "ashbyhq.com" in request.url and hasattr(result, 'html'):
                print("   Processing Ashby board for /crawl")
                html = result.html
                match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                
                if match:
                    try:
                        app_data = json.loads(match.group(1))
                        jobs = app_data.get('jobBoard', {}).get('jobPostings', [])
                        company = app_data.get('organization', {}).get('name', 'Company')
                        
                        content_lines = [f"# {company} Job Openings\n"]
                        for job in jobs:
                            if job.get('isListed'):
                                content_lines.append(f"{job.get('title', 'N/A')} - {job.get('departmentName', 'N/A')} - {job.get('locationName', 'N/A')}")
                        
                        content = "\n".join(content_lines)
                        title = f"{company} Jobs"
                        print(f"   ✅ Extracted {len(jobs)} jobs")
                    except:
                        content = result.markdown if hasattr(result, 'markdown') else ""
                        title = result.metadata.get("title", "") if hasattr(result, 'metadata') else ""
                else:
                    content = result.markdown if hasattr(result, 'markdown') else ""
                    title = result.metadata.get("title", "") if hasattr(result, 'metadata') else ""
            else:
                content = result.markdown if hasattr(result, 'markdown') else ""
                title = result.metadata.get("title", "") if hasattr(result, 'metadata') else ""
            
            # Return in Quest worker expected format
            return {
                "success": bool(content),
                "pages": [{
                    "url": request.url,
                    "title": title,
                    "content": content[:10000] if content else "",
                    "links": []
                }] if content else [],
                "links": [],
                "crawler": "crawl4ai_service"
            }
            
    except Exception as e:
        print(f"❌ Error in /crawl: {e}")
        return {
            "success": False,
            "pages": [],
            "links": [],
            "crawler": "crawl4ai_service",
            "error": str(e)
        }


if __name__ == "__main__":
    print("="*60)
    print("🚀 Starting Crawl4AI Service v4.0.0")
    print("="*60)
    print("Features:")
    print("  ✅ Ashby job board support (extracts from HTML)")
    print("  ✅ Quest worker /crawl endpoint")
    print("  ✅ General web scraping")
    print("="*60)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )