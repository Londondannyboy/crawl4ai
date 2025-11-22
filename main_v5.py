#!/usr/bin/env python3
"""
Crawl4AI Service v5.0 - Multi-page crawling with URL discovery
Adds support for crawling multiple pages and discovering URLs
"""
from __future__ import annotations
import asyncio
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import re
from urllib.parse import urljoin, urlparse

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn

# Import Crawl4AI
try:
    from crawl4ai import AsyncWebCrawler
    from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
    print("✅ Crawl4AI imported successfully")
except ImportError as e:
    print(f"❌ Failed to import crawl4ai: {e}")
    sys.exit(1)


app = FastAPI(
    title="Crawl4AI Service",
    description="Web scraping service with multi-page support",
    version="5.0.0"
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


class DiscoverRequest(BaseModel):
    """URL discovery request"""
    url: str
    pattern: Optional[str] = None  # e.g., "*/jobs/*" or "*/careers/*"
    max_urls: int = 100
    include_external: bool = False


class CrawlManyRequest(BaseModel):
    """Multi-URL crawling request"""
    urls: List[str]
    parallel: int = 5  # Number of parallel crawlers
    delay_between: float = 0.5  # Delay between requests in seconds


@app.get("/")
async def root():
    return {
        "service": "Crawl4AI Service",
        "version": "5.0.0",
        "status": "running",
        "endpoints": {
            "/scrape": "POST - Scrape a single URL",
            "/crawl": "POST - Crawl pages (Quest worker compatibility)",
            "/discover": "POST - Discover URLs from a page",
            "/crawl-many": "POST - Crawl multiple URLs in parallel",
            "/health": "GET - Health check"
        }
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/discover")
async def discover_urls(request: DiscoverRequest):
    """
    Discover URLs from a page
    For job boards, this will find all individual job URLs
    """
    print(f"🔍 Discovering URLs from: {request.url}")
    
    try:
        async with AsyncWebCrawler(verbose=True) as crawler:
            # Crawl the main page
            result = await crawler.arun(
                url=request.url,
                bypass_cache=True
            )
            
            discovered_urls = []
            
            # Special handling for Ashby boards
            if "ashbyhq.com" in request.url and hasattr(result, 'html'):
                print("   Detected Ashby board - extracting job URLs")
                html = result.html
                
                # Extract job data from __appData
                match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                if match:
                    try:
                        app_data = json.loads(match.group(1))
                        jobs = app_data.get('jobBoard', {}).get('jobPostings', [])
                        
                        # Build job URLs
                        base_url = request.url.rstrip('/')
                        for job in jobs:
                            if job.get('isListed'):
                                job_id = job.get('id')
                                if job_id:
                                    job_url = f"{base_url}/{job_id}"
                                    discovered_urls.append({
                                        'url': job_url,
                                        'title': job.get('title', ''),
                                        'type': 'job_posting'
                                    })
                        
                        print(f"   ✅ Found {len(discovered_urls)} job URLs")
                    except Exception as e:
                        print(f"   ❌ Failed to parse Ashby data: {e}")
            
            # Generic link extraction for other sites
            if not discovered_urls and hasattr(result, 'links'):
                print("   Using generic link extraction")
                base_domain = urlparse(request.url).netloc
                
                for link in result.links.get('internal', []):
                    # Apply pattern filter if provided
                    if request.pattern:
                        import fnmatch
                        if not fnmatch.fnmatch(link, request.pattern):
                            continue
                    
                    # Check if it's likely a job URL
                    if any(indicator in link.lower() for indicator in ['job', 'career', 'position', 'opening']):
                        discovered_urls.append({
                            'url': link,
                            'title': '',
                            'type': 'potential_job'
                        })
                    
                    if len(discovered_urls) >= request.max_urls:
                        break
                
                # Include external links if requested
                if request.include_external:
                    for link in result.links.get('external', [])[:10]:
                        discovered_urls.append({
                            'url': link,
                            'title': '',
                            'type': 'external'
                        })
            
            return {
                "success": True,
                "url": request.url,
                "urls_discovered": len(discovered_urls),
                "urls": discovered_urls[:request.max_urls],
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        print(f"❌ Error in discovery: {e}")
        return {
            "success": False,
            "url": request.url,
            "urls_discovered": 0,
            "urls": [],
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.post("/crawl-many")
async def crawl_many_urls(request: CrawlManyRequest):
    """
    Crawl multiple URLs efficiently
    Uses Crawl4AI's arun_many() for parallel processing
    """
    print(f"🕷️ Crawling {len(request.urls)} URLs with {request.parallel} parallel crawlers")
    
    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided")
    
    if len(request.urls) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 URLs per request")
    
    results = []
    
    try:
        # Create browser config for parallel crawling
        browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080
        )
        
        async with AsyncWebCrawler(
            config=browser_config,
            verbose=True
        ) as crawler:
            # Process URLs in batches based on parallel limit
            for i in range(0, len(request.urls), request.parallel):
                batch_urls = request.urls[i:i + request.parallel]
                print(f"   Processing batch: {i//request.parallel + 1} ({len(batch_urls)} URLs)")
                
                # Use arun_many for parallel crawling
                batch_results = await crawler.arun_many(
                    urls=batch_urls,
                    bypass_cache=True
                )
                
                # Process results
                for url, result in zip(batch_urls, batch_results):
                    if result.success:
                        # Extract relevant content
                        content = ""
                        
                        # Special handling for Ashby job pages
                        if "ashbyhq.com" in url and hasattr(result, 'html'):
                            html = result.html
                            # Extract job details from the page
                            # This would parse the individual job page
                            content = result.markdown if hasattr(result, 'markdown') else ""
                        else:
                            content = result.markdown if hasattr(result, 'markdown') else ""
                        
                        results.append({
                            'url': url,
                            'success': True,
                            'title': result.metadata.get("title", "") if hasattr(result, 'metadata') else "",
                            'content_length': len(content),
                            'content': content[:5000]  # Limit content size
                        })
                    else:
                        results.append({
                            'url': url,
                            'success': False,
                            'error': str(result.error) if hasattr(result, 'error') else "Unknown error"
                        })
                
                # Add delay between batches
                if i + request.parallel < len(request.urls):
                    await asyncio.sleep(request.delay_between)
        
        successful = sum(1 for r in results if r['success'])
        
        return {
            "success": True,
            "total_urls": len(request.urls),
            "successful": successful,
            "failed": len(request.urls) - successful,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error in crawl-many: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "total_urls": len(request.urls),
            "successful": 0,
            "failed": len(request.urls),
            "results": [],
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.post("/scrape")
async def scrape_url(request: ScrapeRequest):
    """
    Simple scraping endpoint (kept for compatibility)
    """
    print(f"🔍 Scraping: {request.url}")
    
    try:
        async with AsyncWebCrawler(verbose=True) as crawler:
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
                
                match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                if match:
                    try:
                        app_data = json.loads(match.group(1))
                        jobs = app_data.get('jobBoard', {}).get('jobPostings', [])
                        
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
                    markdown = result.markdown if hasattr(result, 'markdown') else ""
            else:
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
    Quest worker compatibility endpoint (kept for backward compatibility)
    """
    print(f"🕷️ Crawling: {request.url} (Quest worker endpoint)")
    
    try:
        async with AsyncWebCrawler(verbose=True) as crawler:
            result = await crawler.arun(
                url=request.url,
                bypass_cache=True,
                word_count_threshold=10
            )
            
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
    print("🚀 Starting Crawl4AI Service v5.0.0")
    print("="*60)
    print("New Features:")
    print("  ✅ URL discovery with /discover endpoint")
    print("  ✅ Multi-page crawling with /crawl-many endpoint")
    print("  ✅ Parallel processing support")
    print("  ✅ Pattern-based URL filtering")
    print("Existing Features:")
    print("  ✅ Ashby job board support")
    print("  ✅ Quest worker compatibility")
    print("  ✅ General web scraping")
    print("="*60)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )