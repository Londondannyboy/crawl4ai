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
    from crawl4ai import AsyncWebCrawler, CacheMode
    from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
    print("✅ Crawl4AI imported successfully")

    # Try to import advanced features (may not be available in all versions)
    try:
        from crawl4ai.content_filter_strategy import BM25ContentFilter, PruningContentFilter
        from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
        HAS_CONTENT_FILTERS = True
        print("✅ Content filters available (BM25, Pruning)")
    except ImportError:
        HAS_CONTENT_FILTERS = False
        print("⚠️ Content filters not available in this version")

except ImportError as e:
    print(f"❌ Failed to import crawl4ai: {e}")
    sys.exit(1)


app = FastAPI(
    title="Crawl4AI Service",
    description="Web scraping service with multi-page support",
    version="8.0.0"
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


class ArticleResearchRequest(BaseModel):
    """Optimized article research request with topic-based filtering"""
    urls: List[str]
    topic: str  # Topic query for BM25 relevance filtering
    keywords: List[str] = Field(default_factory=list)  # Additional keywords
    parallel: int = 5
    min_word_count: int = 50  # Minimum words per content block
    use_pruning: bool = True  # Remove low-value content
    use_bm25: bool = True  # Filter by topic relevance


@app.get("/")
async def root():
    return {
        "service": "Crawl4AI Service",
        "version": "8.0.0",
        "status": "running",
        "features": {
            "content_filters": HAS_CONTENT_FILTERS,
            "bm25_filtering": HAS_CONTENT_FILTERS,
            "pruning": HAS_CONTENT_FILTERS
        },
        "endpoints": {
            "/scrape": "POST - Scrape a single URL",
            "/crawl": "POST - Crawl pages (Quest worker compatibility)",
            "/discover": "POST - Discover URLs from a page",
            "/crawl-many": "POST - Crawl multiple URLs in parallel",
            "/crawl-articles": "POST - Optimized article research with BM25 filtering",
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
                config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
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
                    config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
                )
                
                # Process results
                for url, result in zip(batch_urls, batch_results):
                    if result.success:
                        # Extract relevant content
                        content = ""
                        title = ""
                        
                        # Special handling for Ashby job detail pages
                        if "ashbyhq.com" in url and "/" in url.split("ashbyhq.com/")[-1]:
                            # This is a job detail page
                            if hasattr(result, 'html'):
                                html = result.html
                                # Extract job data from __appData
                                match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                                if match:
                                    try:
                                        app_data = json.loads(match.group(1))
                                        if 'posting' in app_data:
                                            posting = app_data['posting']
                                            title = posting.get('title', '')
                                            
                                            # Build structured content
                                            content_parts = []
                                            content_parts.append(f"# {title}\n")
                                            content_parts.append(f"**Company:** {app_data.get('organization', {}).get('name', '')}")
                                            content_parts.append(f"**Department:** {posting.get('departmentName', '')}")
                                            content_parts.append(f"**Location:** {posting.get('locationName', '')}")
                                            content_parts.append(f"**Type:** {posting.get('employmentType', '')}")
                                            content_parts.append(f"**Workplace:** {posting.get('workplaceType', '')}\n")
                                            
                                            # Add description
                                            if posting.get('descriptionPlainText'):
                                                content_parts.append("## Description\n")
                                                content_parts.append(posting['descriptionPlainText'])
                                            
                                            # Add compensation if available
                                            if posting.get('scrapeableCompensationSalarySummary'):
                                                content_parts.append(f"\n**Salary:** {posting['scrapeableCompensationSalarySummary']}")
                                            
                                            content = "\n".join(content_parts)
                                            print(f"   ✅ Extracted Ashby job details: {title}")
                                    except Exception as e:
                                        print(f"   ❌ Failed to parse Ashby data: {e}")
                                        content = result.markdown if hasattr(result, 'markdown') else ""
                                else:
                                    content = result.markdown if hasattr(result, 'markdown') else ""
                            else:
                                content = result.markdown if hasattr(result, 'markdown') else ""
                        else:
                            content = result.markdown if hasattr(result, 'markdown') else ""
                            title = result.metadata.get("title", "") if hasattr(result, 'metadata') else ""
                        
                        results.append({
                            'url': url,
                            'success': True,
                            'title': title,
                            'content_length': len(content),
                            'content': content[:10000]  # Increased limit for job descriptions
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


@app.post("/crawl-articles")
async def crawl_articles(request: ArticleResearchRequest):
    """
    Optimized article research endpoint with BM25 filtering.

    Uses:
    - BM25ContentFilter: Filters content by relevance to topic
    - PruningContentFilter: Removes low-value content (sidebars, ads)
    - Higher word_count_threshold: Skips thin content
    - remove_overlay_elements: Removes popups/modals
    """
    print(f"📰 Article research: {len(request.urls)} URLs, topic: '{request.topic}'")

    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided")

    if len(request.urls) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 URLs per request")

    results = []

    try:
        # Build optimized config for article research
        browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080
        )

        # Build CrawlerRunConfig with optimizations
        run_config_kwargs = {
            "word_count_threshold": request.min_word_count,
            "exclude_external_links": True,
            "remove_overlay_elements": True,
            "process_iframes": True,
            "cache_mode": CacheMode.BYPASS
        }

        # Add content filters if available
        if HAS_CONTENT_FILTERS and (request.use_bm25 or request.use_pruning):
            # Build query from topic + keywords
            query = request.topic
            if request.keywords:
                query += " " + " ".join(request.keywords)

            if request.use_bm25:
                # BM25 filters content by relevance to topic
                bm25_filter = BM25ContentFilter(
                    user_query=query,
                    bm25_threshold=1.0  # Relevance threshold
                )
                md_generator = DefaultMarkdownGenerator(content_filter=bm25_filter)
                run_config_kwargs["markdown_generator"] = md_generator
                print(f"   Using BM25 filter with query: '{query}'")
            elif request.use_pruning:
                # Pruning removes low-value content
                prune_filter = PruningContentFilter(
                    threshold=0.4,
                    threshold_type="fixed",
                    min_word_threshold=request.min_word_count
                )
                md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)
                run_config_kwargs["markdown_generator"] = md_generator
                print("   Using Pruning filter")

        run_config = CrawlerRunConfig(**run_config_kwargs)

        async with AsyncWebCrawler(
            config=browser_config,
            verbose=True
        ) as crawler:
            # Process URLs in batches
            for i in range(0, len(request.urls), request.parallel):
                batch_urls = request.urls[i:i + request.parallel]
                print(f"   Batch {i//request.parallel + 1}: {len(batch_urls)} URLs")

                # Use arun_many for parallel crawling
                batch_results = await crawler.arun_many(
                    urls=batch_urls,
                    config=run_config
                )

                for url, result in zip(batch_urls, batch_results):
                    if result.success:
                        # Get filtered/fit markdown if available, else raw
                        content = ""
                        if hasattr(result, 'markdown_v2') and result.markdown_v2:
                            # fit_markdown is the BM25/pruned version
                            if hasattr(result.markdown_v2, 'fit_markdown'):
                                content = result.markdown_v2.fit_markdown or ""
                            if not content and hasattr(result.markdown_v2, 'raw_markdown'):
                                content = result.markdown_v2.raw_markdown or ""
                        if not content:
                            content = result.markdown if hasattr(result, 'markdown') else ""

                        title = result.metadata.get("title", "") if hasattr(result, 'metadata') and result.metadata else ""

                        results.append({
                            'url': url,
                            'success': True,
                            'title': title,
                            'content_length': len(content),
                            'content': content[:15000],  # 15k chars for articles
                            'filtered': HAS_CONTENT_FILTERS and (request.use_bm25 or request.use_pruning)
                        })
                    else:
                        results.append({
                            'url': url,
                            'success': False,
                            'error': str(result.error) if hasattr(result, 'error') else "Unknown error"
                        })

                # Small delay between batches
                if i + request.parallel < len(request.urls):
                    await asyncio.sleep(0.3)

        successful = sum(1 for r in results if r.get('success'))

        return {
            "success": True,
            "total_urls": len(request.urls),
            "successful": successful,
            "failed": len(request.urls) - successful,
            "topic": request.topic,
            "filters_used": {
                "bm25": request.use_bm25 and HAS_CONTENT_FILTERS,
                "pruning": request.use_pruning and HAS_CONTENT_FILTERS
            },
            "results": results,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        print(f"❌ Error in crawl-articles: {e}")
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
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    word_count_threshold=request.word_count_threshold
                )
            )
            
            # Get content
            markdown = ""
            
            # Special handling for Ashby pages
            if "ashbyhq.com" in request.url and hasattr(result, 'html'):
                html = result.html
                match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                
                if match:
                    try:
                        app_data = json.loads(match.group(1))
                        
                        # Check if it's a job detail page
                        if 'posting' in app_data:
                            print("   Detected Ashby job detail page")
                            posting = app_data['posting']
                            
                            # Build markdown from job posting data
                            lines = [f"# {posting.get('title', 'Job Opening')}\n"]
                            lines.append(f"**Company:** {app_data.get('organization', {}).get('name', '')}")
                            lines.append(f"**Department:** {posting.get('departmentName', '')}")
                            lines.append(f"**Location:** {posting.get('locationName', '')}")
                            lines.append(f"**Type:** {posting.get('employmentType', '')}")
                            lines.append(f"**Workplace:** {posting.get('workplaceType', '')}\n")
                            
                            if posting.get('descriptionPlainText'):
                                lines.append("## Description\n")
                                lines.append(posting['descriptionPlainText'])
                            
                            if posting.get('scrapeableCompensationSalarySummary'):
                                lines.append(f"\n**Salary:** {posting['scrapeableCompensationSalarySummary']}")
                            
                            markdown = "\n".join(lines)
                            print(f"   ✅ Extracted full job details: {posting.get('title')}")
                            
                        # Or it's a job board listing page
                        elif 'jobBoard' in app_data:
                            print("   Detected Ashby job board listing")
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
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    word_count_threshold=10
                )
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
    print("🚀 Starting Crawl4AI Service v8.0.0")
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