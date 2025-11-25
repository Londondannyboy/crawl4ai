#!/usr/bin/env python3
"""
Crawl4AI Service v9.0 - Advanced crawling with adaptive resource management
Features:
- MemoryAdaptiveDispatcher for auto-throttling under load
- URL-specific configs for different content types
- Streaming results for progressive processing
- BM25/Pruning content filters
"""
from __future__ import annotations
import asyncio
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any, AsyncGenerator
import json
import re
from urllib.parse import urljoin, urlparse
import fnmatch

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
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

    # Try to import MemoryAdaptiveDispatcher for resource management
    try:
        from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher, SemaphoreDispatcher
        HAS_ADAPTIVE_DISPATCHER = True
        print("✅ MemoryAdaptiveDispatcher available")
    except ImportError:
        HAS_ADAPTIVE_DISPATCHER = False
        print("⚠️ MemoryAdaptiveDispatcher not available, using semaphore fallback")

except ImportError as e:
    print(f"❌ Failed to import crawl4ai: {e}")
    sys.exit(1)


# URL pattern configs for different site types
URL_CONFIGS = {
    "news": {
        "patterns": ["*news*", "*article*", "*blog*", "*/post/*"],
        "word_count_threshold": 100,
        "page_timeout": 30000,
        "description": "News/article sites - stricter word count"
    },
    "government": {
        "patterns": ["*.gov", "*.gov.*", "*.govt.*"],
        "word_count_threshold": 50,
        "page_timeout": 60000,
        "description": "Government sites - longer timeout"
    },
    "corporate": {
        "patterns": ["*/about*", "*/company*", "*/team*", "*/careers*"],
        "word_count_threshold": 30,
        "page_timeout": 30000,
        "description": "Corporate pages - standard settings"
    },
    "default": {
        "patterns": ["*"],
        "word_count_threshold": 10,
        "page_timeout": 30000,
        "description": "Default fallback"
    }
}


app = FastAPI(
    title="Crawl4AI Service",
    description="Web scraping service with adaptive resource management, URL-specific configs, and streaming",
    version="9.0.0"
)


def get_config_for_url(url: str) -> Dict[str, Any]:
    """
    Get the appropriate config based on URL pattern matching.
    Returns config dict with word_count_threshold and page_timeout.
    """
    url_lower = url.lower()

    # Check each config type in order of specificity
    for config_type in ["news", "government", "corporate"]:
        config = URL_CONFIGS[config_type]
        for pattern in config["patterns"]:
            if fnmatch.fnmatch(url_lower, pattern):
                return config

    return URL_CONFIGS["default"]


def create_dispatcher(max_concurrent: int = 10, memory_threshold: float = 70.0):
    """
    Create the best available dispatcher for resource management.
    Uses MemoryAdaptiveDispatcher if available, falls back to semaphore.
    """
    if HAS_ADAPTIVE_DISPATCHER:
        return MemoryAdaptiveDispatcher(
            memory_threshold_percent=memory_threshold,
            check_interval=1.0,
            max_session_permit=max_concurrent,
            memory_wait_timeout=300  # 5 min timeout if memory stays high
        )
    else:
        # Fallback to simple semaphore-based limiting
        return None  # Will use asyncio.Semaphore in the crawl functions


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
    """Multi-URL crawling request with adaptive resource management"""
    urls: List[str]
    parallel: int = 10  # Max parallel crawlers (adaptive dispatcher may reduce)
    delay_between: float = 0.5  # Delay between batches
    use_adaptive: bool = True  # Use MemoryAdaptiveDispatcher if available
    memory_threshold: float = 70.0  # Memory % threshold for throttling
    use_url_configs: bool = True  # Apply URL-specific configs
    stream: bool = False  # Stream results as they complete


class ArticleResearchRequest(BaseModel):
    """Optimized article research request with topic-based filtering"""
    urls: List[str]
    topic: str  # Topic query for BM25 relevance filtering
    keywords: List[str] = Field(default_factory=list)  # Additional keywords
    parallel: int = 10  # Max parallel (adaptive dispatcher manages actual)
    min_word_count: int = 50  # Minimum words per content block
    use_pruning: bool = True  # Remove low-value content
    use_bm25: bool = True  # Filter by topic relevance
    use_adaptive: bool = True  # Use MemoryAdaptiveDispatcher
    memory_threshold: float = 70.0  # Memory % threshold for throttling
    use_url_configs: bool = True  # Apply URL-specific configs
    stream: bool = False  # Stream results as NDJSON


@app.get("/")
async def root():
    return {
        "service": "Crawl4AI Service",
        "version": "9.0.0",
        "status": "running",
        "features": {
            "content_filters": HAS_CONTENT_FILTERS,
            "bm25_filtering": HAS_CONTENT_FILTERS,
            "pruning": HAS_CONTENT_FILTERS,
            "memory_adaptive_dispatcher": HAS_ADAPTIVE_DISPATCHER,
            "url_specific_configs": True,
            "streaming": True
        },
        "url_configs": URL_CONFIGS,
        "endpoints": {
            "/scrape": "POST - Scrape a single URL",
            "/crawl": "POST - Crawl pages (Quest worker compatibility)",
            "/discover": "POST - Discover URLs from a page",
            "/crawl-many": "POST - Crawl multiple URLs with adaptive resource management",
            "/crawl-articles": "POST - Optimized article research with BM25 filtering",
            "/crawl-stream": "POST - Stream results as NDJSON (progressive processing)",
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


def extract_ashby_content(url: str, result) -> tuple:
    """Extract content from Ashby job pages. Returns (content, title)."""
    content = ""
    title = ""

    if "ashbyhq.com" not in url:
        return content, title

    if not hasattr(result, 'html'):
        return content, title

    html = result.html
    match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)

    if not match:
        return content, title

    try:
        app_data = json.loads(match.group(1))
        if 'posting' in app_data:
            posting = app_data['posting']
            title = posting.get('title', '')

            content_parts = [f"# {title}\n"]
            content_parts.append(f"**Company:** {app_data.get('organization', {}).get('name', '')}")
            content_parts.append(f"**Department:** {posting.get('departmentName', '')}")
            content_parts.append(f"**Location:** {posting.get('locationName', '')}")
            content_parts.append(f"**Type:** {posting.get('employmentType', '')}")
            content_parts.append(f"**Workplace:** {posting.get('workplaceType', '')}\n")

            if posting.get('descriptionPlainText'):
                content_parts.append("## Description\n")
                content_parts.append(posting['descriptionPlainText'])

            if posting.get('scrapeableCompensationSalarySummary'):
                content_parts.append(f"\n**Salary:** {posting['scrapeableCompensationSalarySummary']}")

            content = "\n".join(content_parts)
            print(f"   ✅ Extracted Ashby job details: {title}")
    except Exception as e:
        print(f"   ❌ Failed to parse Ashby data: {e}")

    return content, title


@app.post("/crawl-many")
async def crawl_many_urls(request: CrawlManyRequest):
    """
    Crawl multiple URLs with adaptive resource management.

    Features:
    - MemoryAdaptiveDispatcher: Auto-throttles when memory exceeds threshold
    - URL-specific configs: Different settings for news/gov/corporate sites
    - Streaming option: Return results as NDJSON stream
    """
    print(f"🕷️ Crawling {len(request.urls)} URLs")
    print(f"   Adaptive: {request.use_adaptive}, Memory threshold: {request.memory_threshold}%")
    print(f"   URL configs: {request.use_url_configs}, Stream: {request.stream}")

    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided")

    if len(request.urls) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 URLs per request")

    # Handle streaming response
    if request.stream:
        return StreamingResponse(
            crawl_many_stream(request),
            media_type="application/x-ndjson"
        )

    results = []

    try:
        browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080
        )

        # Create dispatcher if adaptive mode enabled
        dispatcher = None
        if request.use_adaptive and HAS_ADAPTIVE_DISPATCHER:
            dispatcher = create_dispatcher(request.parallel, request.memory_threshold)
            print(f"   Using MemoryAdaptiveDispatcher (max {request.parallel}, threshold {request.memory_threshold}%)")
        else:
            print(f"   Using semaphore-based batching (batch size {request.parallel})")

        async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
            if dispatcher and HAS_ADAPTIVE_DISPATCHER:
                # Use dispatcher for all URLs at once - it manages concurrency
                all_results = await crawler.arun_many(
                    urls=request.urls,
                    config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS),
                    dispatcher=dispatcher
                )

                for url, result in zip(request.urls, all_results):
                    results.append(process_crawl_result(url, result, request.use_url_configs))
            else:
                # Fallback: batch processing with semaphore
                for i in range(0, len(request.urls), request.parallel):
                    batch_urls = request.urls[i:i + request.parallel]
                    print(f"   Batch {i//request.parallel + 1}: {len(batch_urls)} URLs")

                    batch_results = await crawler.arun_many(
                        urls=batch_urls,
                        config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
                    )

                    for url, result in zip(batch_urls, batch_results):
                        results.append(process_crawl_result(url, result, request.use_url_configs))

                    if i + request.parallel < len(request.urls):
                        await asyncio.sleep(request.delay_between)

        successful = sum(1 for r in results if r['success'])

        return {
            "success": True,
            "total_urls": len(request.urls),
            "successful": successful,
            "failed": len(request.urls) - successful,
            "dispatcher_used": "memory_adaptive" if dispatcher else "semaphore_batch",
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


def process_crawl_result(url: str, result, use_url_configs: bool = True) -> Dict[str, Any]:
    """Process a single crawl result into standardized format."""
    if not result.success:
        return {
            'url': url,
            'success': False,
            'error': str(result.error) if hasattr(result, 'error') else "Unknown error"
        }

    # Try Ashby extraction first
    content, title = extract_ashby_content(url, result)

    # Fall back to standard extraction
    if not content:
        content = result.markdown if hasattr(result, 'markdown') else ""
        title = result.metadata.get("title", "") if hasattr(result, 'metadata') and result.metadata else ""

    # Get URL-specific config info
    url_config = get_config_for_url(url) if use_url_configs else URL_CONFIGS["default"]

    return {
        'url': url,
        'success': True,
        'title': title,
        'content_length': len(content),
        'content': content[:10000],
        'url_config_type': url_config.get("description", "default") if use_url_configs else None
    }


async def crawl_many_stream(request: CrawlManyRequest) -> AsyncGenerator[str, None]:
    """
    Stream crawl results as NDJSON (newline-delimited JSON).
    Each line is a complete JSON object for one URL result.
    """
    print(f"📡 Streaming {len(request.urls)} URLs")

    browser_config = BrowserConfig(headless=True, viewport_width=1920, viewport_height=1080)

    try:
        async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
            # Process one at a time for true streaming
            for url in request.urls:
                try:
                    result = await crawler.arun(
                        url=url,
                        config=CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
                    )
                    processed = process_crawl_result(url, result, request.use_url_configs)
                    yield json.dumps(processed) + "\n"
                except Exception as e:
                    yield json.dumps({
                        'url': url,
                        'success': False,
                        'error': str(e)
                    }) + "\n"

    except Exception as e:
        yield json.dumps({
            'error': f"Stream error: {str(e)}",
            'timestamp': datetime.now().isoformat()
        }) + "\n"


@app.post("/crawl-articles")
async def crawl_articles(request: ArticleResearchRequest):
    """
    Optimized article research endpoint with BM25 filtering and adaptive resource management.

    Features:
    - BM25ContentFilter: Filters content by relevance to topic
    - PruningContentFilter: Removes low-value content (sidebars, ads)
    - MemoryAdaptiveDispatcher: Auto-throttles under memory pressure
    - URL-specific configs: Different settings for news/gov sites
    - Streaming option: Return results as NDJSON stream
    """
    print(f"📰 Article research: {len(request.urls)} URLs, topic: '{request.topic}'")
    print(f"   Adaptive: {request.use_adaptive}, Memory threshold: {request.memory_threshold}%")
    print(f"   BM25: {request.use_bm25}, Pruning: {request.use_pruning}, Stream: {request.stream}")

    if not request.urls:
        raise HTTPException(status_code=400, detail="No URLs provided")

    if len(request.urls) > 100:
        raise HTTPException(status_code=400, detail="Maximum 100 URLs per request")

    # Handle streaming response
    if request.stream:
        return StreamingResponse(
            crawl_articles_stream(request),
            media_type="application/x-ndjson"
        )

    results = []

    try:
        browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080
        )

        # Build query from topic + keywords
        query = request.topic
        if request.keywords:
            query += " " + " ".join(request.keywords)

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
            if request.use_bm25:
                bm25_filter = BM25ContentFilter(
                    user_query=query,
                    bm25_threshold=1.0
                )
                md_generator = DefaultMarkdownGenerator(content_filter=bm25_filter)
                run_config_kwargs["markdown_generator"] = md_generator
                print(f"   Using BM25 filter with query: '{query}'")
            elif request.use_pruning:
                prune_filter = PruningContentFilter(
                    threshold=0.4,
                    threshold_type="fixed",
                    min_word_threshold=request.min_word_count
                )
                md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)
                run_config_kwargs["markdown_generator"] = md_generator
                print("   Using Pruning filter")

        run_config = CrawlerRunConfig(**run_config_kwargs)

        # Create dispatcher if adaptive mode enabled
        dispatcher = None
        if request.use_adaptive and HAS_ADAPTIVE_DISPATCHER:
            dispatcher = create_dispatcher(request.parallel, request.memory_threshold)
            print(f"   Using MemoryAdaptiveDispatcher (max {request.parallel}, threshold {request.memory_threshold}%)")
        else:
            print(f"   Using semaphore-based batching (batch size {request.parallel})")

        async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
            if dispatcher and HAS_ADAPTIVE_DISPATCHER:
                # Use dispatcher for all URLs at once
                all_results = await crawler.arun_many(
                    urls=request.urls,
                    config=run_config,
                    dispatcher=dispatcher
                )

                for url, result in zip(request.urls, all_results):
                    results.append(process_article_result(url, result, request))
            else:
                # Fallback: batch processing
                for i in range(0, len(request.urls), request.parallel):
                    batch_urls = request.urls[i:i + request.parallel]
                    print(f"   Batch {i//request.parallel + 1}: {len(batch_urls)} URLs")

                    batch_results = await crawler.arun_many(
                        urls=batch_urls,
                        config=run_config
                    )

                    for url, result in zip(batch_urls, batch_results):
                        results.append(process_article_result(url, result, request))

                    if i + request.parallel < len(request.urls):
                        await asyncio.sleep(0.3)

        successful = sum(1 for r in results if r.get('success'))

        return {
            "success": True,
            "total_urls": len(request.urls),
            "successful": successful,
            "failed": len(request.urls) - successful,
            "topic": request.topic,
            "dispatcher_used": "memory_adaptive" if dispatcher else "semaphore_batch",
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


def process_article_result(url: str, result, request: ArticleResearchRequest) -> Dict[str, Any]:
    """Process a single article crawl result."""
    if not result.success:
        return {
            'url': url,
            'success': False,
            'error': str(result.error) if hasattr(result, 'error') else "Unknown error"
        }

    # Get filtered/fit markdown if available, else raw
    content = ""
    if hasattr(result, 'markdown_v2') and result.markdown_v2:
        if hasattr(result.markdown_v2, 'fit_markdown'):
            content = result.markdown_v2.fit_markdown or ""
        if not content and hasattr(result.markdown_v2, 'raw_markdown'):
            content = result.markdown_v2.raw_markdown or ""
    if not content:
        content = result.markdown if hasattr(result, 'markdown') else ""

    title = result.metadata.get("title", "") if hasattr(result, 'metadata') and result.metadata else ""

    # Get URL-specific config info
    url_config = get_config_for_url(url) if request.use_url_configs else None

    return {
        'url': url,
        'success': True,
        'title': title,
        'content_length': len(content),
        'content': content[:15000],
        'filtered': HAS_CONTENT_FILTERS and (request.use_bm25 or request.use_pruning),
        'url_config_type': url_config.get("description") if url_config else None
    }


async def crawl_articles_stream(request: ArticleResearchRequest) -> AsyncGenerator[str, None]:
    """Stream article crawl results as NDJSON."""
    print(f"📡 Streaming article research: {len(request.urls)} URLs, topic: '{request.topic}'")

    browser_config = BrowserConfig(headless=True, viewport_width=1920, viewport_height=1080)

    # Build query and config
    query = request.topic
    if request.keywords:
        query += " " + " ".join(request.keywords)

    run_config_kwargs = {
        "word_count_threshold": request.min_word_count,
        "exclude_external_links": True,
        "remove_overlay_elements": True,
        "cache_mode": CacheMode.BYPASS
    }

    if HAS_CONTENT_FILTERS and request.use_bm25:
        bm25_filter = BM25ContentFilter(user_query=query, bm25_threshold=1.0)
        md_generator = DefaultMarkdownGenerator(content_filter=bm25_filter)
        run_config_kwargs["markdown_generator"] = md_generator

    run_config = CrawlerRunConfig(**run_config_kwargs)

    try:
        async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
            for url in request.urls:
                try:
                    result = await crawler.arun(url=url, config=run_config)
                    processed = process_article_result(url, result, request)
                    yield json.dumps(processed) + "\n"
                except Exception as e:
                    yield json.dumps({
                        'url': url,
                        'success': False,
                        'error': str(e)
                    }) + "\n"

    except Exception as e:
        yield json.dumps({
            'error': f"Stream error: {str(e)}",
            'timestamp': datetime.now().isoformat()
        }) + "\n"


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
    print("🚀 Starting Crawl4AI Service v9.0.0")
    print("="*60)
    print("New in v9.0:")
    print(f"  {'✅' if HAS_ADAPTIVE_DISPATCHER else '⚠️'} MemoryAdaptiveDispatcher (auto-throttle under load)")
    print("  ✅ URL-specific configs (news/gov/corporate sites)")
    print("  ✅ Streaming results (NDJSON for progressive processing)")
    print("Existing Features:")
    print(f"  {'✅' if HAS_CONTENT_FILTERS else '⚠️'} BM25 content filtering")
    print(f"  {'✅' if HAS_CONTENT_FILTERS else '⚠️'} Pruning content filter")
    print("  ✅ Ashby job board support")
    print("  ✅ Quest worker compatibility")
    print("  ✅ Parallel batch processing")
    print("="*60)
    print("URL Configs:")
    for config_type, config in URL_CONFIGS.items():
        print(f"  {config_type}: {config['description']}")
    print("="*60)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )