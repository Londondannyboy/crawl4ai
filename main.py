#!/usr/bin/env python3
"""
Crawl4AI Test Service

Simple FastAPI service to test Crawl4AI scraping on Railway.
"""
import asyncio
import sys
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import uvicorn

try:
    from crawl4ai import AsyncWebCrawler
except ImportError:
    print("❌ crawl4ai not installed")
    sys.exit(1)


app = FastAPI(
    title="Crawl4AI Test Service",
    description="Test Crawl4AI web scraping",
    version="1.0.0"
)


class ScrapeRequest(BaseModel):
    """Request model for scraping"""
    url: HttpUrl
    word_count_threshold: int = 10
    excluded_tags: list[str] = ["form", "nav", "footer"]
    remove_overlay_elements: bool = True


class ScrapeResponse(BaseModel):
    """Response model for scraping"""
    success: bool
    url: str
    content_length: int
    title: Optional[str]
    markdown: Optional[str]
    error: Optional[str]
    timestamp: str


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Crawl4AI Test Service",
        "version": "1.0.0",
        "endpoints": {
            "/scrape": "POST - Scrape a URL",
            "/test/evercore": "GET - Test with Evercore",
            "/health": "GET - Health check"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_url(request: ScrapeRequest):
    """
    Scrape a URL using Crawl4AI

    Args:
        request: Scraping configuration

    Returns:
        Scraped content and metadata
    """
    print(f"🔍 Scraping: {request.url}")

    try:
        async with AsyncWebCrawler(
            verbose=True,
            headless=True
        ) as crawler:
            result = await crawler.arun(
                url=str(request.url),
                bypass_cache=True,
                word_count_threshold=request.word_count_threshold,
                excluded_tags=request.excluded_tags,
                remove_overlay_elements=request.remove_overlay_elements,
            )

            response = ScrapeResponse(
                success=result.success,
                url=result.url,
                content_length=len(result.markdown) if result.markdown else 0,
                title=result.metadata.get("title") if hasattr(result, 'metadata') else None,
                markdown=result.markdown,
                error=None,
                timestamp=datetime.now().isoformat()
            )

            print(f"✅ Success: {response.content_length:,} chars")
            return response

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "type": type(e).__name__
            }
        )


@app.get("/test/evercore", response_model=ScrapeResponse)
async def test_evercore():
    """
    Test scraping on Evercore.com

    Returns:
        Scraped Evercore content
    """
    print("🧪 Testing Evercore scraping...")

    request = ScrapeRequest(url="https://www.evercore.com")
    result = await scrape_url(request)

    # Show preview in logs
    if result.markdown:
        print(f"\n{'='*70}")
        print(f"📝 Preview (first 500 chars):")
        print(result.markdown[:500])
        print(f"{'='*70}\n")

    return result


if __name__ == "__main__":
    print("🚀 Starting Crawl4AI Test Service...")
    print("   Port: 8000")
    print("   Endpoints: /scrape, /test/evercore, /health")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
