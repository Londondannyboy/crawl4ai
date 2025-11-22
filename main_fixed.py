#!/usr/bin/env python3
"""
Fixed Crawl4AI Service for Railway
Properly handles JavaScript rendering for Ashby and other dynamic sites
"""
from __future__ import annotations
import asyncio
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import re

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn

try:
    from crawl4ai import AsyncWebCrawler
    from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
except ImportError:
    print("❌ crawl4ai not installed")
    sys.exit(1)


app = FastAPI(
    title="Crawl4AI Service - Fixed",
    description="Web scraping with proper JS support",
    version="3.0.0"
)


class ScrapeRequest(BaseModel):
    """Request model for scraping"""
    url: str
    word_count_threshold: int = 10
    excluded_tags: List[str] = Field(default_factory=lambda: ["form", "nav", "footer"])
    remove_overlay_elements: bool = True
    wait_for: Optional[str] = None
    js_code: Optional[str] = None
    bypass_cache: bool = True
    verbose: bool = False


class CrawlRequest(BaseModel):
    """Request for multi-page crawling"""
    url: str
    max_pages: int = 5


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Crawl4AI Service - Fixed",
        "version": "3.0.0",
        "endpoints": {
            "/scrape": "POST - Scrape with JS support",
            "/crawl": "POST - Multi-page crawling",
            "/health": "GET - Health check"
        }
    }


@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/scrape")
async def scrape_url(request: ScrapeRequest):
    """
    Scrape a URL with proper JavaScript handling
    """
    print(f"🔍 Scraping: {request.url}")
    
    try:
        # Create crawler with proper configuration
        async with AsyncWebCrawler(
            verbose=request.verbose,
            headless=True,
            browser_type="chromium",
            page_timeout=30000,  # 30 seconds timeout
            wait_until="networkidle"  # Wait for network to be idle
        ) as crawler:
            
            # Build parameters
            params = {
                "url": request.url,
                "bypass_cache": request.bypass_cache,
                "word_count_threshold": request.word_count_threshold,
                "excluded_tags": request.excluded_tags or [],
                "remove_overlay_elements": request.remove_overlay_elements,
            }
            
            # For Ashby boards, inject JS to extract data
            if "ashbyhq.com" in request.url:
                print("   Detected Ashby board - extracting __appData")
                params["js_code"] = """
                // Wait for app data to load
                if (window.__appData && window.__appData.jobBoard) {
                    // Extract job data and inject into page
                    const jobData = window.__appData.jobBoard.jobPostings || [];
                    const container = document.createElement('div');
                    container.id = 'extracted-jobs';
                    container.style.display = 'none';
                    
                    jobData.forEach(job => {
                        if (job.isListed) {
                            const jobDiv = document.createElement('div');
                            jobDiv.className = 'extracted-job';
                            jobDiv.innerHTML = `
                                <h2>${job.title}</h2>
                                <div class="department">${job.departmentName || ''}</div>
                                <div class="location">${job.locationName || ''}</div>
                                <div class="type">${job.employmentType || ''} - ${job.workplaceType || ''}</div>
                                <div class="posted">${job.publishedDate || ''}</div>
                                <a href="/ashbyhq.com/${job.id}">${job.title}</a>
                            `;
                            container.appendChild(jobDiv);
                        }
                    });
                    
                    document.body.appendChild(container);
                    
                    // Also create visible text for markdown conversion
                    const textDiv = document.createElement('div');
                    textDiv.id = 'jobs-text';
                    textDiv.innerHTML = '<h1>Job Openings</h1>';
                    
                    jobData.forEach(job => {
                        if (job.isListed) {
                            textDiv.innerHTML += `
                                <div style="margin: 20px 0;">
                                    <h2>${job.title}</h2>
                                    <p>Department: ${job.departmentName || 'N/A'}</p>
                                    <p>Location: ${job.locationName || 'N/A'}</p>
                                    <p>Type: ${job.employmentType || 'N/A'} - ${job.workplaceType || 'N/A'}</p>
                                    <p>Posted: ${job.publishedDate || 'N/A'}</p>
                                </div>
                            `;
                        }
                    });
                    
                    // Replace page content with job listings
                    document.body.innerHTML = textDiv.innerHTML;
                }
                """
                
                # Wait for the data to be available
                params["wait_for"] = "window.__appData"
                params["timeout"] = 10000
            
            elif request.wait_for:
                params["wait_for"] = request.wait_for
                
            if request.js_code and "ashbyhq.com" not in request.url:
                params["js_code"] = request.js_code
            
            # Execute crawl
            result = await crawler.arun(**params)
            
            # Extract content
            content = result.markdown if hasattr(result, 'markdown') else ""
            
            # For Ashby, also try to extract from the result HTML
            if "ashbyhq.com" in request.url and not content:
                if hasattr(result, 'html'):
                    # Try to extract from HTML
                    html = result.html
                    if '__appData' in html:
                        match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                        if match:
                            try:
                                app_data = json.loads(match.group(1))
                                jobs = app_data.get('jobBoard', {}).get('jobPostings', [])
                                
                                # Convert to markdown
                                content_parts = ["# Job Openings\n"]
                                for job in jobs:
                                    if job.get('isListed'):
                                        content_parts.append(f"## {job.get('title', 'N/A')}")
                                        content_parts.append(f"**Department:** {job.get('departmentName', 'N/A')}")
                                        content_parts.append(f"**Location:** {job.get('locationName', 'N/A')}")
                                        content_parts.append(f"**Type:** {job.get('employmentType', 'N/A')} - {job.get('workplaceType', 'N/A')}")
                                        content_parts.append(f"**Posted:** {job.get('publishedDate', 'N/A')}")
                                        content_parts.append("")
                                
                                content = "\n".join(content_parts)
                                print(f"   ✅ Extracted {len(jobs)} jobs from __appData")
                            except Exception as e:
                                print(f"   ❌ Failed to parse __appData: {e}")
            
            return {
                "success": result.success if hasattr(result, 'success') else bool(content),
                "url": request.url,
                "content_length": len(content),
                "title": result.metadata.get("title", "") if hasattr(result, 'metadata') else "",
                "markdown": content,
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
    Multi-page crawling endpoint for Quest worker compatibility
    """
    print(f"🕷️ Crawling: {request.url}")
    
    pages = []
    
    try:
        async with AsyncWebCrawler(
            verbose=True,
            headless=True,
            browser_type="chromium"
        ) as crawler:
            
            # For now, just crawl the main page
            # (multi-page can be added later)
            result = await crawler.arun(
                url=request.url,
                bypass_cache=True,
                word_count_threshold=10
            )
            
            # Check if it's Ashby and extract accordingly
            content = ""
            if "ashbyhq.com" in request.url:
                # Try to extract Ashby data
                if hasattr(result, 'html'):
                    html = result.html
                    match = re.search(r'window\.__appData\s*=\s*({.*?});', html, re.DOTALL)
                    if match:
                        try:
                            app_data = json.loads(match.group(1))
                            jobs = app_data.get('jobBoard', {}).get('jobPostings', [])
                            
                            content_parts = []
                            for job in jobs:
                                if job.get('isListed'):
                                    content_parts.append(f"{job.get('title', 'N/A')} - {job.get('departmentName', 'N/A')} - {job.get('locationName', 'N/A')}")
                            
                            content = "\n".join(content_parts)
                        except:
                            content = result.markdown if hasattr(result, 'markdown') else ""
                    else:
                        content = result.markdown if hasattr(result, 'markdown') else ""
            else:
                content = result.markdown if hasattr(result, 'markdown') else ""
            
            if content:
                pages.append({
                    "url": request.url,
                    "title": result.metadata.get("title", "") if hasattr(result, 'metadata') else "",
                    "content": content[:10000],  # Limit content size
                    "links": []
                })
            
            # Try simpler approach without links extraction for now
            success = len(pages) > 0
            
    except Exception as e:
        print(f"Error in crawl: {e}")
        success = False
        pages = [{
            "url": request.url,
            "title": "Error",
            "content": str(e),
            "links": []
        }]
    
    return {
        "success": success,
        "pages": pages,
        "links": [],  # Simplified for now
        "crawler": "crawl4ai_service"
    }


if __name__ == "__main__":
    print("🚀 Starting Fixed Crawl4AI Service...")
    print("   Port: 8000")
    print("   Features: Ashby job board support, JS execution")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )