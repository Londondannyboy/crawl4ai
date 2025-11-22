#!/usr/bin/env python3
"""
Test script for Crawl4AI v5.0 multi-page crawling
Demonstrates URL discovery and parallel crawling
"""

import asyncio
import httpx
import json
from datetime import datetime


async def test_discovery():
    """Test URL discovery from a job board"""
    print("\n" + "="*60)
    print("TEST 1: URL DISCOVERY")
    print("="*60)
    
    async with httpx.AsyncClient(timeout=30) as client:
        # Test with Clay Labs job board
        response = await client.post(
            "http://localhost:8000/discover",
            json={
                "url": "https://jobs.ashbyhq.com/claylabs",
                "max_urls": 10
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Discovery successful!")
            print(f"   URLs found: {data['urls_discovered']}")
            
            # Show first few discovered URLs
            for i, url_info in enumerate(data['urls'][:5], 1):
                print(f"   {i}. {url_info['title'] or 'Job'}: {url_info['url']}")
            
            return data['urls']
        else:
            print(f"❌ Discovery failed: {response.status_code}")
            return []


async def test_crawl_many(urls):
    """Test crawling multiple URLs in parallel"""
    print("\n" + "="*60)
    print("TEST 2: MULTI-PAGE CRAWLING")
    print("="*60)
    
    if not urls:
        print("No URLs to crawl")
        return
    
    # Take first 5 URLs for testing
    test_urls = [u['url'] for u in urls[:5]]
    
    print(f"Crawling {len(test_urls)} URLs in parallel...")
    
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            "http://localhost:8000/crawl-many",
            json={
                "urls": test_urls,
                "parallel": 3,
                "delay_between": 0.5
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Multi-crawl completed!")
            print(f"   Successful: {data['successful']}/{data['total_urls']}")
            
            # Show results
            for result in data['results']:
                if result['success']:
                    print(f"   ✓ {result['url'][:50]}... ({result['content_length']} chars)")
                else:
                    print(f"   ✗ {result['url'][:50]}... Error: {result.get('error')}")
        else:
            print(f"❌ Multi-crawl failed: {response.status_code}")


async def test_full_workflow():
    """Test complete workflow: discover -> crawl many"""
    print("\n" + "="*80)
    print("FULL WORKFLOW TEST: DISCOVER + MULTI-CRAWL")
    print("="*80)
    
    print("""
    This demonstrates the complete job scraping workflow:
    1. Discover all job URLs from the board
    2. Crawl multiple job pages in parallel
    3. Extract full job details from each page
    """)
    
    # Step 1: Discover URLs
    discovered_urls = await test_discovery()
    
    if discovered_urls:
        # Step 2: Crawl the discovered URLs
        await test_crawl_many(discovered_urls)
    
    print("\n" + "="*60)
    print("WORKFLOW COMPLETE")
    print("="*60)


async def compare_approaches():
    """Compare old vs new approach"""
    print("\n" + "="*80)
    print("APPROACH COMPARISON")
    print("="*80)
    
    print("""
    OLD APPROACH (Current):
    1. Scrape job board page → Get basic info
    2. Store in database with NULL descriptions
    3. Later, loop through each job URL one by one
    4. Total time: O(n) where n = number of jobs
    
    NEW APPROACH (v5.0):
    1. /discover → Find all job URLs instantly
    2. /crawl-many → Get all pages in parallel
    3. Extract full details in one operation
    4. Total time: O(n/p) where p = parallel workers
    
    BENEFITS:
    - 5x-10x faster with parallel crawling
    - Single API call vs multiple rounds
    - Built-in rate limiting and error handling
    - Can resume from failures
    """)


async def main():
    """Run all tests"""
    print("🚀 Testing Crawl4AI v5.0 Multi-Page Features")
    
    # Make sure service is running
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:8000/health")
            if response.status_code != 200:
                print("❌ Service not running. Start it with: python main_v5.py")
                return
    except:
        print("❌ Cannot connect to service. Start it with: python main_v5.py")
        return
    
    # Run tests
    await compare_approaches()
    await test_full_workflow()


if __name__ == "__main__":
    asyncio.run(main())