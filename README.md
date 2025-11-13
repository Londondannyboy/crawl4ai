# Crawl4AI Test Service

Standalone service to test Crawl4AI web scraping for Quest project.

## Purpose

Test Crawl4AI as a potential replacement for the expensive 4-scraper parallel approach currently used in Quest worker.

**Current setup (Quest worker):**
- 4 parallel scrapers: Exa, Firecrawl, Tavily, Direct
- Cost: ~$0.17-0.50 per scrape
- Time: 5-30 seconds
- Selection: Most content wins

**Crawl4AI benefits:**
- Free/open-source (no API costs)
- Fast (2-5 seconds)
- Good markdown output
- Handles JavaScript rendering
- Headless browser support

## Setup

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Setup Playwright browsers
playwright install chromium

# Run the service
python main.py
```

### Docker

```bash
# Build
docker build -t crawl4ai-test .

# Run
docker run -p 8000:8000 crawl4ai-test
```

## API Endpoints

**Root**
```
GET /
```

**Scrape a URL**
```
POST /scrape
{
  "url": "https://example.com",
  "word_count_threshold": 10,
  "excluded_tags": ["form", "nav", "footer"],
  "remove_overlay_elements": true
}
```

**Test with Evercore**
```
GET /test/evercore
```

**Health Check**
```
GET /health
```

## Deployment

Deployed as a separate Railway service for isolated testing.

## Evaluation

Compare Crawl4AI output quality against current Quest worker scrapers:
- Content completeness
- Markdown formatting
- Speed
- Reliability
- Cost savings

## Next Steps

If Crawl4AI proves effective:
1. Integrate into Quest worker as primary scraper
2. Keep Firecrawl as fallback
3. Remove expensive Tavily/Exa content scraping
4. Reduce per-scrape costs by 80-90%
