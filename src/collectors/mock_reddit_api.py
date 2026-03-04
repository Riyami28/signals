"""
Mock Reddit API server for testing/development without real Reddit credentials.
Serves Reddit-like JSON data for signal collection.
"""

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from typing import Optional
import json
from datetime import datetime, timedelta

app = FastAPI(title="Mock Reddit API")

# Sample Reddit posts data for different companies
MOCK_POSTS = {
    "datadog": [
        {
            "id": "post_001",
            "title": "Datadog Supply Chain Platform Rollout",
            "selftext": "Supply chain platform rollout for vendor consolidation. Integrating multiple SaaS tools into single pane of glass.",
            "subreddit": "devops",
            "url": "https://reddit.com/r/devops/comments/mock_001",
            "created_utc": (datetime.utcnow() - timedelta(days=1)).timestamp(),
            "score": 150,
            "num_comments": 25,
            "author": "datadog_team"
        },
        {
            "id": "post_002",
            "title": "Datadog Infrastructure Modernization",
            "selftext": "Moving to Kubernetes for better orchestration. Using Terraform for IaC across all environments.",
            "subreddit": "devops",
            "url": "https://reddit.com/r/devops/comments/mock_002",
            "created_utc": (datetime.utcnow() - timedelta(days=5)).timestamp(),
            "score": 200,
            "num_comments": 45,
            "author": "datadog_eng"
        },
    ],
    "stripe": [
        {
            "id": "post_003",
            "title": "Stripe FinOps Initiative",
            "selftext": "Just rolled out our new FinOps initiative to reduce cloud spending by 30% using cost optimization tooling.",
            "subreddit": "finops",
            "url": "https://reddit.com/r/finops/comments/mock_003",
            "created_utc": (datetime.utcnow() - timedelta(days=2)).timestamp(),
            "score": 180,
            "num_comments": 35,
            "author": "stripe_ops"
        },
        {
            "id": "post_004",
            "title": "Stripe Payment Infrastructure",
            "selftext": "Scaling our payment processing infrastructure with distributed systems and event-driven architecture.",
            "subreddit": "devops",
            "url": "https://reddit.com/r/devops/comments/mock_004",
            "created_utc": (datetime.utcnow() - timedelta(days=10)).timestamp(),
            "score": 220,
            "num_comments": 50,
            "author": "stripe_infra"
        },
    ],
    "notion": [
        {
            "id": "post_005",
            "title": "Notion Enterprise Modernization",
            "selftext": "Enterprise modernization program: Moving from monolith to event-driven architecture with Kafka and gRPC.",
            "subreddit": "softwaredeveloper",
            "url": "https://reddit.com/r/softwaredeveloper/comments/mock_005",
            "created_utc": (datetime.utcnow() - timedelta(days=3)).timestamp(),
            "score": 160,
            "num_comments": 30,
            "author": "notion_eng"
        },
    ],
    "figma": [
        {
            "id": "post_006",
            "title": "Figma DevOps Challenges",
            "selftext": "Python became our bottleneck in microservices. Evaluating Go and Rust for performance improvements.",
            "subreddit": "golang",
            "url": "https://reddit.com/r/golang/comments/mock_006",
            "created_utc": (datetime.utcnow() - timedelta(days=7)).timestamp(),
            "score": 140,
            "num_comments": 28,
            "author": "figma_devops"
        },
    ],
    "github": [
        {
            "id": "post_007",
            "title": "GitHub IDP Golden Path",
            "selftext": "Adopting IDP (Internal Developer Platform) golden path initiative to reduce developer toil and improve velocity.",
            "subreddit": "platformengineering",
            "url": "https://reddit.com/r/platformengineering/comments/mock_007",
            "created_utc": (datetime.utcnow() - timedelta(days=4)).timestamp(),
            "score": 190,
            "num_comments": 40,
            "author": "github_platform"
        },
    ],
}

@app.get("/search")
async def search_posts(
    q: Optional[str] = Query(None),
    subreddit: Optional[str] = Query(None),
    sort: Optional[str] = Query("new"),
    time_filter: Optional[str] = Query("month"),
    limit: Optional[int] = Query(25),
):
    """Mock Reddit search endpoint"""
    results = []

    # Return all posts if no specific query
    all_posts = []
    for company_posts in MOCK_POSTS.values():
        all_posts.extend(company_posts)

    # Filter by subreddit if specified
    if subreddit:
        all_posts = [p for p in all_posts if p["subreddit"] == subreddit]

    # Filter by search query if specified
    if q:
        q_lower = q.lower()
        all_posts = [p for p in all_posts if q_lower in p["title"].lower() or q_lower in p["selftext"].lower()]

    # Sort by created_utc descending (newest first)
    all_posts = sorted(all_posts, key=lambda x: x["created_utc"], reverse=True)

    # Limit results
    all_posts = all_posts[:limit]

    return JSONResponse({
        "data": {
            "children": [{"data": post} for post in all_posts],
            "after": None,
            "before": None,
        }
    })

@app.get("/r/{subreddit}/new")
async def subreddit_posts(
    subreddit: str,
    limit: Optional[int] = Query(25),
):
    """Mock Reddit subreddit endpoint"""
    all_posts = []
    for company_posts in MOCK_POSTS.values():
        all_posts.extend(company_posts)

    # Filter by subreddit
    all_posts = [p for p in all_posts if p["subreddit"] == subreddit]

    # Sort by created_utc descending
    all_posts = sorted(all_posts, key=lambda x: x["created_utc"], reverse=True)

    # Limit
    all_posts = all_posts[:limit]

    return JSONResponse({
        "data": {
            "children": [{"data": post} for post in all_posts],
            "after": None,
            "before": None,
        }
    })

@app.get("/api/v1/me")
async def get_user_info():
    """Mock Reddit user info endpoint"""
    return JSONResponse({
        "id": "mock_user_id",
        "name": "zopdev_signals",
        "link_karma": 1000,
        "comment_karma": 5000,
    })

@app.get("/robots.txt")
async def robots_txt():
    """Mock robots.txt - allow all"""
    return "User-agent: *\nDisallow: "

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse({"status": "ok", "api": "mock_reddit_api"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8765)
