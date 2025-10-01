from newsapi import NewsApiClient
import datetime
import json
import boto3
import os

def scrape_news(topic):
    newsapi = NewsApiClient(api_key="9372813c3cc844e1882244b9a6c888a3")
    today = datetime.datetime.utcnow().date()
    yesterday = today - datetime.timedelta(days=1)

    articles = newsapi.get_everything(
        q=topic,
        language="en",
        sort_by="publishedAt",
        from_param=yesterday.strftime("%Y-%m-%d"),
        to=yesterday.strftime("%Y-%m-%d"),
        page_size=20,
        page=1
    )
    articles = articles.get("articles", [])

    rows = []
    for art in articles:
        rows.append({
            "market": topic,
            "title": art.get("title"),
            "description": art.get("description"),
            "content": art.get("content"),
            "source": art["source"]["name"],
            "url": art.get("url"),
            "publishedAt": art.get("publishedAt")
        })

    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET", "kalshi-bronze-anubh-001")
    s3.put_object(Bucket=bucket, Key=f"news/{topic       }.json", Body=json.dumps(rows))

    return rows