from kalshi_scraper import scrape_kalshi
from metaculus_scraper import scrape_metaculus
from news_scraper import scrape_news
from x_scraper import scrape_x
from google_trends_scraper import scrape_google_trends
from agent import summarize_topic
import os
import pandas as pd

def main():
    csv_file = "kalshi_political_forecasts.csv"
    # Scrape all Kalshi market questions
    if os.path.exists(csv_file):
        print(f"CSV {csv_file} already exists. Loading it instead of scraping...")
        kalshi_df = pd.read_csv(csv_file, encoding='utf-8')
    else:
        kalshi_df = scrape_kalshi()  
    for i in kalshi_df['Question'].unique():
 
        question_title = i
        print(f"Question: {question_title}")

        # Summarize per question
        topic = summarize_topic(question_title)
        print(f"🎯 General topic for scraping: {topic}")

        # Call other scrapers with this topic
        print(f'Metaculus - {topic}')
        scrape_metaculus(topic)
        print(f'news article - {topic}')
        scrape_news(topic)
        print(f'X posts - {topic}')
        scrape_x(topic)
        print(f'Google Trends - {topic}')
        scrape_google_trends(topic)

if __name__ == "__main__":
    main()