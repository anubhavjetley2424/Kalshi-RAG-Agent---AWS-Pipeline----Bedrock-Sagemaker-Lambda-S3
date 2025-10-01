from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import requests
import pandas as pd
import boto3

# ------------------------
# Step 1: Setup Selenium
# ------------------------

def scrape_kalshi():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://kalshi.com/?category=politics")
    time.sleep(5)  # wait for page to load

    # ------------------------
    # Step 2: Collect all market hrefs
    # ------------------------
    cards = driver.find_elements(By.CSS_SELECTOR, "a.text-text-x10.w-full")
    hrefs = [c.get_attribute("href") for c in cards]
    print(f"Found {len(hrefs)} market links")

    all_forecasts = []

    # ------------------------
    # Step 3: Loop through each market page
    # ------------------------
    for idx, href in enumerate(hrefs[:3]):  # limit to first 50 markets
        try:
            driver.requests.clear()  # clear old XHRs
            driver.get(href)

            # Wait for question title to appear
            h1 = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".flex.flex-col.justify-center.gap\\-0\\.5.w-full.mr\\-3 h1")
                )
            )
            question_title = h1.text.strip()
            print(f"Question title detected: {question_title}")

        

            # Extract ticker from URL and uppercase
            market_ticker = href.rstrip("/").split("/")[-1].upper().split("-")[0]

            # Wait up to 20s for XHR requests containing this ticker
            max_wait = 20
            interval = 1
            elapsed = 0
            matching_requests = []

            while elapsed < max_wait:
                time.sleep(interval)
                elapsed += interval
                matching_requests = [
                    r for r in driver.requests
                    if r.response
                    and "/forecast_history" in r.url.lower()
                    and market_ticker in r.url.upper()
                ]
                if matching_requests:
                    break

            if not matching_requests:
                print(f"No forecast XHR found for {market_ticker}, skipping...")
                continue

            # Process all matching forecast XHRs
            for request in matching_requests:
                forecast_url = request.url
                print(f"Captured forecast_history URL: {forecast_url}")
                resp = requests.get(forecast_url)
                if resp.ok:
                    data = resp.json()
                    for forecast in data.get("forecast_history", []):
                        # Ensure it matches the market ticker
                        if forecast.get("market_ticker", "").upper() != market_ticker:
                            print(f"Correct URL's taken {market_ticker}")
                            
                            option = forecast.get("market_ticker")
                            date = datetime.utcfromtimestamp(forecast["end_period_ts"]).strftime("%Y-%m-%d %H:%M")
                            odds = forecast.get("numerical_forecast") or forecast.get("mean_price")
                            all_forecasts.append([question_title, option, date, odds])
                            print(all_forecasts[-1])

            driver.requests.clear()  # clear before next iteration

        except Exception as e:
            print(f"Error on card {idx} ({href}): {e}")

    driver.quit()

    # ------------------------
    # Step 4: Save to CSV
    # ------------------------
    df = pd.DataFrame(all_forecasts, columns=["Question", "Option", "Date", "Odds (%)"])
    df = df.sort_values(by=["Question", "Date"])
    df.to_csv("kalshi_political_forecasts.csv", index=False)
    print("\nCSV saved: kalshi_political_forecasts.csv")

    # ------------------------
    # Step 5: Upload to S3
    # ------------------------
    AWS_BUCKET = "kalshi-bronze-anubh-001"
    s3 = boto3.client("s3", region_name="ap-southeast-2")

    try:
        csv_file = 'kalshi_political_forecasts.csv'
        s3.upload_file(csv_file, AWS_BUCKET, "kalshi/kalshi_political_forecasts.csv")
        print(f"Uploaded CSV to S3: s3://{AWS_BUCKET}/kalshi/kalshi_political_forecasts.csv")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")

    return df