from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import os
import time
import boto3
import json

def scrape_google_trends(topic):
    download_dir = "/tmp"
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)

    home_url = "https://trends.google.com/trends?geo=US&hl=en-US"
    driver.get(home_url)
    time.sleep(5)

    try:
        search_bar = driver.find_element(By.CSS_SELECTOR, "input.Fgl6fe-fmcmS-wGMbrd.fnqhWc")
        search_bar.clear()
        time.sleep(1)
        search_bar.send_keys(topic)
        search_bar.send_keys(Keys.ENTER)
        print(f"🔎 Searching topic: {topic}")
        time.sleep(8)
    except Exception as e:
        print(f"❌ Could not enter topic: {e}")
        driver.quit()
        return []

    def download_trends(source):
        files = []
        if source == "youtube":
            try:
                picker = driver.find_element(By.CSS_SELECTOR, "md-select.explore-select.compare-picker")
                picker.click()
                time.sleep(1)
                yt_option = driver.find_element(By.CSS_SELECTOR, "md-option[value='youtube']")
                yt_option.click()
                print("🔄 Switched to YouTube")
                time.sleep(5)
            except Exception as e:
                print(f"❌ Failed to switch to YouTube: {e}")
                return files

        try:
            line_chart = driver.find_element(By.CSS_SELECTOR, "div.fe-line-chart")
            try:
                cookie_btn = driver.find_element(By.CSS_SELECTOR, ".cookieBarConsentButton")
                cookie_btn.click()
            except:
                pass
            btn = line_chart.find_element(By.CSS_SELECTOR, "button.widget-actions-item.export")
            btn.click()
            print(f"⬇ Downloading {source} time-series")
            time.sleep(10)
            files.extend([f for f in os.listdir(download_dir) if f.endswith(".csv")])
        except Exception as e:
            print(f"❌ Could not download {source} time-series: {e}")

        try:
            region_chart = driver.find_element(By.CSS_SELECTOR, "div.fe-geo-chart-generated")
            btn = region_chart.find_element(By.CSS_SELECTOR, "button.widget-actions-item.export")
            btn.click()
            print(f"⬇ Downloading {source} regional")
            time.sleep(10)
            files.extend([f for f in os.listdir(download_dir) if f.endswith(".csv")])
        except Exception as e:
            print(f"❌ Could not download {source} region: {e}")

        return files

    google_files = download_trends("google")
    youtube_files = download_trends("youtube")
    driver.quit()

    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET", "kalshi-bronze-anubh-001")
    for file in google_files + youtube_files:
        with open(os.path.join(download_dir, file), "rb") as f:
            s3.put_object(Bucket=bucket, Key=f"trends/{topic}/{file}", Body=f)

    return google_files + youtube_files