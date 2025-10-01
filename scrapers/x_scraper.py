from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import csv
import boto3
import os
from io import StringIO
from datetime import datetime

def scrape_x(topic):
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    username = os.getenv("X_USERNAME", "username")
    password = os.getenv("X_PASSWORD", "password")

    driver.get("https://x.com/login")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "text")))
    user_input = driver.find_element(By.NAME, "text")
    user_input.send_keys(username)
    user_input.send_keys(Keys.RETURN)

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "password")))
    pwd_input = driver.find_element(By.NAME, "password")
    pwd_input.send_keys(password)
    pwd_input.send_keys(Keys.RETURN)

    WebDriverWait(driver, 20).until(EC.url_contains("home"))
    time.sleep(3)

    search_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@data-testid='SearchBox_Search_Input']")))
    search_box.clear()
    search_box.send_keys(topic)
    search_box.send_keys(Keys.RETURN)
    time.sleep(3)

    # Click Latest tab
    try:
        latest_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.css-175oi2r.r-1awozwy.r-6koalj.r-eqz5dr.r-16y2uox.r-1h3ijdo.r-1777fci.r-s8bhmr.r-3pj75a.r-o7ynqc.r-6416eg.r-1ny4l3l.r-1loqt21"))
        )
        latest_tab.click()
        time.sleep(3)
    except:
        print("Latest tab not found, continuing with default feed")

    top_posts = []
    seen = set()
    num_top = 80
    while len(top_posts) < num_top:
        posts = driver.find_elements(By.CSS_SELECTOR, "div.css-175oi2r[data-testid='cellInnerDiv']")
        for post in posts:
            if len(top_posts) >= num_top:
                break
            try:
                spans = post.find_elements(By.CSS_SELECTOR, "div[data-testid='tweetText'] span")
                text = "".join(span.text for span in spans).strip()
                time_elem = post.find_element(By.CSS_SELECTOR, "time")
                date = time_elem.get_attribute("datetime")
                if text and (text, date) not in seen:
                    seen.add((text, date))
                    top_posts.append({"text": text, "date": date})
            except:
                continue
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    driver.quit()

    # Convert to CSV with different columns
    csv_buffer = StringIO()
    fieldnames = ["datetime", "topic", "context"]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    
    for post in top_posts:
        if post["date"]:
            parsed_date = datetime.fromisoformat(post["date"].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
        else:
            parsed_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
            
        writer.writerow({
            "datetime": parsed_date,
            "topic": topic,
            "context": post["text"]
        })

    # Upload CSV to S3
    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET", "kalshi-bronze-anubh-001")
    s3.put_object(
        Bucket=bucket, 
        Key=f"social/{topic}_context.csv", 
        Body=csv_buffer.getvalue()
    )

    return top_posts

if __name__ == "__main__":
    scrape_x("New Jersey Governor Election")
