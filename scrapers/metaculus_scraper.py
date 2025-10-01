from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
from datetime import datetime
import boto3
import os

def scrape_metaculus(topic):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)

    driver.get("https://www.metaculus.com/")
    driver.execute_script("window.scrollBy(0, 400);")
    time.sleep(2)

    search_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.mx-auto.block.size-full.rounded-full")))
    search_box.clear()
    search_box.send_keys(topic)
    search_box.send_keys(Keys.RETURN)
    time.sleep(8)

    filter_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@data-headlessui-state, '') and contains(., 'Hot')]")))
    driver.execute_script("arguments[0].click();", filter_btn)
    time.sleep(3)
    print("Hot Button Clicked")

    results_single = []
    results_multi = []
    CUTOFF_DATE = datetime(2025, 1, 1)
    MAX_QUESTIONS = 5

    for i in range(MAX_QUESTIONS):
        try:
            questions = driver.find_elements(By.XPATH, "//div[contains(@class, 'relative') and contains(@class, 'z-0') and contains(@class, 'flex') and contains(@class, 'flex-col') and contains(@class, 'items-center') and contains(@class, 'overflow-hidden') and contains(@class, 'rounded') and contains(@class, 'border') and contains(@class, 'no-underline') and (contains(@class, 'gap-2.5') or contains(@class, 'gap-2.5'))]")
            if i >= len(questions):
                print(f"Only found {len(questions)} questions, stopping.")
                break

            q = questions[i]
            link = q.find_element(By.TAG_NAME, "a")
            driver.execute_script("arguments[0].click();", link)

            title = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.m-0.w-full.pr-4.text-xl.leading-tight.text-blue-800"))).text.strip()

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            comment_containers = driver.find_elements(By.CSS_SELECTOR, "div.my-1\\.5.rounded-md.border")

            keep_question = False
            comment_data = []
            for comment_div in comment_containers:
                try:
                    date_posted_str = comment_div.find_element(By.TAG_NAME, "relative-time").get_attribute("datetime")
                    date_posted = datetime.fromisoformat(date_posted_str.replace("Z", "+00:00"))
                    if date_posted >= CUTOFF_DATE:
                        keep_question = True
                    text_parts = []
                    for p in comment_div.find_elements(By.CSS_SELECTOR, "p"):
                        if p.text.strip():
                            text_parts.append(p.text.strip())
                    for a in comment_div.find_elements(By.CSS_SELECTOR, "a"):
                        if a.text.strip():
                            text_parts.append(a.text.strip())
                        elif a.get_attribute("href"):
                            text_parts.append(a.get_attribute("href"))
                    for span in comment_div.find_elements(By.CSS_SELECTOR, "span[data-lexical-text='true']"):
                        if span.text.strip():
                            text_parts.append(span.text.strip())
                    text = " ".join(text_parts) if text_parts else "[No text]"
                    comment_data.append([date_posted, text])
                except:
                    continue

            if not keep_question:
                print(f"Skipping question '{title}' because all comments are before Jan 1, 2025")
                driver.back()
                time.sleep(2)
                continue

            is_multi_choice = False
            try:
                driver.find_element(By.CSS_SELECTOR, "div.relative.flex.h-8.w-full.items-center.justify-between.gap-2.rounded-lg.border.border-blue-400")
                is_multi_choice = True
            except:
                pass

            if is_multi_choice:
                option_divs = driver.find_elements(By.CSS_SELECTOR, "div.relative.flex.h-8.w-full.items-center.justify-between.gap-2.rounded-lg.border.border-blue-400")
                for div in option_divs:
                    spans = div.find_elements(By.TAG_NAME, "span")
                    if len(spans) >= 2:
                        option_name = spans[0].text.strip()
                        odd_percentage = spans[1].text.strip()
                        results_multi.append([title, option_name, odd_percentage])
            else:
                prob_elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.absolute.bottom-0.flex.w-\\[60px\\].flex-col.items-center.justify-center span.text-xl.font-bold.leading-8")))
                probability = prob_elem.text.strip()
                for date_posted, text in comment_data:
                    results_single.append([title, date_posted.isoformat(), probability, text])

            driver.back()
            time.sleep(3)
        except Exception as e:
            print(f"Error processing question {i+1}: {e}")
            driver.back()
            time.sleep(3)

    driver.quit()

    s3 = boto3.client("s3")
    bucket = os.getenv("S3_BUCKET", "kalshi-bronze-anubh-001")
    if results_single:
        s3.put_object(Bucket=bucket, Key=f"metaculus/{topic}_single.json", Body=json.dumps(results_single))
    if results_multi:
        s3.put_object(Bucket=bucket, Key=f"metaculus/{topic}_multi.json", Body=json.dumps(results_multi))

    return {"single": results_single, "multi": results_multi}