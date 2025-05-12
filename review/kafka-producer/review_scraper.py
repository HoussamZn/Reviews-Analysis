from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from time import sleep
import random

class Scraper:
    def __init__(self):
        self.driver = None

    def _initialize_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(options=chrome_options)

    def scrape_page(self, target_url:str=""):
        """
        Scrapes the page using Selenium to render JavaScript content.
        """
        self._initialize_driver()
        try:
            self.driver.get(target_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            rendered_html = self.driver.page_source
            return rendered_html
        finally:
            self.driver.quit()

    def extract_reviews(self, html_content):
        """
        Extracts reviews and product ASIN from the HTML content.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        result = {
            "ASIN": "",
            "reviews": []
        }
        try:
            asin_input = soup.find("input", {"id": "ASIN", "name": "ASIN"})
            if asin_input and asin_input.has_attr("value"):
                result["ASIN"] = asin_input["value"]

            review_blocks = soup.find_all('li', class_="review aok-relative")
            # print(f"Review blocks found: {len(review_blocks)}")

            for review in review_blocks:
                customer_name = review.find('span', class_='a-profile-name')
                customer_name = customer_name.get_text(strip=True) if customer_name else None
                review_date = review.find('span', class_='review-date')
                review_date = review_date.get_text(strip=True) if review_date else None
                review_text = review.find('div', class_='a-expander-content reviewText review-text-content a-expander-partial-collapse-content')
                review_text = review_text.find('span').get_text(strip=True) if review_text else None
                review_title = review.find('a', {'data-hook': 'review-title'})
                review_title = review_title.find_all('span')[-1].text.strip() if review_title else None
                result["reviews"].append({
                    'customer_name': customer_name,
                    'review_date': review_date,
                    'review_text': review_text,
                    'review_title': review_title,
                })
            return result
        except Exception as e:
            print(f"Error processing reviews: {str(e)}")
            return result

    def get_reviews(self, target_url:str=""):
        """
        Main method to scrape and extract reviews.
        """
        sleep(random.uniform(1, 3))
        html_content = self.scrape_page(target_url)
        if html_content:
            return self.extract_reviews(html_content)
        else:
            print("Failed to retrieve the page content.")
            return None


if __name__ == "__main__":
    # TARGET_URL = "https://www.amazon.com/SANSUI-Monitor-24-Ultra-Slim-Ergonomic-ES-24F1/dp/B0B17KHCQN?pd_rd_w=akzS2&content-id=amzn1.sym.cd152278-debd-42b9-91b9-6f271389fda7&pf_rd_p=cd152278-debd-42b9-91b9-6f271389fda7&pf_rd_r=1SPTFS9M8G1X0PC6RGS1&pd_rd_wg=AUPcf&pd_rd_r=4f4ed6d1-16ce-4cc2-8a21-d092d7ac605e&pd_rd_i=B0B17KHCQN&th=1"
    # TARGET_URL = "https://www.amazon.com/dp/B0CG9HF9KW?ref=emc_p_m_5_i_atc"
    TARGET_URL = "https://www.amazon.com/dp/B09MLRPTT2?ref=emc_s_m_5_i_atc"
    
    scraper = Scraper()
    reviews = scraper.get_reviews(TARGET_URL)

    if reviews:
        print(f"\nProduct ASIN: {reviews['ASIN']}")
        print("Reviews:")
        for review in reviews["reviews"]:
            print(f"- Customer Name: {review['customer_name']}")
            print(f"- Review Date: {review['review_date']}")
            print(f"- Review Title: {review['review_title']}")
            print(f"- Review Text: {review['review_text']}\n")
            print("-" * 40)
    else:
        print("No reviews found.")