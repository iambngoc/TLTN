"""
CELLPHONES MOBILE CRAWLER
Trích xuất thông số kỹ thuật điện thoại từ CellphoneS và xuất dữ liệu ra file JSON.
"""

import json
import os
import re
import time
from abc import ABC, abstractmethod

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


# ----------------------- LỚP TRỪU TƯỢNG CƠ SỞ -----------------------
class AbstractCrawler(ABC):
    def __init__(self, platform_name):
        self.platform_name = platform_name
        self.browser = None
        self.wait_engine = None

    def validate_url(self, url_str):
        return bool(url_str)

    @abstractmethod
    def fetch_product_urls(self):
        pass

    @abstractmethod
    def extract_product_detail(self, target_url):
        pass


# ----------------------- TRÌNH CRAWL CELLPHONES -----------------------
class CellphoneSScraper(AbstractCrawler):
    def __init__(self):
        super().__init__("CellphoneS")
        # Ánh xạ các từ khóa nhận diện thông số kỹ thuật
        self.spec_mappings = {
            "Công nghệ màn hình": ["công nghệ màn hình", "màn hình"],
            "Cam sau": ["camera sau"],
            "Cam trước": ["camera trước"],
            "Chip": ["chip", "chipset", "cpu"],
            "Sim": ["sim"],
            "Hỗ trợ mạng": ["mạng", "hỗ trợ mạng"],
            "RAM": ["ram"],
            "ROM": ["bộ nhớ trong", "rom"],
            "Pin": ["pin"],
            "Hệ điều hành": ["hệ điều hành"],
            "Kháng nước bụi": ["kháng nước", "chống nước", "chỉ số ip", "ip", "chuẩn kháng nước"]
        }

    def validate_url(self, url_str):
        if not url_str:
            return False

        url_lower = url_str.lower()
        blacklisted_terms = ["bo-loc", "mobile/", "sforum", "tin-tuc"]
        valid_brands = [
            "dien-thoai", "iphone", "samsung", "xiaomi", "oppo",
            "tecno", "honor", "nubia", "sony", "nokia", "vivo",
            "realme", "oneplus"
        ]

        has_forbidden = any(term in url_lower for term in blacklisted_terms)
        has_keyword = any(brand in url_lower for brand in valid_brands)

        return (not has_forbidden) and has_keyword

    def scroll_and_expand_list(self, max_scroll_times=10):
        for index in range(max_scroll_times):
            try:
                self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                show_more_selector = "#blockFilterSort > div.filter-sort__list-product > div > div.cps-block-content_btn-showmore > a"
                load_more_btn = self.browser.find_element(By.CSS_SELECTOR, show_more_selector)

                self.browser.execute_script("arguments[0].click();", load_more_btn)
                time.sleep(2)
            except Exception:
                break

    def fetch_product_urls(self):
        print(f"[{self.platform_name}] Đang thu thập danh sách đường dẫn sản phẩm...")
        target_page = "https://cellphones.com.vn/mobile.html"
        self.browser.get(target_page)

        try:
            self.wait_engine.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.filter-sort__list-product"))
            )
        except TimeoutException:
            print("⛔ Không thể tải danh sách sản phẩm!")
            return []

        self.scroll_and_expand_list()

        collected_links = []
        try:
            product_container = self.browser.find_element(By.CSS_SELECTOR, "#blockFilterSort > div.filter-sort__list-product")
            anchors = product_container.find_elements(By.CSS_SELECTOR, ".product-info > a")

            for item in anchors:
                link = item.get_attribute("href")
                if link and self.validate_url(link):
                    if link not in collected_links:
                        collected_links.append(link)
        except Exception as err:
            print(f"❌ Xảy ra lỗi khi trích xuất liên kết: {err}")

        print(f"✓ Đã tìm thấy {len(collected_links)} sản phẩm")
        return collected_links

    def map_key_to_field(self, raw_key):
        key_clean = raw_key.lower().strip()
        for field, keywords in self.spec_mappings.items():
            if any(kw in key_clean for kw in keywords):
                return field
        return None

    def extract_product_detail(self, target_url):
        try:
            self.browser.get(target_url)
            self.wait_engine.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".box-product-name h1"))
            )
        except Exception:
            return None

        # Khởi tạo bản ghi với giá trị mặc định
        item_data = {
            "Tên sản phẩm": "Không tìm thấy",
            "Giá": "Không tìm thấy",
            "Công nghệ màn hình": "Không tìm thấy",
            "Cam sau": "Không tìm thấy",
            "Cam trước": "Không tìm thấy",
            "Chip": "Không tìm thấy",
            "Sim": "Không tìm thấy",
            "Hỗ trợ mạng": "Không tìm thấy",
            "RAM": "Không tìm thấy",
            "ROM": "Không tìm thấy",
            "Pin": "Không tìm thấy",
            "Hệ điều hành": "Không tìm thấy",
            "Kháng nước bụi": "Không tìm thấy",
            "URL": target_url,
            "Nguồn": self.platform_name
        }

        # Trích xuất tên thiết bị
        try:
            title_node = self.browser.find_element(By.CSS_SELECTOR, ".box-product-name h1")
            item_data["Tên sản phẩm"] = title_node.text.strip()
        except NoSuchElementException:
            pass

        # Trích xuất giá niêm yết
        try:
            price_node = self.browser.find_element(By.CSS_SELECTOR, ".sale-price")
            item_data["Giá"] = price_node.text.strip()
        except NoSuchElementException:
            pass

        # Mở popup/modal thông số kỹ thuật
        try:
            tech_buttons = self.browser.find_elements(By.CSS_SELECTOR, "button.button__show-modal-technical")
            for btn in tech_buttons:
                self.browser.execute_script("arguments[0].click();", btn)
                time.sleep(1)
        except Exception:
            pass

        # Đọc dữ liệu từ bảng thông số kỹ thuật (Cải tiến đọc trường Kháng nước bụi)
        table_rows = self.browser.find_elements(By.CSS_SELECTOR, "table.technical-content tr.technical-content-item")
        for row in table_rows:
            try:
                columns = row.find_elements(By.TAG_NAME, "td")
                if len(columns) < 2:
                    continue

                label_text = columns[0].text.strip()
                val_text = columns[1].text.strip()

                matched_field = self.map_key_to_field(label_text)
                if matched_field and val_text:
                    item_data[matched_field] = val_text
            except Exception:
                continue

        return item_data


# ----------------------- QUẢN LÝ TIẾN TRÌNH -----------------------
class ScraperController:
    def __init__(self, run_headless=False):
        self.is_headless = run_headless
        self.driver_instance = None
        self.scraper = CellphoneSScraper()
        self.dataset = []

    def initialize_driver(self):
        chrome_opts = webdriver.ChromeOptions()
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--window-size=1920,1080")
        if self.is_headless:
            chrome_opts.add_argument("--headless")

        service_driver = Service(ChromeDriverManager().install())
        self.driver_instance = webdriver.Chrome(service=service_driver, options=chrome_opts)

        self.scraper.browser = self.driver_instance
        self.scraper.wait_engine = WebDriverWait(self.driver_instance, 10)

    def execute_scraping(self, max_limit=None):
        product_urls = self.scraper.fetch_product_urls()

        if max_limit:
            product_urls = product_urls[:max_limit]

        for url in product_urls:
            product_info = self.scraper.extract_product_detail(url)
            if product_info:
                self.dataset.append(product_info)

        return len(product_urls)

    def export_to_json(self, output_path="unified_products2.json"):
        with open(output_path, "w", encoding="utf-8") as file_out:
            json.dump(self.dataset, file_out, ensure_ascii=False, indent=2)
        print(f"✓ Đã lưu thành công {len(self.dataset)} sản phẩm vào file '{output_path}'")

    def terminate_driver(self):
        if self.driver_instance:
            self.driver_instance.quit()


# ----------------------- ĐIỂM CHẠY CHÍNH -----------------------
def main():
    IS_HEADLESS_MODE = False
    LIMIT_PRODUCTS = 50

    controller = ScraperController(run_headless=IS_HEADLESS_MODE)
    controller.initialize_driver()

    try:
        controller.execute_scraping(max_limit=LIMIT_PRODUCTS)
        controller.export_to_json()
    finally:
        controller.terminate_driver()


if __name__ == "__main__":
    main()