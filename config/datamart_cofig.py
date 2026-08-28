# ============================================================
# FILE: config_datamart.py
# MỤC ĐÍCH: Chứa cấu hình và hàm connect DB cho Data Mart ETL
# ============================================================

import pymysql
from typing import Optional, Dict, Any

# =========================
# ⚙️ CẤU HÌNH
# =========================
CONFIG: Dict[str, Any] = {
    "DB_USER": "root",
    "DB_PASS": "",
    "DB_HOST": "localhost",
    "DB_PORT": 3306,

    # DB Names
    "DWH_DB_NAME": "datawh",
    "DATA_MART_DB": "data_mart_prod",
    "CONTROL_DB": "crawl_controller",

    # Bảng nguồn
    "SOURCE_TABLE": "stg_products"
}

# =========================
# Hàm kết nối DB
# =========================
def connect_db(db_name: Optional[str] = None):
    """
    Tạo kết nối tới cơ sở dữ liệu MySQL.
    Nếu db_name=None → kết nối không chọn DB.
    """
    return pymysql.connect(
        host=CONFIG["DB_HOST"],
        user=CONFIG["DB_USER"],
        password=CONFIG["DB_PASS"],
        database=db_name,
        port=CONFIG["DB_PORT"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# =========================
# 🔹 DIMENSION MAPPINGS: DWH column → Data Mart column
# =========================
DIMENSION_MAPPINGS = {
    'dim_brand': {
        'brand_key': 'brand_key',
        'brand_name': 'brand_name'
    },

    'date_dims': {
        'date_sk': 'date_sk',
        'full_date': 'full_date',
        'day_since_2026': 'day_since_2026',
        'month_since_2026': 'month_since_2026',
        'day_of_week': 'day_of_week',
        'calendar_month': 'calendar_month',
        'calendar_year': 'calendar_year',
        'calendar_year_month': 'calendar_year_month',
        'day_of_month': 'day_of_month',
        'day_of_year': 'day_of_year',
        'week_of_year_sunday': 'week_of_year_sunday',
        'year_week_sunday': 'year_week_sunday',
        'week_sunday_start': 'week_sunday_start',
        'week_of_year_monday': 'week_of_year_monday',
        'year_week_monday': 'year_week_monday',
        'week_monday_start': 'week_monday_start',
        'holiday': 'holiday',
        'day_type': 'day_type'
    },

    'dim_product': {
        'product_id': 'product_id',
        'ten_san_pham': 'product_name',
        'brand_key': 'brand_key',
        'category': 'category',
        'sale_price_vnd': 'price',
        'Chip': 'cpu',
        'RAM': 'ram',
        'ROM': 'storage',
        'Hệ điều hành': 'os',
        'Công nghệ màn hình': 'screen_size',
        'Pin': 'battery',
        'ngay_crawl': 'date_collected',
        'date_key': 'date_key',
        'expiry_date': 'expired_dt'
    }
}
