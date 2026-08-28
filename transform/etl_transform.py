import json
import re
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Text as SQLText

# ============================================
# MYSQL CONNECTION
# ============================================
MYSQL_DB = "staging"  # Schema Data Warehouse
CONTROL_DB = "crawl_controller"  # Schema Log quy trình


def get_mysql_url():
    return f"mysql+pymysql://root:@localhost:3306/{MYSQL_DB}?charset=utf8mb4"


def create_mysql_engine():
    return create_engine(get_mysql_url(), pool_pre_ping=True)


def get_control_mysql_url():
    return f"mysql+pymysql://root:@localhost:3306/{CONTROL_DB}?charset=utf8mb4"


def create_control_engine():
    return create_engine(get_control_mysql_url(), pool_pre_ping=True)


# ============================================
# ETL LOG FUNCTIONS
# ============================================
def start_etl_log():
    engine = create_mysql_engine()
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_log (batch_id, source_table, target_table, status) 
                VALUES (:batch_id, 'general', 'stg_products,dim_product', 'running')
            """), {"batch_id": batch_id})
            res_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        print(f" Bắt đầu ETL batch: {batch_id} (ID: {res_id})")
        return res_id, batch_id
    except Exception as e:
        print(f" Không thể ghi log: {e}")
        return None, batch_id


def update_error_log(etl_id, error_msg):
    if not etl_id:
        return
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_log
            SET status='failed',
                end_time=NOW()
            WHERE etl_id = :id
        """), {"id": etl_id})
        print(f"   ⚠️  Lỗi ETL: {str(error_msg)[:200]}")


def update_success_log(etl_id, inserted_count, updated_count=0):
    if not etl_id:
        return
    engine = create_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_log
            SET status='success',
                records_inserted=:inserted,
                records_updated=:updated,
                end_time=NOW()
            WHERE etl_id = :id
        """), {"inserted": inserted_count, "updated": updated_count, "id": etl_id})
    print(f" Đã cập nhật Log: Success (Inserted: {inserted_count}, Updated: {updated_count})")


PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONTROL_PROCESS_METADATA = {
    "extract": {
        "name": "Extract",
        "description": "Trích xuất dữ liệu từ nguồn vào bảng general.",
        "order": 1,
    },
    "transform": {
        "name": "Transform",
        "description": "Chuẩn hóa dữ liệu trung gian trước khi load.",
        "order": 2,
    },
    "load_staging": {
        "name": "Load_Staging",
        "description": "Đưa dữ liệu chuẩn hóa vào stg_products.",
        "order": 3,
    },
    "load_dwh": {
        "name": "LoadDataWarehouse",
        "description": "Đồng bộ dữ liệu vào dim_product.",
        "order": 4,
    },
}


def resolve_simulated_datetime(simulated_date):
    if simulated_date is None:
        return None
    if isinstance(simulated_date, datetime):
        return simulated_date
    if isinstance(simulated_date, pd.Timestamp):
        return simulated_date.to_pydatetime()
    if isinstance(simulated_date, str):
        cleaned = simulated_date.strip()
        date_formats = [
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y %H:%M:%S",
            "%Y%m%d",
        ]
        for fmt in date_formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
    raise ValueError(f"Không thể chuyển đổi ngày giả lập: {simulated_date}.")


def normalize_date_key(value, fallback_dt=None):
    """Đảm bảo date_key luôn trả về VARCHAR(8) hợp lệ, không bao giờ là None/NaN/Empty."""
    if value is not None and not pd.isna(value):
        v_str = str(value).strip().replace(".0", "")
        if v_str.isdigit() and len(v_str) == 8:
            return v_str

    if fallback_dt and isinstance(fallback_dt, (datetime, pd.Timestamp)):
        return fallback_dt.strftime("%Y%m%d")

    return "19000101"


# ============================================
# EXTRACT
# ============================================
def extract_from_json(json_path=None):
    print("\n" + "=" * 60)
    print("BƯỚC 1: EXTRACT - Đọc dữ liệu từ file JSON")
    print("=" * 60)
    if json_path is None:
        json_path = PROJECT_ROOT / "crawed" / "unified_products2.json"
    else:
        json_path = Path(json_path)
        if not json_path.is_absolute():
            json_path = (PROJECT_ROOT / json_path).resolve()

    try:
        print(f"   → File: {json_path}")
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            records = data.get("data")
            if records is None:
                for v in data.values():
                    if isinstance(v, list):
                        records = v
                        break
        else:
            records = data

        df = pd.DataFrame(records)
        print(f" Đã đọc {len(df)} dòng từ file {json_path}")
        return df
    except Exception as e:
        print(f" Lỗi khi đọc dữ liệu từ JSON: {e}")
        raise


def load_raw_json_to_general(json_path=None):
    print("\n" + "=" * 60)
    print("BƯỚC 0: LOAD RAW - Nạp dữ liệu JSON thô vào bảng general")
    print("=" * 60)
    df_raw = extract_from_json(json_path)
    if df_raw.empty:
        print(" ⚠️ File JSON không có dữ liệu, bỏ qua.")
        return 0

    df_text = df_raw.copy()
    for col in df_text.columns:
        df_text[col] = df_text[col].apply(
            lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
        )

    engine = create_mysql_engine()
    dtype_map = {col: SQLText() for col in df_text.columns}

    try:
        df_text.to_sql('general', engine, if_exists='replace', index=False, dtype=dtype_map, chunksize=1000)
        print(f" ✅ Đã nạp {len(df_text)} dòng vào bảng general")
        return len(df_text)
    except Exception as e:
        print(f" ❌ Lỗi khi nạp dữ liệu vào general: {e}")
        raise


def extract_from_general():
    print("\n" + "=" * 60)
    print("BƯỚC 1: EXTRACT - Đọc dữ liệu từ bảng general")
    print("=" * 60)
    engine = create_mysql_engine()
    try:
        query = "SELECT * FROM general"
        df = pd.read_sql(query, engine)
        print(f" Đã đọc {len(df)} dòng từ bảng general")
        return df
    except Exception as e:
        print(f" Lỗi khi đọc dữ liệu: {e}")
        raise

def get_date_sk(crawl_dt):
    """
    Tìm date_sk trong datawh.date_dims dựa trên full_date.
    """

    engine = create_engine(
        "mysql+pymysql://root:@localhost:3306/datawh?charset=utf8mb4",
        pool_pre_ping=True
    )

    try:
        query = text("""
            SELECT date_sk
            FROM date_dims
            WHERE full_date = :full_date
            LIMIT 1
        """)

        with engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    "full_date": crawl_dt.date()
                }
            ).fetchone()

        if result is None:
            raise ValueError(
                f"Không tìm thấy date_sk cho ngày "
                f"{crawl_dt.strftime('%Y-%m-%d')} "
                f"trong bảng datawh.date_dims."
            )

        date_sk = result[0]

        print(
            f" 📅 full_date = {crawl_dt.strftime('%Y-%m-%d')}"
            f" → date_sk = {date_sk}"
        )

        return date_sk

    finally:
        engine.dispose()


# ============================================
# TRANSFORM
# ============================================
def transform_data(df, simulated_date=None):
    print("\n" + "=" * 60)
    print("BƯỚC 2: TRANSFORM - Làm sạch và chuẩn hóa dữ liệu")
    print("=" * 60)
    df = df.copy()
    crawl_dt = resolve_simulated_datetime(simulated_date) if simulated_date else datetime.now()

    # Lọc dữ liệu rác
    initial_count = len(df)
    if 'Tên sản phẩm' in df.columns:
        df = df.dropna(subset=['Tên sản phẩm'])
        df = df[df['Tên sản phẩm'] != 'Không tìm thấy']
        df = df[df['Tên sản phẩm'].astype(str).str.strip() != '']
    print(f" 🔍 Loại bỏ {initial_count - len(df)} dòng dữ liệu rác")

    # Mapping cột khớp với DB
    rename_mapping = {
        'Tên sản phẩm': 'ten_san_pham',
        'Giá': 'sale_price_vnd',
        'Nguồn': 'nguon'
    }
    df.rename(columns=rename_mapping, inplace=True)

    # Trích xuất Brand
    brands_dict = {
        'IPHONE': 'Apple', 'SAMSUNG': 'Samsung', 'XIAOMI': 'Xiaomi', 'OPPO': 'Oppo',
        'REALME': 'Realme', 'VIVO': 'Vivo', 'NOKIA': 'Nokia', 'TECNO': 'Tecno',
        'HONOR': 'Honor', 'SONY': 'Sony', 'ASUS': 'Asus', 'INFINIX': 'Infinix',
        'POCO': 'Xiaomi', 'NOTHING': 'Nothing', 'NUBIA': 'Nubia', 'GOOGLE': 'Google'
    }

    def extract_brand(name):
        if pd.isna(name) or str(name).strip() == '':
            return 'Other'
        n = str(name).upper()
        for k, v in brands_dict.items():
            if k in n:
                return v
        return 'Other'

    df['brand'] = df['ten_san_pham'].apply(extract_brand)

    # Categorize
    def categorize(name):
        if pd.isna(name) or str(name).strip() == '':
            return 'Smartphone'
        n = str(name).upper()
        if any(x in n for x in ['FOLD', 'FLIP', 'GALAXY Z']):
            return 'Foldable'
        if 'TAB' in n or 'IPAD' in n:
            return 'Tablet'
        return 'Smartphone'

    df['category'] = df['ten_san_pham'].apply(categorize)

    # Metadata thời gian & DATE KEY
    df['ngay_crawl'] = crawl_dt

    # Lấy date_sk từ datawh.date_dims dựa trên full_date
    date_sk = get_date_sk(crawl_dt)

    df['date_key'] = date_sk

    print(
        f" 📅 ngay_crawl = {crawl_dt.strftime('%Y-%m-%d')}"
        f" → date_key = {date_sk}"
    )

    df['nguon'] = df['nguon'].fillna('CellphoneS').astype(str).str.strip()

    # Chuẩn hóa giá trị các cột Text
    for col in df.columns:
        if col not in ['brand', 'category', 'ngay_crawl', 'date_key']:
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaT', '<NA>'], None)
            df[col] = df[col].apply(lambda x: None if x is None or str(x).strip() in ['', 'nan', 'None'] else x)

    # Bỏ cột thừa không dùng
    for col in ['id', 'created_at']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if 'URL' in df.columns:
        cols = ['URL'] + [c for c in df.columns if c != 'URL']
        df = df[cols]

    print(f" ✅ Dữ liệu đã làm sạch. Tổng dòng: {len(df)}")
    return df


# ============================================
# LOAD TO STAGING
# ============================================
def load_to_staging(df):
    print("\n" + "=" * 60)
    print("BƯỚC 3: LOAD - Nạp dữ liệu vào stg_products")
    print("=" * 60)
    engine = create_mysql_engine()
    try:
        df_to_load = df.copy()
        if 'ngay_crawl' in df_to_load.columns:
            df_to_load['ngay_crawl'] = pd.to_datetime(df_to_load['ngay_crawl'], errors='coerce')

        df_to_load.to_sql('stg_products', engine, if_exists='replace', index=False, chunksize=1000)
        print(f" ✅ Đã load {len(df)} dòng vào bảng 'stg_products'")
        return len(df)
    except Exception as e:
        print(f" ❌ Lỗi load vào staging: {e}")
        raise


# ============================================
# LOAD TO DIMENSION TABLE (ĐÃ FIX PRIMARY KEY LỖI DUPLICATE 0-)
# ============================================
def load_to_dim():
    print("\n" + "=" * 60)
    print("BƯỚC 4: LOAD - Nạp dữ liệu vào dim_product")
    print("=" * 60)
    engine = create_mysql_engine()

    # 1. Đọc dữ liệu từ stg_products
    stg_df = pd.read_sql("SELECT * FROM stg_products", engine)
    if stg_df.empty:
        print(" ⚠️ stg_products đang trống, bỏ qua load dim_product.")
        return 0, 0

    # 2. Xử lý chuẩn hóa cột date_key & ngay_crawl
    stg_df['ngay_crawl'] = pd.to_datetime(stg_df['ngay_crawl'], errors='coerce')
    stg_df['date_key'] = stg_df.apply(
        lambda row: normalize_date_key(row.get('date_key'), row.get('ngay_crawl')),
        axis=1
    )

    # 3. Lấy product_id lớn nhất hiện tại trong dim_product để tự sinh Auto-Increment trên Python
    try:
        max_id_df = pd.read_sql("SELECT MAX(product_id) as max_id FROM dim_product", engine)
        max_id = max_id_df['max_id'].iloc[0]
        max_id = int(max_id) if pd.notnull(max_id) else 0
    except Exception:
        max_id = 0

    print(f" ℹ️ product_id lớn nhất hiện tại trong dim_product: {max_id}")

    # 4. Lấy danh sách cột thực tế của bảng dim_product trong DB
    db_cols_df = pd.read_sql("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dim_product'
    """, engine)
    db_columns = db_cols_df['COLUMN_NAME'].tolist()

    # 5. Gán ID tự tăng và lọc đúng danh sách cột trùng với Schema DB
    df_insert = stg_df.copy()
    df_insert['product_id'] = range(max_id + 1, max_id + 1 + len(df_insert))

    # Giữ lại các cột khớp với DB Schema
    insert_cols = [c for c in db_columns if c in df_insert.columns]
    df_insert = df_insert[insert_cols]

    # 6. Insert dữ liệu vào MySQL
    try:
        with engine.begin() as conn:
            df_insert.to_sql(
                'dim_product',
                con=conn,
                if_exists='append',
                index=False,
                chunksize=500
            )
        inserted_count = len(df_insert)
        print(f" ✅ Đã nạp thành công {inserted_count} bản ghi mới vào dim_product "
              f"(product_id từ {max_id + 1} đến {max_id + inserted_count})")
        return inserted_count, 0
    except Exception as e:
        print(f" ❌ Lỗi khi insert vào dim_product: {e}")
        raise


# ============================================
# MAIN PIPELINE RUNNER
# ============================================
if __name__ == "__main__":
    etl_id, batch_id = start_etl_log()
    try:
        # 1. Load Raw JSON
        load_raw_json_to_general()

        # 2. Extract
        df_raw = extract_from_general()

        # 3. Transform
        df_transformed = transform_data(df_raw)

        # 4. Load Staging
        load_to_staging(df_transformed)

        # 5. Load Data Warehouse (dim_product)
        inserted, updated = load_to_dim()

        # Update success log
        update_success_log(etl_id, inserted, updated)
        print("\n🎉 HOÀN THÀNH ETL PIPELINE THÀNH CÔNG!")
    except Exception as err:
        update_error_log(etl_id, err)
        print(f"\n❌ CHƯƠNG TRÌNH DỪNG LẠI CÓ LỖI: {err}")