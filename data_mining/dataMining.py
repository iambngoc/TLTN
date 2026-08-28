import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.cluster import KMeans

# ==========================================
# 1. CẤU HÌNH KẾT NỐI DATABASE
# ==========================================
DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'port': '3306',
    'db_name': 'datawh'
}


def get_engine():
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db_name']}?charset=utf8mb4"
    return create_engine(conn_str)


def run_mining():
    engine = get_engine()
    print("--- Bắt đầu quá trình Data Mining (Rút gọn) ---")

    try:
        with engine.connect() as conn:
            # ==========================================
            # 2. TIỀN XỬ LÝ DỮ LIỆU (SQL VIEW)
            # Chuyển đổi các cột văn bản sang số để tính toán
            # ==========================================
            print("1. Đang làm sạch dữ liệu...")
            sql_view = """
            CREATE OR REPLACE VIEW v_clean_product AS
            SELECT 
                product_id, ten_san_pham, brand, category,
                CAST(REPLACE(REPLACE(sale_price_vnd, 'đ', ''), '.', '') AS UNSIGNED) AS price_numeric,
                CAST(REGEXP_REPLACE(RAM, '[^0-9]', '') AS UNSIGNED) AS ram_gb,
                CAST(REGEXP_REPLACE(ROM, '[^0-9]', '') AS UNSIGNED) AS rom_gb
            FROM dim_product
            WHERE expiry_date IS NULL;
            """
            conn.execute(text(sql_view))
            conn.commit()

            # ==========================================
            # 3. MINING 1: GIÁ THEO THƯƠNG HIỆU
            # ==========================================
            print("2. Đang phân tích giá theo thương hiệu...")
            sql_brand = """
            SELECT 
                brand AS `brand_name`, 
                COUNT(*) AS `product_count`, 
                AVG(price_numeric) AS `average_price`, 
                MIN(price_numeric) AS `min_price`, 
                MAX(price_numeric) AS `max_price` 
            FROM v_clean_product 
            GROUP BY brand
            """
            df_brand = pd.read_sql(sql_brand, engine)
            df_brand.to_sql('mining_brand_price', engine, if_exists='replace', index=False)

            # ==========================================
            # 4. MINING 2: PHÂN CỤM SẢN PHẨM (K-MEANS)
            # ==========================================
            print("3. Đang chạy thuật toán K-Means phân cụm sản phẩm...")
            df_full = pd.read_sql("SELECT * FROM v_clean_product", engine)

            if not df_full.empty:
                # Lấy 3 thuộc tính: Giá, RAM, ROM để làm đặc trưng phân cụm
                features = df_full[['price_numeric', 'ram_gb', 'rom_gb']].fillna(0)

                # Chạy K-Means phân làm 4 nhóm
                kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
                df_full['cluster_id'] = kmeans.fit_predict(features)

                # Gán tên phân khúc dựa trên giá trung bình của cụm
                # Cụm có giá thấp nhất = Giá rẻ, cao nhất = Siêu sang
                avg_prices = df_full.groupby('cluster_id')['price_numeric'].mean().sort_values().index
                cluster_map = {
                    avg_prices[0]: "Phân khúc Giá rẻ",
                    avg_prices[1]: "Phân khúc Tầm trung",
                    avg_prices[2]: "Phân khúc Cao cấp",
                    avg_prices[3]: "Phân khúc Siêu sang"
                }
                df_full['cluster_name'] = df_full['cluster_id'].map(cluster_map)

                # Lưu kết quả vào bảng mining_product_cluster
                df_full.to_sql('mining_product_cluster', engine, if_exists='replace', index=False)
                print("--- Hoàn thành! Đã cập nhật bảng Thương hiệu và bảng Phân cụm ---")
            else:
                print("Lỗi: Không tìm thấy dữ liệu trong View để phân cụm.")

    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")


if __name__ == "__main__":
    run_mining()