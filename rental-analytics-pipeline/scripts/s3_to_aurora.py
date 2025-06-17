import csv
import boto3
import pymysql

# ---------- Configuration ----------
BUCKET_NAME = "s3-to-aurora-001"
S3_KEY = "apartment_attributes.csv"
LOCAL_PATH = "/tmp/apartment_attributes.csv"

DB_HOST = "database-1.cluster-cbge0k4ia2u6.eu-north-1.rds.amazonaws.com"
DB_NAME = "rental_marketplace"
DB_USER = "admin"
DB_PASSWORD = "*29gm.cn._._[kQG5gomaIzI206o"
DB_PORT = 3306

TABLE_NAME = "apartment_attributes"

# ---------- Step 1: Download CSV from S3 ----------
print("📥 Downloading file from S3...")
s3 = boto3.client('s3')
s3.download_file(BUCKET_NAME, S3_KEY, LOCAL_PATH)
print("✅ Download complete!")

# ---------- Step 2: Connect to Aurora ----------
print("🔗 Connecting to Aurora...")
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    db=DB_NAME,
    port=DB_PORT,
    connect_timeout=10
)
cursor = conn.cursor()
print("✅ Connected!")

# ---------- Step 3: Prepare SQL ----------
INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    id, category, body, amenities, bathrooms, bedrooms, fee,
    has_photo, pets_allowed, price_display, price_type,
    square_feet, address, cityname, state, latitude, longitude
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# ---------- Step 4: Insert Data ----------
print(f"🚀 Inserting data into {TABLE_NAME}...")
with open(LOCAL_PATH, 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    header = next(reader)  # Skip header row

    inserted = 0
    for row in reader:
        if len(row) != 17:
            print("❌ Skipping malformed row:", row)
            continue
        try:
            cursor.execute(INSERT_SQL.replace("INSERT INTO", "INSERT IGNORE INTO", 1), row)
            inserted += 1
        except Exception as e:
            print("⚠️ Error inserting row:", row)
            print(e)

conn.commit()
print(f"✅ Done! {inserted} rows inserted into {TABLE_NAME}.")

# ---------- Cleanup ----------
cursor.close()
conn.close()
print("🔒 Connection closed.")
