import os
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

INT4_MIN = -2147483648
INT4_MAX = 2147483647
NULL_STRINGS = {"", "nan", "NaN", "None", "null", "NULL"}

# =====================================================
# DATABASE CONNECTIONS
# =====================================================
LOCAL_DB = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": os.getenv("LOCAL_DB_PASSWORD", "dost"),
}

GCP_DB = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# =====================================================
# SAFE UTILITIES
# =====================================================
def is_nullish(v):
    return v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and v.strip() in NULL_STRINGS)

def safe_text(v):
    return None if is_nullish(v) else str(v).strip()

def safe_float(v):
    try:
        return None if is_nullish(v) else float(str(v).replace(",", ""))
    except Exception:
        return None

def safe_int4(v):
    try:
        if is_nullish(v):
            return None
        iv = int(float(v))
        return iv if INT4_MIN <= iv <= INT4_MAX else None
    except Exception:
        return None

def safe_bool(v):
    if is_nullish(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "y"}
    return None

def safe_json_text(v):
    if is_nullish(v):
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    try:
        json.loads(v)
        return v
    except Exception:
        return json.dumps(str(v), ensure_ascii=False)

def normalize_phone(v):
    v = safe_text(v)
    if not v:
        return None
    return "".join(c for c in v if c in "0123456789+ -()")

# =====================================================
# FETCH TARGET SCHEMA
# =====================================================
def get_target_schema():
    conn = psycopg2.connect(**GCP_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='ingestion'
          AND table_name='web_scraped_hotel'
        ORDER BY ordinal_position;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # exclude id
    return [(n, t) for n, t in rows if n != "id"]

# =====================================================
# COERCION
# =====================================================
def coerce(df, schema):
    json_cols = {"links", "pet_amenities", "pet_fee_variations"}
    for col, dtype in schema:
        if col not in df.columns:
            continue
        if col == "phone_number":
            df[col] = df[col].apply(normalize_phone)
        elif col in json_cols:
            df[col] = df[col].apply(safe_json_text)
        elif dtype in ("text", "character varying"):
            df[col] = df[col].apply(safe_text)
        elif dtype == "boolean":
            df[col] = df[col].apply(safe_bool)
        elif dtype == "integer":
            if col == "max_pets":
                df[col] = df[col].apply(safe_int4).astype("Int64")
            else:
                df[col] = df[col].apply(safe_int4)
        elif dtype in ("double precision", "numeric"):
            df[col] = df[col].apply(safe_float)
        elif "timestamp" in dtype:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.where(pd.notnull(df), None)

def detect_int_overflows(df):
    offenders = df[df["max_pets"].notna() & ((df["max_pets"] < INT4_MIN) | (df["max_pets"] > INT4_MAX))]
    if not offenders.empty:
        print("🚨 INT4 OVERFLOW DETECTED IN max_pets")
        print(offenders[["hotel_code", "max_pets"]].head(10))
        df.loc[offenders.index, "max_pets"] = None
    return df

# =====================================================
# MAIN ETL
# =====================================================
def run_etl():
    print("🔌 Loading local web_scraped_hotels...")
    engine = create_engine(
        f"postgresql+psycopg2://{LOCAL_DB['user']}:{LOCAL_DB['password']}@"
        f"{LOCAL_DB['host']}:{LOCAL_DB['port']}/{LOCAL_DB['dbname']}"
    )

    df = pd.read_sql(
        """SELECT * FROM public.web_scraped_hotels """,
        engine
    )
    print(f"📦 Loaded {len(df)} rows from local web_scraped_hotels")

    print("🧭 Fetching target schema...")
    schema = get_target_schema()
    target_cols = [c for c, _ in schema]

    df = df[[c for c in target_cols if c in df.columns]]
    df = coerce(df, schema)
    df = detect_int_overflows(df)
    df = df.astype(object).where(pd.notna(df), None)

    print("⚠️ Truncating existing GCP ingestion.web_scraped_hotel table...")
    conn = psycopg2.connect(**GCP_DB)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE ingestion.web_scraped_hotel;")
    conn.commit()

    print("☁️ Inserting new data into GCP...")
    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO ingestion.web_scraped_hotel ({cols}) VALUES ({placeholders})"

    try:
        execute_batch(cur, sql, df.values.tolist(), page_size=500)
        conn.commit()
        print(f"✅ ETL completed successfully! Inserted {len(df)} hotels 🎉")
    except Exception as e:
        conn.rollback()
        print("❌ Insert failed:", e)
        raise
    finally:
        cur.close()
        conn.close()

# =====================================================
if __name__ == "__main__":
    run_etl()
