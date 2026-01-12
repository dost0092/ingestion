import pandas as pd
import psycopg2
import re
from psycopg2.extras import execute_batch
import json
from rapidfuzz import fuzz

# =====================================================
# CONFIG
# =====================================================
EXCEL_FILE = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": "dost"
}

# =====================================================
# HELPERS
# =====================================================
def normalize_name(name: str) -> str:
    """Normalize names for comparison."""
    if not name:
        return None
    name = str(name)
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()

def normalize_hotel_code(code):
    """
    Normalize hotel_code:
    - Remove .0 if it's a float like 100207330.0
    - Convert to string
    """
    if code is None:
        return None
    if isinstance(code, float) and code.is_integer():
        code = int(code)
    return str(code)

def safe_numeric(value, max_val=None):
    """Convert to float and clip to max_val if provided."""
    if pd.isnull(value):
        return None
    try:
        value = float(value)
        if max_val is not None and value > max_val:
            return max_val
        return value
    except:
        return None

def safe_int(value, max_val=None):
    """Convert to int and clip to max_val if provided."""
    if pd.isnull(value):
        return None
    try:
        value = int(value)
        if max_val is not None and value > max_val:
            return max_val
        return value
    except:
        return None

# =====================================================
# LOAD EXCEL
# =====================================================
print("📄 Loading Excel file...")
df_excel = pd.read_excel(EXCEL_FILE)
df_excel = df_excel.where(pd.notnull(df_excel), None)
df_excel['normalized_name'] = df_excel['Global Property Name'].apply(normalize_name)
df_excel['Global Property ID'] = df_excel['Global Property ID'].apply(normalize_hotel_code)  # Normalize hotel codes

# =====================================================
# CONNECT TO DATABASE
# =====================================================
print("🔌 Connecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# =====================================================
# FETCH MASTERFILE DATA
# =====================================================
print("📥 Fetching MASTERFILE data...")
cur.execute("SELECT * FROM web_scraped_hotels;")
master_cols = [desc[0] for desc in cur.description]
master_data = cur.fetchall()
df_master = pd.DataFrame(master_data, columns=master_cols)
df_master['normalized_name'] = df_master['name'].apply(normalize_name)
df_master['hotel_code'] = df_master['hotel_code'].apply(normalize_hotel_code)  # Normalize hotel codes

print(f"🔎 Loaded {len(df_master)} MASTERFILE rows")
print(f"🔎 Loaded {len(df_excel)} Excel rows")

# =====================================================
# ENHANCED MERGE — FUZZY MATCH + COUNTRY + CHAIN BOOST
# =====================================================
print("🤖 Performing fuzzy match (≥80% threshold)...")

excel_records = df_excel.to_dict(orient="records")
matched_rows = []
unmatched_rows = []

for _, master_row in df_master.iterrows():
    master_name = master_row['normalized_name']
    master_country = master_row.get('country_code', None)
    if not master_name:
        continue

    candidates = (
        [r for r in excel_records if r.get('Property Country Code') == master_country]
        if master_country else excel_records
    )

    best_match = None
    best_score = 0

    # Fuzzy name matching
    for excel_row in candidates:
        excel_name = excel_row['normalized_name']
        if not excel_name:
            continue

        score = fuzz.token_sort_ratio(master_name, excel_name)

        # Add bonus for HY or HI chain codes
        chain_code = excel_row.get('Global Chain Code')
        if chain_code in ('HY', 'HI'):
            score += 5

        if score > best_score:
            best_score = score
            best_match = excel_row

    if best_score >= 80 and best_match:
        merged = {**master_row.to_dict(), **{f"{k}_excel": v for k, v in best_match.items()}}
        merged["match_score"] = best_score
        matched_rows.append(merged)
    else:
        unmatched_rows.append(master_row.to_dict())

df_merged = pd.DataFrame(matched_rows)
print(f"✅ Fuzzy matched {len(df_merged)} / {len(df_master)} MASTERFILE records")

# =====================================================
# PREPARE RECORDS FOR INSERT
# =====================================================
records = []
for _, row in df_merged.iterrows():
    record = {
        "hotel_code": normalize_hotel_code(row.get('Global Property ID_excel')) if pd.notnull(row.get('Global Property ID_excel')) else normalize_hotel_code(row['hotel_code']),
        "chain_code": row.get('Global Chain Code_excel') if pd.notnull(row.get('Global Chain Code_excel')) else row['chain_code'],
        "chain": row['chain'],
        "name": row.get('Global Property Name_excel') if pd.notnull(row.get('Global Property Name_excel')) else row['name'],
        "state_code": row.get('Property State/Province_excel') if pd.notnull(row.get('Property State/Province_excel')) else row['state_code'],
        "state": row['state'],
        "country_code": row.get('Property Country Code_excel') if pd.notnull(row.get('Property Country Code_excel')) else row['country_code'],
        "country": row['country'],
        "city": row.get('Property City Name_excel') if pd.notnull(row.get('Property City Name_excel')) else row['city'],
        "postal_code": row.get('Property Zip/Postal_excel') if pd.notnull(row.get('Property Zip/Postal_excel')) else row['postal_code'],
        "address_line_1": row.get('Property Address 1_excel') if pd.notnull(row.get('Property Address 1_excel')) else row['address_line_1'],
        "address_line_2": row.get('Property Address 2_excel') if pd.notnull(row.get('Property Address 2_excel')) else row['address_line_2'],
        "full_address": row['full_address'],
        "latitude": safe_numeric(row.get('Property Latitude_excel')) if pd.notnull(row.get('Property Latitude_excel')) else safe_numeric(row['latitude']),
        "longitude": safe_numeric(row.get('Property Longitude_excel')) if pd.notnull(row.get('Property Longitude_excel')) else safe_numeric(row['longitude']),
        "primary_airport_code": row.get('Primary Airport Code_excel') if pd.notnull(row.get('Primary Airport Code_excel')) else row['primary_airport_code'],
        "property_quality_type": row['property_quality_type'],
        "property_style_description": row['description'] if pd.notnull(row.get('description')) else row['property_style_description'],
        "sabre_rating": safe_numeric(row.get('Sabre Property Rating_excel'), max_val=99.9) if pd.notnull(row.get('Sabre Property Rating_excel')) else safe_numeric(row.get('sabre_rating'), max_val=99.9),
        "sabre_context": row['sabre_context'],
        "parking": row['parking'],
        "links": row['links'],
        "phone_number": row.get('Property Phone Number_excel') if pd.notnull(row.get('Property Phone Number_excel')) else row['phone_number'],
        "fax_number": row.get('Property Fax Number_excel') if pd.notnull(row.get('Property Fax Number_excel')) else row['fax_number'],
        "is_verified": row['is_verified'],
        "verification_type": row['verification_type'],
        "is_pet_friendly": row['is_pet_friendly'],
        "pet_policy": row['pet_policy'],
        "service_animal_policy": row['service_animal_policy'],
        "pet_fee_night": safe_numeric(row['pet_fee_night']),
        "pet_fee_total_max": safe_numeric(row['pet_fee_total_max']),
        "pet_fee_deposit": safe_numeric(row['pet_fee_deposit']),
        "pet_fee_currency": row['pet_fee_currency'],
        "pet_fee_interval": row['pet_fee_interval'],
        "pet_fee_variations": row['pet_fee_variations'],
        "has_pet_deposit": row['has_pet_deposit'],
        "is_deposit_refundable": row['is_deposit_refundable'],
        "has_extra_fee_info": row['has_extra_fee_info'],
        "allowed_pet_types": row['allowed_pet_types'],
        "weight_limit": row['weight_limit'],
        "has_extra_weight_info": row['has_extra_weight_info'],
        "has_pet_friendly_rooms": row['has_pet_friendly_rooms'],
        "max_pets": safe_int(row['max_pets'], max_val=150),
        "has_max_pets_extra_info": row['has_max_pets_extra_info'],
        "breed_restrictions": row['breed_restrictions'],
        "pet_amenities": row['pet_amenities'],
        "has_pet_amenities": row['has_pet_amenities'],
        "nearby_parks": row['nearby_parks'],
        "parks_distance_miles": safe_numeric(row['parks_distance_miles'], max_val=999.99),
        "contact_note": row['contact_note'],
        "followup": row['followup'],
        "source": 'CSL_EXCEL_SCRAPING_MAPPED' if pd.notnull(row.get('Global Property Name_excel')) else 'MASTERFILE',
        "created_at": row['created_at'],
        "updated_at": row['updated_at'],
        "last_updated": row['last_updated'],
        "description": row['description']
    }
    records.append(record)

# =====================================================
# CONVERT JSON-LIKE FIELDS TO STRINGS
# =====================================================
for record in records:
    for field in ['links', 'pet_fee_variations', 'pet_amenities']:
        if record.get(field) is not None:
            record[field] = json.dumps(record[field])

# =====================================================
# INSERT INTO DATABASE
# =====================================================
if records:
    insert_cols = list(records[0].keys())
    insert_sql = f"""
    INSERT INTO web_scraped_hotels ({', '.join(insert_cols)})
    VALUES ({', '.join([f'%({c})s' for c in insert_cols])})
    ON CONFLICT (hotel_code) DO UPDATE SET
    chain_code = EXCLUDED.chain_code,
    name = EXCLUDED.name,
    state_code = EXCLUDED.state_code,
    country_code = EXCLUDED.country_code,
    city = EXCLUDED.city,
    postal_code = EXCLUDED.postal_code,
    address_line_1 = EXCLUDED.address_line_1,
    address_line_2 = EXCLUDED.address_line_2,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    phone_number = EXCLUDED.phone_number,
    source = 'CSL_EXCEL_SCRAPING_MAPPED',
    updated_at = NOW();
    """

    print("💾 Inserting merged records into database...")
    execute_batch(cur, insert_sql, records, page_size=50)
    conn.commit()
    print(f"✅ SUCCESS: {len(records)} hotels inserted into web_scraped_hotels")
else:
    print("⚠️ No matched records found — nothing inserted.")

cur.close()
conn.close()
