import json
import re
from datetime import datetime
import psycopg2
from psycopg2.extras import Json

# =====================================================
# CONFIG
# =====================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": "dost"
}

JSON_FILE = "ihg_hotels_output.json"
SOURCE = "ihg_scraper"
START_HOTEL_ID = 1500  # Start hotel code from 1500

# =====================================================
# HELPERS
# =====================================================

def safe_json(val):
    if not val:
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None

def parse_rating(val):
    try:
        return float(val)
    except Exception:
        return None

def parse_pet_policy(pet_text):
    """
    Extremely defensive parser. Never throws.
    """
    if not pet_text:
        return {
            "fee": None,
            "deposit": None,
            "currency": None,
            "interval": None,
            "pet_types": None,
            "weight_limit": None,
            "max_pets": None,
            "free_pets": False
        }

    text = pet_text.lower()

    # ---------- Free pets ----------
    if any(x in text for x in [
        "no extra charge",
        "no additional charge",
        "free of charge",
        "at no charge"
    ]):
        return {
            "fee": 0.0,
            "deposit": None,
            "currency": None,
            "interval": None,
            "pet_types": None,
            "weight_limit": None,
            "max_pets": None,
            "free_pets": True
        }

    # ---------- Currency ----------
    currency = None
    if "$" in pet_text:
        currency = "USD"

    # ---------- Numbers ----------
    numbers = []
    for m in re.finditer(r"(\d[\d,]*(?:\.\d+)?)", pet_text):
        try:
            numbers.append(float(m.group(1).replace(",", "")))
        except ValueError:
            pass

    fee = None
    deposit = None

    if "deposit" in text and numbers:
        deposit = numbers[0]
    elif "fee" in text and numbers:
        fee = numbers[-1]

    # ---------- Interval ----------
    interval = None
    if "per night" in text:
        interval = "per_night"
    elif "per stay" in text:
        interval = "per_stay"

    # ---------- Pet types ----------
    pet_types = []
    if "dog" in text:
        pet_types.append("dog")
    if "cat" in text:
        pet_types.append("cat")
    pet_types = ", ".join(pet_types) if pet_types else None

    # ---------- Weight ----------
    weight_limit = None
    w = re.search(r"(\d+\s?(kg|kgs|lb|lbs))", text)
    if w:
        weight_limit = w.group(1)

    # ---------- Max pets ----------
    max_pets = None
    m = re.search(r"up to\s*(\d+)", text)
    if m:
        max_pets = int(m.group(1))

    return {
        "fee": fee,
        "deposit": deposit,
        "currency": currency,
        "interval": interval,
        "pet_types": pet_types,
        "weight_limit": weight_limit,
        "max_pets": max_pets,
        "free_pets": False
    }

# =====================================================
# INGESTION
# =====================================================

def ingest():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open(JSON_FILE, "r", encoding="utf-8") as f:
        hotels = json.load(f)

    insert_sql = """
    INSERT INTO hotel_masterfile (
        hotel_code,
        chain_code,
        chain,
        name,
        full_address,
        phone_number,
        sabre_rating,
        description,
        parking,
        is_pet_friendly,
        pet_policy,
        pet_fee_total_max,
        pet_fee_deposit,
        pet_fee_currency,
        pet_fee_interval,
        allowed_pet_types,
        weight_limit,
        max_pets,
        has_pet_deposit,
        has_pet_friendly_rooms,
        pet_amenities,
        nearby_parks,
        source,
        last_updated
    )
    VALUES (
        %(hotel_code)s,
        %(chain_code)s,
        %(chain)s,
        %(name)s,
        %(address)s,
        %(phone)s,
        %(rating)s,
        %(description)s,
        %(parking)s,
        %(is_pet_friendly)s,
        %(pet_policy)s,
        %(pet_fee)s,
        %(pet_deposit)s,
        %(currency)s,
        %(interval)s,
        %(pet_types)s,
        %(weight_limit)s,
        %(max_pets)s,
        %(has_deposit)s,
        %(has_pet_rooms)s,
        %(amenities)s,
        %(nearby)s,
        %(source)s,
        %(updated)s
    )
    ON CONFLICT (hotel_code) DO UPDATE SET
        name = EXCLUDED.name,
        full_address = EXCLUDED.full_address,
        phone_number = EXCLUDED.phone_number,
        sabre_rating = EXCLUDED.sabre_rating,
        description = EXCLUDED.description,
        parking = EXCLUDED.parking,
        is_pet_friendly = EXCLUDED.is_pet_friendly,
        pet_policy = EXCLUDED.pet_policy,
        pet_fee_total_max = EXCLUDED.pet_fee_total_max,
        pet_fee_deposit = EXCLUDED.pet_fee_deposit,
        pet_fee_currency = EXCLUDED.pet_fee_currency,
        pet_fee_interval = EXCLUDED.pet_fee_interval,
        allowed_pet_types = EXCLUDED.allowed_pet_types,
        weight_limit = EXCLUDED.weight_limit,
        max_pets = EXCLUDED.max_pets,
        has_pet_deposit = EXCLUDED.has_pet_deposit,
        has_pet_friendly_rooms = EXCLUDED.has_pet_friendly_rooms,
        pet_amenities = EXCLUDED.pet_amenities,
        nearby_parks = EXCLUDED.nearby_parks,
        source = EXCLUDED.source,
        last_updated = EXCLUDED.last_updated;
    """

    hotel_code_counter = START_HOTEL_ID
    unique_codes_set = set()

    for h in hotels:
        # Ensure unique hotel_code
        while True:
            new_code = f"{hotel_code_counter}-{h.get('hotel_code', 'unknown')}"
            if new_code not in unique_codes_set:
                unique_codes_set.add(new_code)
                hotel_code_counter += 1
                break
            hotel_code_counter += 1

        pets = safe_json(h.get("pets_json"))
        pet_text = pets.get("policy") if pets else None
        pet_data = parse_pet_policy(pet_text)

        data = {
            "hotel_code": new_code,
            "chain_code": "IHG",
            "chain": "IHG",
            "name": h.get("hotel_name"),
            "address": h.get("address"),
            "phone": h.get("phone"),
            "rating": parse_rating(h.get("rating")),
            "parking": Json(safe_json(h.get("parking_json"))),
            "is_pet_friendly": str(h.get("is_pet_friendly")).lower() == "true",
            "pet_policy": pet_text,
            "pet_fee": pet_data["fee"],
            "pet_deposit": pet_data["deposit"],
            "currency": pet_data["currency"],
            "interval": pet_data["interval"],
            "pet_types": pet_data["pet_types"],
            "weight_limit": pet_data["weight_limit"],
            "max_pets": pet_data["max_pets"],
            "description": h.get("description"),
            "has_deposit": True if pet_data["deposit"] else False,
            "has_pet_rooms": True if pet_text else None,
            "amenities": Json(safe_json(h.get("amenities_json"))),
            "nearby": Json(safe_json(h.get("nearby_json"))),
            "source": SOURCE,
            "updated": datetime.fromisoformat(h.get("last_updated"))
        }

        cur.execute(insert_sql, data)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ IHG ingestion completed successfully with unique hotel codes")

# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":
    ingest()

