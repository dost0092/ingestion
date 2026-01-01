
# import json
# import re
# from datetime import datetime
# import psycopg2
# from psycopg2.extras import Json

# # =====================================================
# # DATABASE CONFIG
# # =====================================================
# DB_CONFIG = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "kruiz-dev-sql",
#     "user": "postgres",
#     "password": "dost"
# }

# JSON_FILE = "hilton_pet_friendly_hotels.json"
# SOURCE_NAME = "hilton_hotels"

# # =====================================================
# # HELPERS
# # =====================================================

# def safe_json(val):
#     if not val:
#         return None
#     if isinstance(val, (dict, list)):
#         return val
#     try:
#         return json.loads(val)
#     except Exception:
#         return None


# def parse_rating(text):
#     if not text:
#         return None
#     m = re.search(r"([\d.]+)", text)
#     return float(m.group(1)) if m else None


# def parse_pet_policy(pet_text):
#     if not pet_text:
#         return {
#             "fee": None,
#             "deposit": None,
#             "currency": None,
#             "interval": None,
#             "pet_types": None,
#             "weight_limit": None,
#             "max_pets": None
#         }

#     text = pet_text.lower()

#     # ---------- Currency ----------
#     currency = None
#     if any(x in pet_text for x in ["¥", "￥", "RMB", "Yuan"]):
#         currency = "CNY"
#     elif "NZ$" in pet_text:
#         currency = "NZD"
#     elif "PHP" in pet_text or "P " in pet_text:
#         currency = "PHP"
#     elif "JPY" in pet_text:
#         currency = "JPY"

#     # ---------- Extract all numbers ----------
#     amounts = []
#     for m in re.finditer(r"(\d[\d,]*(?:\.\d+)?)", pet_text):
#         try:
#             amounts.append(float(m.group(1).replace(",", "")))
#         except ValueError:
#             pass

#     fee = None
#     deposit = None

#     if "deposit" in text and amounts:
#         deposit = amounts[0]
#         if len(amounts) > 1:
#             fee = amounts[-1]
#     elif "fee" in text and amounts:
#         fee = amounts[-1]

#     # ---------- Interval ----------
#     interval = None
#     if "per stay" in text:
#         interval = "per_stay"
#     elif "per night" in text:
#         interval = "per_night"

#     # ---------- Pet types ----------
#     pet_types = []
#     if "dog" in text:
#         pet_types.append("dog")
#     if "cat" in text:
#         pet_types.append("cat")
#     pet_types = ", ".join(pet_types) if pet_types else None

#     # ---------- Weight limit ----------
#     weight_limit = None
#     w = re.search(r"(\d+\s?(kg|kgs|lb|lbs))", text)
#     if w:
#         weight_limit = w.group(1)

#     # ---------- Max pets ----------
#     max_pets = None
#     m = re.search(r"up to\s*(\d+)\s*(pets|cats|dogs)", text)
#     if m:
#         max_pets = int(m.group(1))

#     return {
#         "fee": fee,
#         "deposit": deposit,
#         "currency": currency,
#         "interval": interval,
#         "pet_types": pet_types,
#         "weight_limit": weight_limit,
#         "max_pets": max_pets
#     }

# # =====================================================
# # INGESTION
# # =====================================================

# def ingest():
#     conn = psycopg2.connect(**DB_CONFIG)
#     cur = conn.cursor()

#     with open(JSON_FILE, "r", encoding="utf-8") as f:
#         hotels = json.load(f)

#     insert_sql = """
#     INSERT INTO hotel_masterfile (
#         hotel_code,
#         chain_code,
#         chain,
#         name,
#         full_address,
#         phone_number,
#         sabre_rating,
#         parking,
#         is_pet_friendly,
#         pet_policy,
#         pet_fee_total_max,
#         pet_fee_deposit,
#         pet_fee_currency,
#         pet_fee_interval,
#         allowed_pet_types,
#         weight_limit,
#         max_pets,
#         has_pet_deposit,
#         has_pet_friendly_rooms,
#         pet_amenities,
#         nearby_parks,
#         source,
#         last_updated
#     )
#     VALUES (
#         %(hotel_code)s,
#         %(chain_code)s,
#         %(chain)s,
#         %(name)s,
#         %(full_address)s,
#         %(phone_number)s,
#         %(sabre_rating)s,
#         %(parking)s,
#         %(is_pet_friendly)s,
#         %(pet_policy)s,
#         %(pet_fee_total_max)s,
#         %(pet_fee_deposit)s,
#         %(pet_fee_currency)s,
#         %(pet_fee_interval)s,
#         %(allowed_pet_types)s,
#         %(weight_limit)s,
#         %(max_pets)s,
#         %(has_pet_deposit)s,
#         %(has_pet_friendly_rooms)s,
#         %(pet_amenities)s,
#         %(nearby_parks)s,
#         %(source)s,
#         %(last_updated)s
#     );
#     """

#     for h in hotels:
#         pets_data = safe_json(h.get("pets_json"))
#         pet_text = pets_data.get("Pets") if pets_data else None

#         pet_data = parse_pet_policy(pet_text)

#         data = {
#             "hotel_code": h.get("hotel_code"),
#             "chain_code": "HILTON",
#             "chain": "Hilton",
#             "name": h.get("hotel_name"),
#             "full_address": h.get("address") or None,
#             "phone_number": h.get("phone"),
#             "sabre_rating": parse_rating(h.get("rating")),
#             "parking": json.dumps(safe_json(h.get("parking_json"))),
#             "is_pet_friendly": str(h.get("is_pet_friendly")).lower() == "true",
#             "pet_policy": pet_text,
#             "pet_fee_total_max": pet_data["fee"],
#             "pet_fee_deposit": pet_data["deposit"],
#             "pet_fee_currency": pet_data["currency"],
#             "pet_fee_interval": pet_data["interval"],
#             "allowed_pet_types": pet_data["pet_types"],
#             "weight_limit": pet_data["weight_limit"],
#             "max_pets": pet_data["max_pets"],
#             "has_pet_deposit": True if pet_data["deposit"] else False,
#             "has_pet_friendly_rooms": True if pet_text else None,
#             "pet_amenities": Json(safe_json(h.get("amenities_json"))),
#             "nearby_parks": json.dumps(safe_json(h.get("nearby_json"))),
#             "source": SOURCE_NAME,
#             "last_updated": datetime.fromisoformat(h.get("last_updated"))
#         }

#         cur.execute(insert_sql, data)

#     conn.commit()
#     cur.close()
#     conn.close()
#     print("✅ Hilton ingestion completed successfully")

# # =====================================================
# # RUN
# # =====================================================

# if __name__ == "__main__":
#     ingest()



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

JSON_FILE = "hilton_pet_friendly_hotels.json"
SOURCE_NAME = "hilton_hotels"
CHAIN_CODE = "HILTON"
CHAIN_NAME = "Hilton Hotels"

# Starting hotel_code number
START_HOTEL_ID = 5000

# =====================================================
# HELPERS
# =====================================================
def safe_json(val, default=None):
    """
    Converts strings/dicts/lists to JSON.
    Empty strings or malformed strings return default.
    """
    if not val:
        return default
    if isinstance(val, (dict, list)):
        return val
    try:
        j = json.loads(val)
        if isinstance(j, (dict, list)):
            return j
        else:
            return default
    except Exception:
        return default

def parse_rating(val):
    if not val:
        return None
    try:
        # Extract float from strings like "Rating: 3.5 out of 5.0"
        m = re.search(r"(\d+(\.\d+)?)", str(val))
        if m:
            return float(m.group(1))
    except:
        pass
    return None

def parse_pet_policy(pet_text):
    """
    Normalizes pet policy into structured fields.
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
        fee = numbers[0]

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
    m = re.search(r"(\d+)\s*pets", text)
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

def get_last_ingested_timestamp(cur):
    cur.execute("""
        SELECT MAX(last_updated)
        FROM hotel_masterfile
        WHERE chain_code = %s
    """, (CHAIN_CODE,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None




# =====================================================
# INGESTION
# =====================================================
def ingest():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    last_db_updated = get_last_ingested_timestamp(cur)
    print("🕒 Last Hilton record in DB:", last_db_updated)
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        hotels = json.load(f)

    # Track hotel_code increment
    hotel_counter = START_HOTEL_ID

    insert_sql = """
    INSERT INTO hotel_masterfile (
        hotel_code,
        chain_code,
        chain,
        name,
        full_address,
        city,
        state,
        country,
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
        last_updated,
        primary_airport_code,
        links
    )
    VALUES (
        %(hotel_code)s,
        %(chain_code)s,
        %(chain)s,
        %(name)s,
        %(full_address)s,
        %(city)s,
        %(state)s,
        %(country)s,
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
        %(updated)s,
        %(primary_airport_code)s,
        %(links)s
    )
    ON CONFLICT (hotel_code) DO UPDATE SET
        name = EXCLUDED.name,
        full_address = EXCLUDED.full_address,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
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
        last_updated = EXCLUDED.last_updated,
        primary_airport_code = EXCLUDED.primary_airport_code,
        links = EXCLUDED.links
    ;
    """

    for h in hotels:


        if h.get("last_updated"):
            json_updated = datetime.fromisoformat(h.get("last_updated"))

        # Skip old records
        if last_db_updated and json_updated and json_updated <= last_db_updated:
            continue
        # Generate unique hotel_code


        # Try to find existing hotel
        cur.execute("""
            SELECT hotel_code
            FROM hotel_masterfile
            WHERE chain_code = %s
            AND lower(name) = lower(%s)
            AND city IS NOT DISTINCT FROM %s
            AND state IS NOT DISTINCT FROM %s
            AND country IS NOT DISTINCT FROM %s
            LIMIT 1
        """, (
            CHAIN_CODE,
            h.get("hotel_name"),
            h.get("city"),
            h.get("state"),
            h.get("country")
        ))

        row = cur.fetchone()
        if row:
            hotel_code = row[0]   # UPDATE existing
        else:
            hotel_code = f"{hotel_counter}-{h.get('hotel_code','')}"
            hotel_counter += 1    # INSERT new

        # Pet policyhotel_code
        pets = safe_json(h.get("pets_json"))
        pet_text = None
        if pets:
            pet_text = pets.get("Pets") or pets.get("policy") or list(pets.values())[0]

        pet_data = parse_pet_policy(pet_text)

        # Airport code
        airport_list = safe_json(h.get("airport_json"), [])
        primary_airport_code = None
        if airport_list and isinstance(airport_list, list) and len(airport_list) > 0:
            first_airport = airport_list[0]
            m = re.search(r"\((\w+)\)", first_airport.get("airport",""))
            if m:
                primary_airport_code = m.group(1)

        # Build address if missing
        address = h.get("address") or h.get("address_map_url")
        if not address:
            address_parts = [h.get("hotel_name"), h.get("city"), h.get("state"), h.get("country")]
            address = ", ".join([p for p in address_parts if p])

        data = {
            "hotel_code": hotel_code,
            "chain_code": CHAIN_CODE,
            "chain": CHAIN_NAME,
            "name": h.get("hotel_name") or "",
            "full_address": address,
            "city": h.get("city") or None,
            "state": h.get("state") or None,
            "country": h.get("country") or None,
            "phone": h.get("phone") or "",
            "rating": parse_rating(h.get("rating")),
            "description": h.get("description") or "",
            "parking": Json(safe_json(h.get("parking_json"), {})),
            "is_pet_friendly": str(h.get("is_pet_friendly")).lower() == "true",
            "pet_policy": Json({"policy": pet_text}) if pet_text else None,
            "pet_fee": pet_data["fee"],
            "pet_deposit": pet_data["deposit"],
            "currency": pet_data["currency"],
            "interval": pet_data["interval"],
            "pet_types": pet_data["pet_types"],
            "weight_limit": pet_data["weight_limit"],
            "max_pets": pet_data["max_pets"],
            "has_deposit": True if pet_data["deposit"] else False,
            "has_pet_rooms": True if pet_text else None,
            "amenities": Json(safe_json(h.get("amenities_json"), [])),
            "nearby": Json(safe_json(h.get("nearby_json"), [])),
            "source": SOURCE_NAME,
            "updated": datetime.fromisoformat(h.get("last_updated")),
            "primary_airport_code": primary_airport_code,
            "links": Json({"map_url": h.get("address_map_url")}) if h.get("address_map_url") else None,
        }

        cur.execute(insert_sql, data)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Hilton ingestion completed successfully!")

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    ingest()
