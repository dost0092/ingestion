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
#     "dbname": "kruiz-dev",
#     "user": "postgres",
#     "password": "dost"
# }

# JSON_FILE = "hyatt_hotels.json"
# SOURCE_NAME = "hyatt_hotels"

# # =====================================================
# # HELPERS
# # =====================================================

# def safe_json(val):
#     if not val:
#         return None
#     if isinstance(val, (list, dict)):
#         return val
#     try:
#         return json.loads(val)
#     except Exception:
#         return None


# def parse_address_parts(address_line_2):
#     """
#     Example:
#     'Jacksonville, Florida, United States, 32256'
#     """
#     if not address_line_2:
#         return None, None, None, None

#     parts = [p.strip() for p in address_line_2.split(",")]

#     city = parts[0] if len(parts) > 0 else None
#     state = parts[1] if len(parts) > 1 else None
#     country = parts[2] if len(parts) > 2 else None
#     postal = parts[3] if len(parts) > 3 else None

#     return city, state, country, postal


# def parse_pet_policy(pet_text):
#     if not pet_text:
#         return {}

#     text = pet_text.lower()

#     # -------- Currency --------
#     currency = "USD" if "$" in pet_text else None

#     # -------- Fees --------
#     amounts = [float(x.replace(",", "")) for x in re.findall(r"\$(\d+\.?\d*)", pet_text)]
#     fee_max = max(amounts) if amounts else None

#     # -------- Interval --------
#     interval = "per_stay"
#     if "night" in text:
#         interval = "per_night"

#     # -------- Max Pets --------
#     max_pets = None
#     m = re.search(r"maximum number of pets is (\d+)", text)
#     if m:
#         max_pets = int(m.group(1))

#     # -------- Weight Limit --------
#     weight_limit = None
#     m = re.search(r"(\d+)\s*pounds", text)
#     if m:
#         weight_limit = f"{m.group(1)} lbs"

#     return {
#         "fee_max": fee_max,
#         "currency": currency,
#         "interval": interval,
#         "max_pets": max_pets,
#         "weight_limit": weight_limit
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
#         city,
#         state,
#         country,
#         postal_code,
#         address_line_1,
#         address_line_2,
#         full_address,
#         phone_number,
#         links,
#         is_pet_friendly,
#         pet_policy,
#         pet_fee_total_max,
#         pet_fee_currency,
#         pet_fee_interval,
#         max_pets,
#         weight_limit,
#         pet_amenities,
#         source,
#         last_updated,
#         updated_at
#     )
#     VALUES (
#         %(hotel_code)s,
#         %(chain_code)s,
#         %(chain)s,
#         %(name)s,
#         %(city)s,
#         %(state)s,
#         %(country)s,
#         %(postal_code)s,
#         %(address_line_1)s,
#         %(address_line_2)s,
#         %(full_address)s,
#         %(phone_number)s,
#         %(links)s,
#         %(is_pet_friendly)s,
#         %(pet_policy)s,
#         %(pet_fee_total_max)s,
#         %(pet_fee_currency)s,
#         %(pet_fee_interval)s,
#         %(max_pets)s,
#         %(weight_limit)s,
#         %(pet_amenities)s,
#         %(source)s,
#         %(last_updated)s,
#         NOW()
#     );
#     """

#     for h in hotels:
#         city, state, country, postal = parse_address_parts(h.get("address_line_2"))
#         pet_text = h.get("pet_policy_description")
#         pet_data = parse_pet_policy(pet_text)

#         data = {
#             "hotel_code": h.get("hotel_code"),
#             "chain_code": "HYATT",
#             "chain": "Hyatt",
#             "name": h.get("hotel_name"),
#             "city": city,
#             "state": state,
#             "country": country,
#             "postal_code": postal,
#             "address_line_1": h.get("address_line_1"),
#             "address_line_2": h.get("address_line_2"),
#             "full_address": f"{h.get('address_line_1')}, {h.get('address_line_2')}",
#             "phone_number": h.get("phone"),
#             "links": Json({
#                 "property_url": h.get("hotel_url")
#             }),
#             "is_pet_friendly": True if pet_text else False,
#             "pet_policy": pet_text,
#             "pet_fee_total_max": pet_data.get("fee_max"),
#             "pet_fee_currency": pet_data.get("currency"),
#             "pet_fee_interval": pet_data.get("interval"),
#             "max_pets": pet_data.get("max_pets"),
#             "weight_limit": pet_data.get("weight_limit"),
#             "pet_amenities": Json(safe_json(h.get("amenities_json"))),
#             "source": SOURCE_NAME,
#             "last_updated": datetime.fromisoformat(h.get("last_updated"))
#         }

#         cur.execute(insert_sql, data)

#     conn.commit()
#     cur.close()
#     conn.close()
#     print("✅ Hyatt ingestion completed successfully")

# # =====================================================
# # RUN
# # =====================================================

# if __name__ == "__main__":
#     ingest()


import psycopg2
import json

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": "dost"
}

JSON_FILE = "hyatt_hotels.json"

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

with open(JSON_FILE, "r", encoding="utf-8") as f:
    hotels = json.load(f)

for h in hotels:
    hotel_code = h.get("hotel_code", "").strip()
    if not hotel_code:
        continue

    # Combine pet_policy_description + pet_fees_json
    pet_description = h.get("pet_policy_description") or ""
    pet_fees_list = h.get("pet_fees_json") or []
    pet_fees_text = "\n".join(pet_fees_list) if pet_fees_list else ""
    full_pet_policy = pet_description
    if pet_fees_text:
        full_pet_policy += "\n" + pet_fees_text

    # Debug: check what is being updated
    print(f"Updating hotel_code: {hotel_code}, pet_policy length: {len(full_pet_policy)}")

    # Update DB
    cur.execute("""
        UPDATE hotel_masterfile
        SET pet_policy = %s
        WHERE TRIM(hotel_code) = %s
    """, (full_pet_policy, hotel_code))

conn.commit()
cur.close()
conn.close()

print("✅ pet_policy column updated successfully from JSON file")
