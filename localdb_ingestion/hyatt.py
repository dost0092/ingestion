

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
