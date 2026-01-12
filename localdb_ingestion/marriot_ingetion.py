

import json
import re
import psycopg2
from psycopg2.extras import Json

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": "dost"
}

JSON_FILE = "marriott_pet_friendly_hotels.json"
SOURCE = "marriott_scraper"
CHAIN_CODE = "MAR"
CHAIN_NAME = "Marriott International"
START_HOTEL_ID = 1000  # starting unique id for hotel_code

# ---------- LOAD JSON ----------
def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# ---------- PARSE PET FEES ----------
def parse_pet_fees(raw_text):
    """
    Parse pet fee info from raw text into structured fields
    """
    result = {
        "pet_fee_night": None,
        "pet_fee_total_max": None,
        "pet_fee_deposit": None,
        "pet_fee_currency": None,
        "pet_fee_interval": None,
        "pet_fee_variations": None,
        "has_pet_deposit": False,
        "is_deposit_refundable": None
    }

    if not raw_text:
        return result

    # Match amounts + currency + interval
    fee_pattern = re.compile(r"(\d+\.?\d*)\s*(USD|JOD|EUR|ILS)?\s*(Per Stay|Per Night|Deposit)?", re.IGNORECASE)
    matches = fee_pattern.findall(raw_text)

    for amt, currency, interval in matches:
        amt = float(amt)
        currency = currency if currency else "USD"
        interval_lower = interval.lower() if interval else ""

        if "per night" in interval_lower:
            result["pet_fee_night"] = amt
        elif "per stay" in interval_lower:
            result["pet_fee_total_max"] = amt
        elif "deposit" in interval_lower:
            result["pet_fee_deposit"] = amt
            result["has_pet_deposit"] = True

        result["pet_fee_currency"] = currency
        result["pet_fee_interval"] = interval if interval else None

    # Non-refundable
    result["is_deposit_refundable"] = False if "non-refundable" in raw_text.lower() else True

    # Store full raw variations
    result["pet_fee_variations"] = Json({"raw": raw_text})

    return result

# ---------- INSERT HOTELS ----------
def insert_hotels(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    insert_query = """
    INSERT INTO public.hotel_masterfile (
        hotel_code,
        chain_code,
        chain,
        name,
        state,
        country_code,
        country,
        city,
        postal_code,
        address_line_1,
        full_address,
        description,
        links,
        phone_number,
        is_pet_friendly,
        pet_policy,
        has_pet_friendly_rooms,
        last_updated,
        source,
        sabre_rating,
        pet_fee_night,
        pet_fee_total_max,
        pet_fee_deposit,
        pet_fee_currency,
        pet_fee_interval,
        pet_fee_variations,
        has_pet_deposit,
        is_deposit_refundable
    ) VALUES (
        %(hotel_code)s,
        %(chain_code)s,
        %(chain)s,
        %(name)s,
        %(state)s,
        %(country_code)s,
        %(country)s,
        %(city)s,
        %(postal_code)s,
        %(address_line_1)s,
        %(full_address)s,
        %(description)s,
        %(links)s,
        %(phone_number)s,
        %(is_pet_friendly)s,
        %(pet_policy)s,
        %(has_pet_friendly_rooms)s,
        %(last_updated)s,
        %(source)s,
        %(sabre_rating)s,
        %(pet_fee_night)s,
        %(pet_fee_total_max)s,
        %(pet_fee_deposit)s,
        %(pet_fee_currency)s,
        %(pet_fee_interval)s,
        %(pet_fee_variations)s,
        %(has_pet_deposit)s,
        %(is_deposit_refundable)s
    )
    ON CONFLICT ON CONSTRAINT hotel_code_unique DO UPDATE SET
        name = EXCLUDED.name,
        state = EXCLUDED.state,
        country_code = EXCLUDED.country_code,
        country = EXCLUDED.country,
        city = EXCLUDED.city,
        postal_code = EXCLUDED.postal_code,
        address_line_1 = EXCLUDED.address_line_1,
        full_address = EXCLUDED.full_address,
        description = EXCLUDED.description,
        links = EXCLUDED.links,
        phone_number = EXCLUDED.phone_number,
        is_pet_friendly = EXCLUDED.is_pet_friendly,
        pet_policy = EXCLUDED.pet_policy,
        has_pet_friendly_rooms = EXCLUDED.has_pet_friendly_rooms,
        last_updated = EXCLUDED.last_updated,
        source = EXCLUDED.source,
        sabre_rating = EXCLUDED.sabre_rating,
        pet_fee_night = EXCLUDED.pet_fee_night,
        pet_fee_total_max = EXCLUDED.pet_fee_total_max,
        pet_fee_deposit = EXCLUDED.pet_fee_deposit,
        pet_fee_currency = EXCLUDED.pet_fee_currency,
        pet_fee_interval = EXCLUDED.pet_fee_interval,
        pet_fee_variations = EXCLUDED.pet_fee_variations,
        has_pet_deposit = EXCLUDED.has_pet_deposit,
        is_deposit_refundable = EXCLUDED.is_deposit_refundable;
    """

    # Track hotel codes for uniqueness
    hotel_code_counter = START_HOTEL_ID
    unique_codes_set = set()

    inserted_count = 0
    for hotel in data:
        base_code = hotel.get("hotel_code", "UNKNOWN")
        # Generate a unique hotel_code
        while True:
            new_code = f"{hotel_code_counter}-{CHAIN_CODE}-{base_code}"
            if new_code not in unique_codes_set:
                unique_codes_set.add(new_code)
                hotel_code_counter += 1
                break
            hotel_code_counter += 1

        # Parse pets
        raw_pets = hotel.get("pets_json")
        if isinstance(raw_pets, str) and raw_pets.strip():
            try:
                raw_pets = json.loads(raw_pets)
            except json.JSONDecodeError:
                raw_pets = {}
        pet_fees = parse_pet_fees(raw_pets.get("raw") if raw_pets else None)

        hotel_data = {
            "hotel_code": new_code,
            "chain_code": CHAIN_CODE,
            "chain": CHAIN_NAME,
            "name": hotel.get("hotel_name"),
            "state": hotel.get("state"),
            "country_code": hotel.get("country"),
            "country": hotel.get("state"),
            "city": hotel.get("city"),
            "postal_code": hotel.get("country"),
            "address_line_1": hotel.get("address"),
            "full_address": hotel.get("address"),
            "description": hotel.get("description"),
            "links": Json({"property_website": hotel.get("property_website")}) if hotel.get("property_website") else Json({}),
            "phone_number": hotel.get("phone"),
            "is_pet_friendly": hotel.get("is_pet_friendly") == "true",
            "pet_policy": raw_pets.get("raw") if raw_pets else None,
            "has_pet_friendly_rooms": hotel.get("is_pet_friendly") == "true",
            "last_updated": hotel.get("last_updated"),
            "source": SOURCE,
            "sabre_rating": float(hotel.get("rating")) if hotel.get("rating") else None,
            **pet_fees
        }

        cur.execute(insert_query, hotel_data)
        inserted_count += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Hotels inserted/updated with unique codes: {inserted_count}")

# ---------- MAIN ----------
if __name__ == "__main__":
    hotels = load_json(JSON_FILE)
    insert_hotels(hotels)

