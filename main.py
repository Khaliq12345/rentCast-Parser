import os
import httpx
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import config

# setup and initialise db client
uri = f"mongodb+srv://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/?retryWrites=true&w=majority&appName=ClusterV1"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi("1"))
database = client["property"]
collection = database["records"]

headers = {
    "X-Api-Key": config.RENT_CAST_API,
    "accept": "application/json",
}

LIMIT = 100
PROGRESS = "progress.txt"


def get_offset() -> int:
    """Get"""
    if not os.path.exists(PROGRESS):
        return 0
    with open(PROGRESS, "r") as f:
        return int(f.read())


def update_offset(offset: int) -> None:
    """Update progress"""
    with open(PROGRESS, "w") as f:
        f.write(f"{offset}")


def send_to_db(records: list[dict]):
    """Send data to the database"""
    print("Save data")
    try:
        collection.insert_many(records)
    except Exception as e:
        print(e)


def scrape_one_page(offset: int) -> int:
    """Extract records and save"""
    print(f"Offset -> {offset} -> LIMIT {LIMIT}")
    params = {
        "city": "Nashville",
        "state": "TN",
        "limit": LIMIT,
        "offset": offset,
    }

    response = httpx.get(
        "https://api.rentcast.io/v1/properties", params=params, headers=headers
    )
    json_data = response.json()
    send_to_db(json_data)
    return len(json_data)


def scrape_multiple_pages():
    """Scrape multiple pages"""
    offset = get_offset()
    page = 0
    while True:
        print(f"Page - {page}")
        total = scrape_one_page(offset)
        if not total:
            break

        # update pagination stuffs
        offset += LIMIT
        if page == 2:
            break
        page += 1
        update_offset(offset)


def main():
    """Start scraper"""
    try:
        print("Hello from rentcast!")
        scrape_multiple_pages()
    except Exception as e:
        print(f"Errored - {e}")
        client.close()


if __name__ == "__main__":
    main()
