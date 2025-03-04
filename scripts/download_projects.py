import copy
import datetime
import logging
import json
import os
import requests
from pprint import pprint

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


REPORTER_API_URL = "https://api.reporter.nih.gov/v2/"


def get_all_items(criteria, offset=0):
    items = []

    # Attempt to download a single data packet
    payload = {
        "criteria": criteria,
        "offset": offset,
        "limit": 500
    }
    response = requests.post(
        os.path.join(REPORTER_API_URL, "projects", "search"),
        json=payload
    )
    response.raise_for_status()
    data = response.json()

    items.extend(
        data["results"]
    )

    n_downloaded = offset + len(data["results"])
    logger.info(
        f"Downloaded {n_downloaded} of {data['meta']['total']}"
    )

    # Recursively download if more data
    next_offset = offset + data["meta"]["limit"]
    if next_offset < data["meta"]["total"]:
        items.extend(
            get_all_items(criteria, next_offset)
        )

    return items


def download_items_for_date(from_date):
    # Skip if already downloaded
    dest_dir = os.path.join(
        "data",
        "json",
        "projects",
        f"year_added={from_date.strftime('%Y')}",
        f"month_added={from_date.strftime('%m')}"
    )
    dest_filename = f"projects_added_{from_date.strftime('%Y_%m_%d')}.json"
    if os.path.exists(os.path.join(dest_dir, dest_filename)):
        logger.info(f"Skipping download for {from_date.strftime('%Y-%m-%d')}")
        return

    # Attempt to download data for a given day
    logger.info(
        f"Downloading data for {from_date.strftime('%Y-%m-%d')}"
    )
    to_date = (
        from_date +
            datetime.timedelta(days=1) -
            datetime.timedelta(microseconds=1)
    )

    criteria = {
        "date_added": {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat()
        },
    }
    items = get_all_items(criteria)
    if not items:
        return

    # Only write file if records are present
    os.makedirs(dest_dir, exist_ok=True)
    with open(os.path.join(dest_dir, dest_filename), "w") as dest:
        json.dump(items, dest, indent=4)


if __name__ == "__main__":
    query_datetime = datetime.datetime.fromisoformat("2016-01-01")
    while query_datetime < datetime.datetime.now() - datetime.timedelta(days=1):
        download_items_for_date(query_datetime)
        query_datetime += datetime.timedelta(days=1)
