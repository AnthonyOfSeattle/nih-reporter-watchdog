import copy
import datetime
import logging
import json
import itertools
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
        "sort_field": "appl_id",
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


def download_items_for_date(from_date, force=False):
    # Skip if already downloaded
    dest_dir = os.path.join(
        "data",
        "json",
        "projects",
        f"year_added={from_date.strftime('%Y')}",
        f"month_added={from_date.strftime('%m')}"
    )
    dest_filename = f"projects_added_{from_date.strftime('%Y_%m_%d')}.json"
    if os.path.exists(os.path.join(dest_dir, dest_filename)) and not force:
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


def get_items_for_date_range(from_date, to_date):
    # Attempt to download data
    logger.info(
        f"Downloading data from {from_date.isoformat()}"
        f" to {to_date.isoformat()}"
    )
    criteria = {
        "date_added": {
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat()
        },
    }
    items = get_all_items(criteria)
    if not items:
        logger.info(
            "No items found for date range"
        )
        return

    # Group by date_added and send to files
    logger.info(
        f"Found {len(items)} items for date range"
    )
    items = sorted(
        items,
        key=lambda x: (
            datetime.datetime.fromisoformat(x["date_added"]).date(),
            x["appl_id"]
        )
    )
    grouped_items = itertools.groupby(
        items,
        lambda x: datetime.datetime.fromisoformat(x["date_added"]).date()
    )
    for date, group in grouped_items:
        group = list(group)
        logger.info(
            f"Writing {len(group)} items for {date}"
        )
        dest_dir = os.path.join(
            "data",
            "json",
            "projects",
            f"year_added={date.strftime('%Y')}",
            f"month_added={date.strftime('%m')}"
        )
        dest_filename = f"projects_added_{date.strftime('%Y_%m_%d')}.json"
        with open(os.path.join(dest_dir, dest_filename), "w") as dest:
            json.dump(group, dest, indent=4)

    

def get_date_of_first_refresh(year):
    """Walks through dates to find first date where a refresh happened"""
    logger.info(f"Searching for date of first refresh in {year}")
    cur_date = datetime.datetime(year=year, month=1, day=1)
    while cur_date < datetime.datetime(year=year + 1, month=1, day=1):
        to_date = (
            cur_date +
                datetime.timedelta(days=1) -
                datetime.timedelta(microseconds=1)
        )
        payload = {
            "criteria": {
                "date_added": {
                    "from_date": cur_date.isoformat(),
                    "to_date": to_date.isoformat()
                }

            },
            "limit": 1
        }

        response = requests.post(
            os.path.join(REPORTER_API_URL, "projects", "search"),
            json=payload
        )
        response.raise_for_status()
        data = response.json()

        if data["meta"]["total"]:
            return cur_date
        cur_date += datetime.timedelta(days=1)


def get_data_for_year(year):
    """Walks through a year week by week to download data"""
    from_date = get_date_of_first_refresh(year)
    while from_date < datetime.datetime(year=year + 1, month=1, day=1):
        # Download data a week at a time
        to_date = (
            from_date +
                datetime.timedelta(days=7) -
                datetime.timedelta(microseconds=1)
        )
        get_items_for_date_range(from_date, to_date)
        from_date += datetime.timedelta(days=7)


if __name__ == "__main__":
    get_data_for_year(2024)
    get_data_for_year(2025)
    #query_datetime = datetime.datetime.fromisoformat("2015-01-01")
    #while query_datetime < datetime.datetime.fromisoformat("2016-01-01"):
    #    download_items_for_date(query_datetime, force=True)
    #    query_datetime += datetime.timedelta(days=1)
