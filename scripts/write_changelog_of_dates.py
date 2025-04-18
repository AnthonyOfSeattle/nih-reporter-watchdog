import duckdb
import logging
import os
import pandas as pd

from datetime import datetime, timedelta
from glob import glob


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


JSON_VS_EXPORTER_QUERY = """
    SELECT new_data.appl_id APPLICATION_ID,
           new_data.core_project_num CORE_PROJECT_NUM,
           new_data.project_num PROJECT_NUM,
           new_data.fiscal_year FY,
           new_data.organization.org_name ORG_NAME,
           new_data.organization.org_country ORG_COUNTRY,
           DATE '{}' DATE_OF_CHANGE,
           old_data.PROJECT_START PROJECT_START_OLD,
           date_trunc('day', new_data.project_start_date) PROJECT_START_NEW,
           old_data.PROJECT_END PROJECT_END_OLD,
           date_trunc('day', new_data.project_end_date) PROJECT_END_NEW,
           old_data.BUDGET_START BUDGET_START_OLD,
           date_trunc('day', new_data.budget_start) BUDGET_START_NEW,
           old_data.BUDGET_END BUDGET_END_OLD,
           date_trunc('day', new_data.budget_end) BUDGET_END_NEW,
    FROM read_json('/data/json_{}/projects/year_added=202[012345]/*/*') AS new_data
    INNER JOIN read_csv('/data/exporter/projects/RePORTER_PRJ_C_FY2024.csv') AS old_data
      ON new_data.appl_id = old_data.APPLICATION_ID
    WHERE PROJECT_START_NEW != PROJECT_START_OLD
       OR PROJECT_END_NEW != PROJECT_END_OLD
       OR BUDGET_START_NEW != BUDGET_START_OLD
       OR BUDGET_END_NEW != BUDGET_END_OLD
    ORDER BY APPLICATION_ID
"""


JSON_VS_JSON_QUERY = """
    SELECT new_data.appl_id APPLICATION_ID,
           new_data.core_project_num CORE_PROJECT_NUM,
           new_data.project_num PROJECT_NUM,
           new_data.fiscal_year FY,
           new_data.organization.org_name ORG_NAME,
           new_data.organization.org_country ORG_COUNTRY,
           DATE '{}' DATE_OF_CHANGE,
           date_trunc('day', old_data.project_start_date) PROJECT_START_OLD,
           date_trunc('day', new_data.project_start_date) PROJECT_START_NEW,
           date_trunc('day', old_data.project_end_date) PROJECT_END_OLD,
           date_trunc('day', new_data.project_end_date) PROJECT_END_NEW,
           date_trunc('day', old_data.budget_start) BUDGET_START_OLD,
           date_trunc('day', new_data.budget_start) BUDGET_START_NEW,
           date_trunc('day', old_data.budget_end) BUDGET_END_OLD,
           date_trunc('day', new_data.budget_end) BUDGET_END_NEW,
    FROM read_json('/data/json_{}/projects/year_added=202[012345]/*/*') AS new_data
    INNER JOIN read_json('/data/json_{}/projects/year_added=202[012345]/*/*') AS old_data
      ON new_data.appl_id = old_data.appl_id
    WHERE PROJECT_START_NEW != PROJECT_START_OLD
       OR PROJECT_END_NEW != PROJECT_END_OLD
       OR BUDGET_START_NEW != BUDGET_START_OLD
       OR BUDGET_END_NEW != BUDGET_END_OLD
    ORDER BY new_data.appl_id
"""


def clean_data(data):
    # Melt data to linearized fields
    data_long = data.melt(
        id_vars = ["APPLICATION_ID", "CORE_PROJECT_NUM", "PROJECT_NUM", "FY", "ORG_NAME", "ORG_COUNTRY", "DATE_OF_CHANGE"],
        var_name="FIELD",
        value_name="VALUE"
    )
    data_long.insert(5, "COLUMN", "")
    data_long.loc[data_long.FIELD.str.contains("OLD"), "COLUMN"] = "OLD_VALUE"
    data_long.loc[data_long.FIELD.str.contains("NEW"), "COLUMN"] = "NEW_VALUE"
    data_long.loc[:, "FIELD"] = (
        data_long.FIELD
                 .str.replace("_[^_]+$", "", regex=True)
    )

    # Widen out old and new values to make changelog
    data_final = data_long.pivot(
        index = ["APPLICATION_ID", "CORE_PROJECT_NUM", "PROJECT_NUM", "FY", "ORG_NAME", "ORG_COUNTRY", "DATE_OF_CHANGE", "FIELD"],
        columns = "COLUMN",
        values = "VALUE"
    ).reset_index()
    data_final = data_final[data_final.NEW_VALUE != data_final.OLD_VALUE]
    data_final = data_final.sort_values(
        ["APPLICATION_ID", "FIELD", "DATE_OF_CHANGE"]
    ).loc[:,
        ["APPLICATION_ID", "CORE_PROJECT_NUM", "PROJECT_NUM", "FY", "ORG_NAME", "ORG_COUNTRY", "FIELD", "DATE_OF_CHANGE", "OLD_VALUE", "NEW_VALUE"]
    ]

    return data_final


def write_initial_changelog():
    data_date = datetime.fromisoformat("2025-03-02")
    dest_file = os.path.join(
        "/public/changelogs/weekly/date/",
        f"reporter_date_changelog_{data_date.strftime('%Y_%m_%d')}.csv"
    )

    if os.path.exists(dest_file):
        logger.info(
            f"Found changelog for {data_date}. Skipping..."
        )
        return
    logger.info(
        f"Writing changelog for {data_date}"
    )

    data = duckdb.query(
        JSON_VS_EXPORTER_QUERY.format(
            data_date,
            data_date.strftime("%Y_%m_%d")
        )
    ).to_df()
    data_final = clean_data(data)
    data_final.to_csv(dest_file, index=False)


def write_weekly_changelog():
    data_date = datetime.fromisoformat("2025-03-09")
    data_path = f"/data/json_{data_date.strftime('%Y_%m_%d')}"
    while os.path.exists(data_path):
        dest_file = os.path.join(
            "/public/changelogs/weekly/date/",
            f"reporter_date_changelog_{data_date.strftime('%Y_%m_%d')}.csv"
        )

        if os.path.exists(dest_file):
            logger.info(
                f"Found changelog for {data_date}. Skipping..."
            )
            data_date += timedelta(days=7)
            data_path = f"/data/json_{data_date.strftime('%Y_%m_%d')}"
            continue
        logger.info(
            f"Writing changelog for {data_date}"
        )
    
        reference_date = data_date - timedelta(days=7)
        data = duckdb.query(
            JSON_VS_JSON_QUERY.format(
                data_date,
                data_date.strftime("%Y_%m_%d"),
                reference_date.strftime("%Y_%m_%d")
            )
        ).to_df()
        data_final = clean_data(data)
    
        data_final.to_csv(
            dest_file,
            index = False
        )
    
        data_date += timedelta(days=7)
        data_path = f"/data/json_{data_date.strftime('%Y_%m_%d')}"


def write_combined_changelog():
    logger.info("Combining changelogs...")
    data_full = pd.concat([
        pd.read_csv(f)
        for f in glob("/public/changelogs/weekly/date/reporter_date_changelog_*")
    ])
    data_full = data_full.sort_values(
        ["APPLICATION_ID", "FIELD", "DATE_OF_CHANGE"]
    )
    data_full = data_full[data_full.FY >= 2023]
    data_full.to_csv(
        "/public/changelogs/combined/reporter_date_changelog.csv",
        index=False
    )


if __name__ == "__main__":
    write_initial_changelog()
    write_weekly_changelog()
    write_combined_changelog()
