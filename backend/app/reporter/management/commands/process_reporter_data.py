import datetime
import glob
import itertools
import json
import os
import time

from django.core.management.base import BaseCommand

from reporter import models
from reporter.management import utils


def create_project_record_changeset(db_record, new_data, recording_date):
    project_record_changeset = []
    diff_list = utils.dict_diff(db_record.data, new_data)
    for diff in diff_list:
        flag = models.ProjectRecordChange.Flag.ALTERED
        project_record_changeset.append(
            models.ProjectRecordChange(
                project_record = db_record,
                date_of_change = recording_date,
                field = diff["field"],
                flag = flag,
                data = {
                    "old_value": diff["old_value"],
                    "new_value": diff["new_value"]
                }
            )
        )

    return project_record_changeset


def process_reporter_json(path, recording_date):
    with open(path) as src:
        data = json.load(src)

    for record in data:
        try:
            new_data_hash = utils.hash_dict(record)
            db_record = models.ProjectRecord.objects.get(
                appl_id = record["appl_id"]
            )
            if db_record.data_hash != new_data_hash:
                if db_record.date_of_last_change and db_record.date_of_last_change == recording_date:
                    raise ValueError(
                        "A data integrity assumption has been violated"
                    )
                changeset = create_project_record_changeset(db_record, record, recording_date)
                [change.save() for change in changeset]

                db_record.data = record
                db_record.data_hash = new_data_hash
                db_record.date_of_last_change = recording_date
                db_record.save()

        except models.ProjectRecord.DoesNotExist:
            db_record = models.ProjectRecord(
                appl_id = record["appl_id"],
                project_num = record["project_num"],
                fain = utils.get_fain(record),
                date_of_last_change = None,
                data_hash = utils.hash_dict(record),
                data = record
            ).save()


class Command(BaseCommand):
    help = "Process a json dump of NIH RePORTER Data"

    def add_arguments(self, parser):
        parser.add_argument("path_to_data", type=str, help="Path to json NIH RePORTER exports")
        parser.add_argument("recording_date", type=str, help="Date of data recording")

    def handle(self, *args, **options):
        path_to_data = options["path_to_data"]
        try:
            recording_date = datetime.datetime.fromisoformat(
                options["recording_date"]
            )
        except Exception as e:
            raise ValueError("Recording date must be in isoformat")

        self.stdout.write("Detecting files...")
        json_files = list(sorted(set((
            glob.glob(os.path.join(path_to_data, "*.json")) +
                glob.glob(os.path.join(path_to_data, "*/*.json")) +
                glob.glob(os.path.join(path_to_data, "*/*/*.json")) +
                glob.glob(os.path.join(path_to_data, "*/*/*/*.json"))
        ))))
        self.stdout.write(f"Detected {len(json_files)} files to process...")


        n_processed = 0
        for batch in itertools.batched(json_files, 10):
            self.stdout.write(f"Processing files {n_processed + 1} to {n_processed + 10}")
            for f in batch:
                process_reporter_json(f, recording_date)

            n_processed += 10

        self.stdout.write(self.style.SUCCESS('Successfully executed your command'))
