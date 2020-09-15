from s3_handler import get_files_in_path
from s3_handler import download_public_file
from s3_handler import get_download_dir
from s3_handler import get_filename_from_key
from s3_handler import upload_file_to_s3
from s3_handler import reset_download_dir
from warc_parser import extract_from_archive
from html_content_extractor import extract_doc
import logging
import json
import sys
import os
import argparse


logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='WARC parser')
parser.add_argument('month_id', type=str, help='month id, e.g. 01, 02, etc')
parser.add_argument('year_id', type=str, default="2020", help='year id, e.g. 2020, 2019')
args = parser.parse_args()

month_id = args.month_id
year_id = args.year_id

logger.info(f"Month ID: {month_id}")
logger.info(f"YEAR ID: {year_id}")

assert len(month_id) == 2
assert 0 < int(month_id) < 13


BUCKET_SOURCE = "commoncrawl"
FILE_PREFIX = f"crawl-data/CC-NEWS/{year_id}/{month_id}"

BUCKET_DESTINATION = "cooltext_commoncrawl"

OUT_DIR = "data/out_dir"

if not os.path.exists:
    os.makedirs(OUT_DIR, exist_ok=True)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # get list of files
    keys_all = get_files_in_path(bucket=BUCKET_SOURCE,
                                  prefix=FILE_PREFIX,
                                  is_public=True)

    logger.info(f"File count in source: {len(keys_all)}")

    # remove processed files
    keys_processed = get_files_in_path(bucket=BUCKET_DESTINATION,
                                        prefix=FILE_PREFIX,
                                        is_public=False)
    # -5 is to remove the .json extension
    filenames_proc = [get_filename_from_key(obj_key)[:-5] for obj_key in keys_processed]
    logger.info(f"File count in processed: {len(filenames_proc)}")

    keys_pending = [obj_key for obj_key in keys_all if get_filename_from_key(obj_key) not in filenames_proc]
    logger.info(f"File count in pending: {len(keys_pending)}")

    # process next file
    for obj_ix, obj_key in enumerate(keys_pending):
        obj_file_name = get_filename_from_key(obj_key)
        logger.info(f"Processing file: {obj_ix}/{len(keys_pending)}")
        logger.info(f"File Name: {obj_file_name}")

        destination_dir = get_download_dir()
        # download archive file
        # obj_key = "CC-NEWS-20200913085541-00012.warc.gz"
        archive_path = download_public_file(obj_key, destination_dir)
        logger.info(f"File downloaded to: {archive_path}")

        # extract contents of archive
        # archive_path = "data/temp_dir/CC-NEWS-20200913085541-00012.warc.gz"
        contents, domain_blames = extract_from_archive(archive_path)
        logger.info(f"Number of contents found: {len(contents)}")

        # further cleaning
        for ix, arch_content in enumerate(contents):
            sys.stdout.write(f"finished {ix}/{len(contents)}\r")
            arch_content["doc"] = extract_doc(arch_content["content"])

        # keep valid docs only
        contents = [c for c in contents if c["doc"] is not None]
        logger.info(f"Number of docs with valid doc: {len(contents)}")

        # delete 'content' key
        for c in contents:
            c.pop('content', None)

        # save results
        out_content = {"docs": contents}
        out_content = json.dumps(out_content)
        out_filename = obj_file_name + ".json"
        out_path = os.path.join(OUT_DIR, out_filename)
        with open(out_path, "w") as f:
            f.write(out_content)
        logger.info(f"Processed content written to: {out_path}")

        # upload file
        dest_key = os.path.join(FILE_PREFIX, out_filename)
        upload_file_to_s3(path_local=out_path,
                          bucket_dest=BUCKET_DESTINATION,
                          key_dest=dest_key)
        logger.info(f"File uploaded to: {dest_key}")

        # delete archive file
        reset_download_dir()
        logger.info(f"Cleaned up download dir")
