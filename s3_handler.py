import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import shutil


def get_s3_handle(is_public):
    s3 = boto3.client('s3')
    if is_public:
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    return s3


def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')


def get_files_in_path(bucket: str, prefix: str, is_public) -> list:
    s3 = get_s3_handle(is_public=is_public)
    object_keys = []
    for obj in get_all_s3_objects(s3, Bucket=bucket, Prefix=prefix):
        object_keys.append(obj["Key"])
    return object_keys


def reset_dir(mydir):
    shutil.rmtree(mydir)
    os.makedirs(mydir, exist_ok=True)


def download_public_file(object_key, destination_dir):
    file_name = object_key.split("/")[-1]
    dest_path = os.path.join(destination_dir, file_name)

    s3 = get_s3_handle(is_public=True)
    with open(dest_path, 'wb') as f:
        s3.download_fileobj("commoncrawl", object_key, f)
    return dest_path


def get_filename_from_key(obj_key):
    # obj_key = 'crawl-data/CC-NEWS/2020/09/CC-NEWS-20200915174842-00048.warc.gz'
    return obj_key.split("/")[-1]


def upload_file_to_s3(path_local, bucket_dest, key_dest):
    s3 = get_s3_handle(is_public=False)
    s3.upload_file(Filename=path_local,
                   Bucket=bucket_dest,
                   Key=key_dest)
