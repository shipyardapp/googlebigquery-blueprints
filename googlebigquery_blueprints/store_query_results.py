import os
import json
import tempfile
import argparse
import sys

from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from bq_iterate import BqQueryRowIterator, BqTableRowIterator, batchify_iterator


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument(
        '--service-account',
        dest='service_account',
        required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument('--gcs-bucket', dest = 'bucket', required = False, default= None)
    args = parser.parse_args()
    return args


def set_environment_variables(args):
    """
    Set GCP credentials as environment variables if they're provided via keyword
    arguments rather than seeded as environment variables. This will override
    system defaults.
    """
    credentials = args.service_account
    try:
        json_credentials = json.loads(credentials)
        fd, path = tempfile.mkstemp()
        print(f'Storing json credentials temporarily at {path}')
        with os.fdopen(fd, 'w') as tmp:
            tmp.write(credentials)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        return path
    except Exception:
        print('Using specified json credentials file')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        return


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def create_csv(query, client, destination_file_path, destination_file_name = None, bucket = None):
    """
    Read in data from a SQL query and into a bucket. Store the data as a csv.
    """

    # if bucket is not None:
    #     data = client.query(query)
    #     data.result()
    #     temp_table_ids = data._properties["configuration"]["query"]["destinationTable"]
    #     location = data._properties["jobReference"]["location"]
    #     project_id = temp_table_ids.get('projectId')
    #     dataset_id = temp_table_ids.get('datasetId')
    #     table_id = temp_table_ids.get('tableId')
    #     destination_uri = f'gs://{bucket}/{destination_file_name}'
    #     dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    #     table_ref = dataset_ref.table(table_id)
    #     try:
    #         extract_job = client.extract_table(
    #             table_ref,
    #             destination_uri,
    #             location="US")
    #         extract_job.result()
    #     except Exception as e:
    #         raise(e)
    #     print(f'Successfully exported your query to {destination_uri}')

    # else:
    #     try:
    #         data = client.query(query).to_dataframe()
    #     except Exception as e:
    #         print(f'Failed to execute your query: {query}')
    #         raise(e)

    #     if len(data) > 0:
    #         try:
    #             data.to_csv(destination_file_path, index=False)
    #             print(
    #                 f'Successfully stored query results to {destination_file_path}')
    #         except Exception as e:
    #             print(f'Failed to write the data to csv {destination_file_path}')
    #             raise(e)
    #     else:
    #         print(f'No data was found. File not created')
    #         pass


def get_client(credentials):
    """
    Attempts to create the Google Drive Client with the associated
    environment variables
    """
    try:
        client = bigquery.Client()
        return client
    except Exception as e:
        print(f'Error accessing Google Drive with service account '
              f'{credentials}')
        raise(e)


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    destination_file_name = args.destination_file_name
    destination_folder_name = args.destination_folder_name
    destination_full_path = combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name)
    query = args.query
    bucket = args.bucket

    if tmp_file:
        client = get_client(tmp_file)
    else:
        client = get_client(args.service_account)

    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    create_csv(query=query, client=client,
               destination_file_path=destination_full_path, destination_file_name=destination_file_name,
               bucket=bucket)

    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
