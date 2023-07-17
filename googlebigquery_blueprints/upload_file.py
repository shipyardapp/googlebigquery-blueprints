import os
import re
import json
import glob
import tempfile
import argparse
import ast
import pandas as pd
import sys
import shipyard_utils as shipyard

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_DATASET = 201
EXIT_CODE_INVALID_SCHEMA = 202
EXIT_CODE_SCHEMA_MISMATCH = 203
EXIT_CODE_FILE_NOT_FOUND = 204


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', dest='dataset', required=True)
    parser.add_argument('--table', dest='table', required=True)
    parser.add_argument(
        '--service-account',
        dest='service_account',
        required=True)
    parser.add_argument(
        '--upload-type',
        dest='upload_type',
        default='append',
        choices={
            'append',
            'overwrite'},
        required=False)
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        default='exact_match',
        choices={
            'exact_match',
            'regex_match'},
        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--schema',
        dest='schema',
        default='',
        required=False
    )
    parser.add_argument(
        '--skip-header-rows',
        dest='skip_header_rows',
        default='',
        required=False
    )
    parser.add_argument('--quoted-newline', dest = 'quoted_newline', default = False, required = False)
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


def string_to_boolean(value):
    if isinstance(value, bool):
        return value
    if value.lower() in ('true', 't', 'y'):
        return True
    elif value.lower() in ('false', 'f', 'n'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def find_all_local_file_names(source_folder_name):
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.
    """
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f'{cwd}/{source_folder_name}/**')
    file_names = glob.glob(cwd_extension, recursive=True)
    return [file_name for file_name in file_names if os.path.isfile(file_name)]


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for _file in file_names:
        if re.search(file_name_re, _file):
            matching_file_names.append(_file)

    return matching_file_names


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path
    variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def copy_from_csv(
        client,
        dataset,
        table,
        source_file_path,
        upload_type,
        schema=None,
        skip_header_rows=None,
        quoted_newline=False):
    """
    Copy CSV data into Bigquery table.
    """
    try:
        dataset_ref = client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.LoadJobConfig()
        if upload_type == 'overwrite':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.CSV
        if skip_header_rows:
            job_config.skip_leading_rows = skip_header_rows
        if schema:
            job_config.autodetect = False
            job_config.schema = format_schema(schema)
        else:
            job_config.autodetect = True
        if quoted_newline:
            job_config.allow_quoted_newlines = True
        with open(source_file_path, 'rb') as source_file:
            job = client.load_table_from_file(source_file, table_ref,
                                              job_config=job_config)
        job.result()
    except NotFound as nf_e:
        if 'Not found: Dataset' in str(nf_e):
            print(
                f'The dataset {dataset} could not be found. Please check for typos and try again')
            print(nf_e)
            sys.exit(EXIT_CODE_INVALID_DATASET)
    except Exception as e:
        if ('Invalid value for mode' or 'Invalid value for type') in str(e):
            print(
                'The provided schema was not valid. Please check to make sure that the provided schema matches the following format and accepted values. \n\n \
                Format: [["column","datatype"],["column","datatype","MODE"]] \
                Accepted Values: https://cloud.google.com/bigquery/docs/schemas')
            print(e)
            sys.exit(EXIT_CODE_INVALID_SCHEMA)
        if 'Provided Schema does not match' in str(e):
            print('The provided schema does not match the schema for the existing table. Please check your table and ensure that the column names and data types match up exactly.')
            print(e)
            sys.exit(EXIT_CODE_SCHEMA_MISMATCH)
        else:
            print(f'Failed to copy CSV {source_file_path} to BigQuery.')
            print(e)
            sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    print(
        f'Successfully copied csv {source_file_path} to {dataset}.{table} on BigQuery')


def get_client(credentials):
    """
    Attempts to create the Google Bigquery Client with the associated
    environment variables
    """
    try:
        client = bigquery.Client()
        return client
    except Exception as e:
        print(f'Error accessing Google Bigquery with service account '
              f'{credentials}')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)


def format_schema(schema):
    formatted_schema = []
    schema = ast.literal_eval(schema)
    for item in schema:
        schema_column = 'bigquery.SchemaField("'
        for value in item:
            schema_column += value + '","'
        schema_column += '")'
        formatted_schema.append(eval(schema_column))
    return formatted_schema


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    dataset = args.dataset
    table = args.table
    upload_type = args.upload_type
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
        folder_name=f'{os.getcwd()}/{source_folder_name}',
        file_name=source_file_name)
    source_file_name_match_type = args.source_file_name_match_type
    schema = args.schema
    quoted_newline = shipyard.args.convert_to_boolean(args.quoted_newline)

    skip_header_rows = args.skip_header_rows

    if skip_header_rows:
        skip_header_rows = int(args.skip_header_rows)

    if tmp_file:
        client = get_client(tmp_file)
    else:
        client = get_client(args.service_account)

    if source_file_name_match_type == 'regex_match':
        file_names = find_all_local_file_names(source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, file_name in enumerate(matching_file_names):
            print(f'Uploading file {index+1} of {len(matching_file_names)}')
            copy_from_csv(
                client=client,
                dataset=dataset,
                table=table,
                source_file_path=file_name,
                upload_type=upload_type,
                schema=schema,
                skip_header_rows=skip_header_rows,
                quoted_newline=quoted_newline)
    else:
        if not os.path.isfile(source_full_path):
            print(f'File {source_full_path} does not exist')
            sys.exit(EXIT_CODE_FILE_NOT_FOUND)

        copy_from_csv(
            client=client,
            dataset=dataset,
            table=table,
            source_file_path=source_full_path,
            upload_type=upload_type,
            schema=schema,
            skip_header_rows=skip_header_rows,
            quoted_newline=quoted_newline)

    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
