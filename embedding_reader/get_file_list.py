"""get_file_list module gets the file list from a path for both readers"""

import fsspec
from typing import List, Tuple, Union
import os
import requests


def get_file_list(embeddings_folder: str, api_endpoint: str, file_format: str) -> Tuple[fsspec.AbstractFileSystem, List[str]]:
    """
    Get the file system and all the file paths through api_endpoint.

    :raises ValueError: if file system is inconsistent under different folders.
    """
    return _get_file_list(embeddings_folder, api_endpoint, file_format)


def get_embedding_paths(api_endpoint: str) -> List[str]:
    response = requests.get(api_endpoint)
    if response.status_code != 200:
        raise ValueError(f"Failed to get embedding paths from {api_endpoint}")
    data = response.json()["data"]
    paths = [record['key'] for record in data]
    return paths


def filter_parquet_files(file_list, extension='parquet') -> List[str]:
    if not extension.startswith('.'):
        extension = '.' + extension

    return [file for file in file_list if file.endswith(extension)]


def _get_file_list(
    s3_bucket: str, api_endpoint: str, file_format: str, sort_result: bool = True
) -> Tuple[fsspec.AbstractFileSystem, List[str]]:
    """Get the file system and all the file paths that matches `file_format` given a single path."""
    file_paths = get_embedding_paths(api_endpoint)
    file_paths = filter_parquet_files(file_paths)
    fs, _ = fsspec.core.url_to_fs(s3_bucket)
    prefix = s3_bucket.rstrip("/")
    file_paths_with_prefix = [os.path.join(prefix, file_path) for file_path in file_paths]

    if sort_result:
        file_paths_with_prefix.sort()

    return fs, file_paths_with_prefix