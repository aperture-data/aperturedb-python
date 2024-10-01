#!/usr/bin/env python3

"""
This file contains some utilities to help with converting existing CSV files
to the format required by ApertureDB.  The main functions are for writing
CSV files for creating entities, images, and connections.  The functions
take in a pandas DataFrame and write it to a CSV file with the appropriate
headers required by the various *CSV loaders in ApertureDB.  This makes it
easier to generate CSV files without having to remember the exact column
names required by the CSV loaders.
"""

from typing import Optional

import pandas as pd


def convert_entity_data(input, entity_class: str, unique_key: Optional[str] = None):
    """
    Convert data to the format required for creating entities

    Arguments:
        input: Anything that can be used as input to a pandas DataFrame, including a pandas DataFrame
        entity_class: The entity class to write to the CSV file as the first column
        unique_key: (optional) An existing key to call out as a constraint

    Returns:
        A pandas DataFrame with the entity class as the first column
    """
    df = pd.DataFrame(input)
    df.insert(0, 'EntityClass', entity_class)
    if unique_key:
        assert unique_key in df.columns, f"unique_key {unique_key} not found in the input data"
        df[f"constraint_{unique_key}"] = df[unique_key]
    return df


def write_entity_csv(filename: str, input, **kwargs):
    """
    Write data to a CSV file for creating entities

    Arguments:
        filename: The name of the file to write to.
            Recommended to end in ".entity.csv".
        input: Anything that can be used as input to a pandas DataFrame, including a pandas DataFrame
        entity_class: The entity class to write to the CSV file as the first column
        unique_key: (optional) An existing key to call out as a constraint
    """
    df = convert_entity_data(input, **kwargs)
    df.to_csv(filename, index=False)


def convert_image_data(input, source_column: str, source_type: Optional[str] = None, format: Optional[str] = None, unique_key: Optional[str] = None):
    """
    Convert data to the format required for creating images

    Arguments:
        input: Anything that can be used as input to a pandas DataFrame, including a pandas DataFrame
        source_column: The name of the column that contains the image data
        source_type: (optional) The type the source column.  If not specified, the source column will be used.  Should be one of "filename", "url", "gsurl", or "s3url".
        format: (optional) The format of the image data.  If not provided, there should be a column called "format" in the input data.
        unique_key: (optional) An existing key to call out as a constraint

    Returns:
        A pandas DataFrame with the source column as the first column
    """
    df = pd.DataFrame(input)

    assert source_column in df.columns, f"source_column {source_column} not found in the input data"

    if source_type is None:
        source_type = source_column

    assert source_type in ["filename", "url", "gsurl",
                           "s3url"], f"source_type must be one of 'filename', 'url', 'gsurl', or 's3url', found: {source_type}"

    if source_column == source_type:
        # reordering the columns to make the source column the first column
        df = df[[source_column] +
                [col for col in df.columns if col != source_column]]
    else:
        df.insert(0, source_type, df[source_column])

    if unique_key is not None:
        assert unique_key in df.columns, f"unique_key {unique_key} not found in the input data"
        df[f"constraint_{unique_key}"] = df[unique_key]

    if format is not None:
        assert 'format' not in df.columns, "format column already exists in the input data"
        df['format'] = format
    else:
        assert 'format' in df.columns, "format column not found in the input data"
        # Reorder the columns to make the format column the last column
        df = df[[col for col in df.columns if col != 'format'] + ['format']]

    return df


def write_image_csv(filename: str, input, **kwargs):
    """
    Write data to a CSV file for creating images

    Arguments:
        filename: The name of the file to write to.
            Recommended to end in ".image.csv".
        input: Anything that can be used as input to a pandas DataFrame, including a pandas DataFrame
        source_column: The name of the column that contains the image data
        source_type: (optional) The type the source column.  If not specified, the source column will be used.  Should be one of "filename", "url", "gsurl", or "s3url".
        format: (optional) The format of the image data.  If not provided, there should be a column called "format" in the input data.
        unique_key: (optional) An existing key to call out as a constraint
    """
    df = convert_image_data(input, **kwargs)
    df.to_csv(filename, index=False)


def convert_connection_data(input,
                            connection_class: str,
                            source_class: str, source_property: str,
                            destination_class: str, destination_property: str,
                            source_column: Optional[str] = None, destination_column: Optional[str] = None,
                            unique_key: Optional[str] = None):
    """
    Convert data to the format required for creating connections

    Arguments:
        input: Anything that can be used as input to a pandas DataFrame, including a pandas DataFrame
        connection_class: The connection class to write to the CSV file as the first column
        source_class: The source entity class
        source_property: The property containing the source key in ApertureDB
        destination_class: The destination entity class
        destination_property: The property of the destination entity in ApertureDB
        source_column: (optional) The column containing the source keys in the input data.  Defaults to the source_property.
        destination_column: (optional) The column containing the destination keys in the input data.  Defaults to the destination_property.
        unique_key: (optional) An existing key to call out as a constraint
        """
    assert "@" not in source_class, "source_class should not contain '@'"
    assert "@" not in destination_class, "destination_class should not contain '@'"

    df = pd.DataFrame(input)

    if source_column is None:
        source_column = source_property
    assert source_column in df.columns, f"source_column {source_column} not found in the input data"

    if destination_column is None:
        destination_column = destination_property
    assert destination_column in df.columns, f"destination_column {destination_column} not found in the input data"

    df.insert(0, 'ConnectionClass', connection_class)
    df.insert(1, f"{source_class}@{source_property}", df[source_column])
    df.insert(2, f"{destination_class}@{destination_property}",
              df[destination_column])

    if unique_key:
        assert unique_key in df.columns, f"unique_key {unique_key} not found in the input data"
        df[f"constraint_{unique_key}"] = df[unique_key]

    return df


def write_connection_csv(filename: str, input, **kwargs):
    """
    Write data to a CSV file for creating connections

    Arguments:
        filename: The name of the file to write to.
            Recommended to end in ".connection.csv".
        input: Anything that can be used as input to a pandas DataFrame, including a pandas DataFrame
        connection_class: The connection class to write to the CSV file as the first column
        source_class: The source entity class
        source_property: The property containing the source key in ApertureDB
        source_column: (optional) The column containing the source keys in the input data.  Defaults to the source_property.
        destination_class: The destination entity class
        destination_property: The property of the destination entity in ApertureDB
        destination_column: (optional) The column containing the destination keys in the input data.  Defaults to the destination_property.
        unique_key: (optional) An existing key to call out as a constraint
    """
    df = convert_connection_data(input, **kwargs)
    df.to_csv(filename, index=False)
