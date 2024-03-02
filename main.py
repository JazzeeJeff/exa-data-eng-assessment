import contextlib
import json
import logging
import os
import typing
from typing import Dict, Any

import pandas
import psycopg2
import pandas as pd
from pandas import DataFrame

DBNAME = "postgres"
USER = "postgres"
PASSWORD = "my_password"
HOST = "localhost"
PORT = "5432"
FHIR_JSON_DIRECTORY = "data"
RESOURCE_TYPES = [
    "Patient",
    "Encounter",
    "Condition",
    "DiagnosticReport",
    "ExplanationOfBenefit",
    "MedicationRequest",
    "CareTeam",
    "CarePlan",
    "Procedure",
    "Immunization",
    "Observation",
    "Provenance",
    "Device",
]


def create_con(dbname: str, user: str, password: str, host: str, port: str):
    con = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    return con


def read_json_file(file_path):
    with open(file_path, "r", encoding="utf8") as file:
        fhir_data = json.load(file)
    return fhir_data


def normalize_nested_json(data):
    df = pd.json_normalize(data, record_path=["entry"], max_level=2, sep="_")
    return df


def get_fhir_json(directory: str) -> typing.List[typing.Dict]:
    all_json = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            json_data = read_json_file(file_path)
            all_json.append(json_data)
    return all_json


def filter_dataframe_by_resource_type(dataframe: pandas.DataFrame) -> dict[Any, DataFrame]:
    resource_type = dataframe.resource_resourceType.unique()

    df_dict_resource_types = {elem: pd.DataFrame() for elem in resource_type}

    for key in df_dict_resource_types.keys():
        df_dict_resource_types[key] = dataframe[:][dataframe.resource_resourceType == key]
    return df_dict_resource_types


def main():
    scripts_logger = logging.getLogger()
    with contextlib.closing(
        create_con(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    ) as db_con:
        fhir_json = get_fhir_json(directory=FHIR_JSON_DIRECTORY)
        normalise_json_df = normalize_nested_json(fhir_json)
        filtered_df = filter_dataframe_by_resource_type(normalise_json_df)
        create_tables =


if __name__ == "__main__":
    main()
