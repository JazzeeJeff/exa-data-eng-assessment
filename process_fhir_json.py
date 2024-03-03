import contextlib
import json
import logging
import os
import typing
import psycopg2.extras
import pandas
import psycopg2

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
    """
    Create a connection to a PostgreSQL database.

    Args:
        dbname (str): The name of the database.
        user (str): The username for database authentication.
        password (str): The password for database authentication.
        host (str): The host address of the database server.
        port (str): The port number of the database server.

    Returns:
        psycopg2.extensions.connection: A connection object to the PostgreSQL database.

    """
    # Establish a connection to the PostgreSQL database using psycopg2
    con = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    return con


def read_json_file(file_path):
    """
    Read JSON data from a file.

    Args:
        file_path (str): The path to the JSON file to be read.

    Returns:
        dict: A dictionary containing the JSON data from the file.

    """
    # Open the JSON file in read mode
    with open(file_path, "r", encoding="utf8") as file:
        # Load JSON data from the file into a dictionary
        fhir_data = json.load(file)
    return fhir_data


def normalize_nested_json(data: typing.List[typing.Dict]) -> pandas.DataFrame:
    """
    Normalize nested JSON data into a pandas DataFrame.

    Args:
        data: Nested JSON data to be normalized.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the normalized data.

    """
    # Normalize the nested JSON data into a DataFrame
    # Using record_path=["entry"] to specify the path to the nested records
    # Setting max_level=2 to control the depth of normalization
    # Using sep="_" to separate nested keys in the resulting DataFrame
    df = pandas.json_normalize(data, record_path=["entry"], max_level=2, sep="_")
    return df


def get_fhir_json(directory: str) -> typing.List[typing.Dict]:
    """
    Retrieve FHIR JSON data from files in the specified directory.

    Args:
        directory (str): The directory path where FHIR JSON files are located.

    Returns:
        typing.List[typing.Dict]: A list containing dictionaries, each representing
        the JSON data from a file.

    """
    # Initialize an empty list to store all JSON data
    all_json = []

    # Iterate through files in the specified directory
    for filename in os.listdir(directory):
        # Check if the file is a JSON file
        if filename.endswith(".json"):
            # Construct the full path to the file
            file_path = os.path.join(directory, filename)
            # Read JSON data from the file
            json_data = read_json_file(file_path)  # Assuming read_json_file is defined elsewhere
            # Append the JSON data to the list
            all_json.append(json_data)

    return all_json


def filter_dataframe_by_resource_type(dataframe: pandas.DataFrame) -> list[typing.Dict[str, str]]:
    """
    Filter a DataFrame by resource type and return a list of dictionaries containing
    DataFrames corresponding to each resource type.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be filtered.

    Returns:
        list[Dict[str, Any]]: A list of dictionaries where each dictionary contains
        a DataFrame corresponding to a unique resource type.

    """
    # Extract unique resource types from the DataFrame
    resource_types = dataframe.resource_resourceType.unique()
    # Initialize an empty list to store dictionaries of DataFrames
    df_list_resource_types = []
    # Iterate through each unique resource type
    for resource_type in resource_types:
        # Filter the DataFrame by the current resource type
        df_filtered = dataframe[dataframe.resource_resourceType == resource_type]
        # Create a dictionary where the key is the resource type and the value is the filtered DataFrame
        df_dict = {resource_type: df_filtered}
        # Append the dictionary to the list
        df_list_resource_types.append(df_dict)

    return df_list_resource_types


def create_tables(con, list_of_dictionaries: typing.List[typing.Dict], logger: logging.Logger):
    """
    Create tables in the database based on the provided dictionaries containing DataFrames.

    Args:
        con (psycopg2.extensions.connection): The connection to the PostgreSQL database.
        list_of_dictionaries (List[Dict]): A list of dictionaries containing DataFrames.

    Returns:
        None

    Raises:
        psycopg2.errors: Exceptions raised by psycopg2 while interacting with the database.

    """
    try:
        # Set autocommit to True to execute the queries immediately
        con.autocommit = True

        # Create a new cursor to execute queries
        cursor = con.cursor()

        # Iterate through each dictionary in the list
        for dictionary in list_of_dictionaries:
            # Iterate through each key-value pair in the dictionary
            for resource_type, df in dictionary.items():
                # Construct the column definitions for creating the table
                cols = ", ".join(
                    [f"{col} TEXT" for col in df.columns]
                )

                # Construct the CREATE TABLE query
                create_table_query = f"CREATE TABLE IF NOT EXISTS {resource_type} ({cols});"

                # Execute the CREATE TABLE query
                cursor.execute(create_table_query)

                # Convert DataFrame rows to tuples for bulk insert
                values = [tuple(row) for row in df.itertuples(index=False)]
                # Convert any JSON-like data types to string representation for insertion
                values = [tuple(json.dumps(item) if isinstance(item, (dict, list)) else item for item in row) for row in values]

                # Construct the INSERT INTO query with placeholders for bulk insert
                insert_query = f"INSERT INTO {resource_type} ({', '.join(df.columns)}) VALUES %s;"

                # Execute the bulk insert query using psycopg2's execute_values
                psycopg2.extras.execute_values(cursor, insert_query, values)

                # Print success message
                print(f"Table '{resource_type}' created and data inserted successfully.")

        # Close the cursor and connection
        cursor.close()
        con.close()

    except psycopg2.Error as e:
        # Handle any exceptions raised by psycopg2
        logger.error(f"Data insertion failure. Error: {e}")


def process_fhir_json():
    logger = logging.getLogger()
    logger.info("Starting fhir message processing.")
    fhir_json = get_fhir_json(directory=FHIR_JSON_DIRECTORY)
    if not fhir_json:
        raise ValueError("No json files received from directory.")
    normalise_json_df = normalize_nested_json(fhir_json)
    filtered_df = filter_dataframe_by_resource_type(normalise_json_df)
    with contextlib.closing(
        create_con(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    ) as con:
        create_tables(con, list_of_dictionaries=filtered_df, logger=logger)
        logger.info("Tables created and fhir json data processed.")


if __name__ == "__main__":
    process_fhir_json()
