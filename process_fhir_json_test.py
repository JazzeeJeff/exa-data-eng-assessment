import unittest
from unittest.mock import patch, MagicMock

import psycopg2
import pandas
from process_fhir_json import (
    create_con,
    create_tables,
    filter_dataframe_by_resource_type,
    RESOURCE_TYPES,
)


class TestProcessFHIRJson(unittest.TestCase):
    def setUp(self):
        self.test_dbname = "test_dbname"
        self.test_user = "test_user"
        self.test_password = "test_password"
        self.test_host = "test_host"
        self.test_port = "test_port"

    @patch("process_fhir_json.psycopg2.connect")
    def test_create_con(self, mock_connect):
        # Set up the mock object to return a connection object
        mock_connection = mock_connect.return_value
        # Mock the close method of the connection object
        mock_connection.close = MagicMock()

        # Test database connection creation
        con = create_con(
            dbname=self.test_dbname,
            user=self.test_user,
            password=self.test_password,
            host=self.test_host,
            port=self.test_port
        )
        self.assertIsNotNone(con)

        # Verify that the close method of the connection object is called
        con.close()
        mock_connection.close.assert_called_once()

    @patch("process_fhir_json.psycopg2.connect")
    def test_create_tables(self, mock_connect):
        # Create a mock connection object
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value

        # Mock data
        list_of_dictionaries = [
            {"Table1": pandas.DataFrame({"col1": ["data1", "data2"], "col2": ["data3", "data4"]})},
            {"Table2": pandas.DataFrame({"col1": ["data5", "data6"], "col2": ["data7", "data8"]})}
        ]

        # Mock the execute_values method
        mock_execute_values = MagicMock()
        psycopg2.extras.execute_values = mock_execute_values

        # Mock logger
        mock_logger = MagicMock()

        # Run the function
        create_tables(mock_connection, list_of_dictionaries, mock_logger)

        # Assert that cursor and connection methods were called
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_execute_values.assert_called()
        mock_logger.error.assert_not_called()

        # Check autocommit value directly
        self.assertTrue(mock_connection.autocommit)

    def test_filter_dataframe_by_resource_type(self):
        # Sample data
        data = {
            "resource_resourceType": ["Patient", "Encounter", "Patient", "Condition", "Condition", "Observation"],
            "other_column": [1, 2, 3, 4, 5, 6]
        }
        df = pandas.DataFrame(data)

        # Call the function
        result = filter_dataframe_by_resource_type(df)

        # Assert that the result is a list
        self.assertIsInstance(result, list)

        # Assert the length of the result list
        self.assertEqual(len(result), 4)  # Assuming 4 different resource types in the sample data

        # Assert each item in the result list is a dictionary
        for item in result:
            self.assertIsInstance(item, dict)

            # Assert each dictionary contains only one key
            self.assertEqual(len(item), 1)

            # Assert that the key is a string and exists in the RESOURCE_TYPES list
            resource_type = list(item.keys())[0]
            self.assertIsInstance(resource_type, str)
            self.assertIn(resource_type, RESOURCE_TYPES)

            # Assert that the value associated with the key is a DataFrame
            resource_df = item[resource_type]
            self.assertIsInstance(resource_df, pandas.DataFrame)

            # Assert that the DataFrame has only rows corresponding to the resource type
            self.assertTrue((resource_df["resource_resourceType"] == resource_type).all())


if __name__ == "__main__":
    unittest.main()
