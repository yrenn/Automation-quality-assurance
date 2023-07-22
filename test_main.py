import unittest
from unittest.mock import patch, MagicMock
from io import StringIO

import pyodbc 
import pandas as pd
import csv
import numpy as np
import snowflake.connector
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import datacompy
from datetime import datetime
import pytz
import logging

class TestGetSrcTable(unittest.TestCase):

    @patch('my_module.pd.read_sql')
    def test_getSrcTable_successful(self, mock_read_sql):
        # Mock the return value of pd.read_sql to avoid an actual database call
        expected_result = {'col1': [1, 2], 'col2': [3, 4]}
        mock_read_sql.return_value = expected_result

        # Mock the database connection object (you can use MagicMock if needed)
        connection_mock = None

        # Call the function with mock objects
        result_df = getSrcTable(connection_mock, 'SELECT * FROM table_name', 'src_table')

        # Assert that the function returns the expected DataFrame
        self.assertEqual(result_df.to_dict(), expected_result)

        # Assert that the logging message is correct
        expected_log_message = "***** get table src_table from sql server *****"
        self.assertEqual(expected_log_message, logging_info_mock.call_args[0][0])

    @patch('my_module.pd.read_sql')
    def test_getSrcTable_exception_handling(self, mock_read_sql):
        # Mock the exception raised by pd.read_sql
        mock_read_sql.side_effect = Exception("Database connection error")

        # Mock the database connection object (you can use MagicMock if needed)
        connection_mock = None

        # Call the function with mock objects
        result_df = getSrcTable(connection_mock, 'SELECT * FROM table_name', 'src_table')

        # Assert that the function returns None when an exception occurs
        self.assertIsNone(result_df)

        # Assert that the error message is logged correctly
        expected_log_message = "fail to read or run the sql query in sql serverDatabase connection error"
        self.assertEqual(expected_log_message, logging_error_mock.call_args[0][0])

class TestGetTarTable(unittest.TestCase):

    @patch('my_module.pd.read_sql')
    def test_getTarTable_successful(self, mock_read_sql):
        # Mock the return value of pd.read_sql to avoid an actual database call
        expected_result = {'col1': [1, 2], 'col2': [3, 4]}
        mock_read_sql.return_value = pd.DataFrame(expected_result)

        # Mock the database connection object (you can use MagicMock if needed)
        connection_mock = MagicMock()

        # Call the function with mock objects
        result_df = getTarTable(connection_mock, 'SELECT * FROM table_name', 'tar_table')

        # Assert that the function returns the expected DataFrame
        self.assertEqual(result_df.to_dict(), expected_result)

        # Assert that the logging message is correct
        logging_info_mock.assert_called_once_with("***** get table tar_table from sql server *****")

    @patch('my_module.pd.read_sql')
    def test_getTarTable_exception_handling(self, mock_read_sql):
        # Mock the exception raised by pd.read_sql
        mock_read_sql.side_effect = Exception("Database connection error")

        # Mock the database connection object (you can use MagicMock if needed)
        connection_mock = MagicMock()

        # Call the function with mock objects
        result_df = getTarTable(connection_mock, 'SELECT * FROM table_name', 'tar_table')

        # Assert that the function returns None when an exception occurs
        self.assertIsNone(result_df)

        # Assert that the error message is logged correctly
        logging_error_mock.assert_called_once_with("fail to read or run the sql query in snowflakeDatabase connection error")




class TestWriteReport(unittest.TestCase):

    @patch('my_module.open')
    @patch('my_module.logging.info')
    def test_writeReport(self, mock_logging_info, mock_open):
        # Input test data
        src_name = "source_table"
        tar_name = "target_table"
        report = "This is a sample report."

        # Mock the timezone and current datetime
        mock_tz = pytz.timezone('America/TORONTO')
        mock_now = datetime(2023, 7, 21, 12, 34, 56, tzinfo=mock_tz)
        with patch('my_module.datetime') as mock_datetime:
            mock_datetime.now.return_value = mock_now

            # Call the function
            writeReport(src_name, tar_name, report)

            # Assert that the filename is correctly generated
            expected_filename = 'Report_source_table_12_34_56.txt'
            mock_open.assert_called_once_with(expected_filename, 'w')

            # Assert that the report content is correctly written to the file
            mock_file = mock_open().__enter__.return_value
            mock_file.write.assert_called_once_with(report)

            # Assert that the logging message is correct
            expected_log_message = "===== write comparison report for source_table ====="
            mock_logging_info.assert_called_once_with(expected_log_message)




class TestCompareTables(unittest.TestCase):

    @patch('my_module.open')
    @patch('my_module.logging.error')
    @patch('my_module.getSrcTable')
    @patch('my_module.getTarTable')
    @patch('my_module.datacompy.Compare')
    @patch('my_module.writeReport')
    def test_compareTables_successful(self, mock_writeReport, mock_Compare, mock_getTarTable, mock_getSrcTable, mock_logging_error, mock_open):
        # Input test data
        src_connection_mock = MagicMock()
        tar_connection_mock = MagicMock()
        mock_reader = MagicMock()
        mock_reader.__enter__.return_value = mock_reader
        mock_reader.__iter__.return_value = [
            {'SOURCETABLE': 'source_table1', 'SRCCOL': 'src_col1', 'TARGETTABLE': 'target_table1', 'TGTCOL': 'tgt_col1'},
            {'SOURCETABLE': 'source_table2', 'SRCCOL': 'src_col2', 'TARGETTABLE': 'target_table2', 'TGTCOL': 'tgt_col2'}
        ]

        mock_open.return_value = mock_reader
        mock_getSrcTable.return_value = pd.DataFrame({'src_col1': [1, 2, 3], 'src_col2': [4, 5, 6]})
        mock_getTarTable.return_value = pd.DataFrame({'tgt_col1': [1, 2, 3], 'tgt_col2': [4, 5, 6]})
        mock_Compare.return_value.matches.return_value = True

        # Call the function
        compareTables(src_connection_mock, tar_connection_mock)

        # Assert that the function reads the CSV file and processes each row correctly
        mock_open.assert_called_once_with('config.csv', newline='\n')
        self.assertEqual(mock_reader.__iter__.call_count, 1)

        # Assert that the expected queries are generated
        expected_src_que = 'select src_col1 from source_table1;'
        expected_tar_que = 'select tgt_col1 from target_table1;'
        mock_getSrcTable.assert_called_once_with(src_connection_mock, expected_src_que, 'source_table1')
        mock_getTarTable.assert_called_once_with(tar_connection_mock, expected_tar_que, 'target_table1')

        # Assert that the comparison is performed and logging is done accordingly
        self.assertEqual(mock_Compare.call_count, 1)
        mock_Compare.return_value.matches.assert_called_once_with()
        mock_logging_error.assert_not_called()
        self.assertEqual(mock_writeReport.call_count, 1)
            