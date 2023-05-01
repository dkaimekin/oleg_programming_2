import psycopg2  # type: ignore
import csv
import re

from ..laboratory_10.main import PostgresWorker


class AdvancedPostgresWorker(PostgresWorker):
    def __init__(self, host, database, user, password):
        PostgresWorker.__init__(
            host=host, database=database, user=user, password=password)

    # Extract data based on a regex pattern
    def extract_data_by_pattern(self, table_name, column_name, regex_pattern):
        # Escape any special characters in the regex pattern
        regex_pattern = re.escape(regex_pattern)
        # Construct the query with a regex comparison operator
        query = f"SELECT * FROM {table_name} WHERE {column_name} ~* '{regex_pattern}'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if rows:
            print(f"Found {len(rows)} rows matching the regex pattern:")
            for row in rows:
                print(row)
        else:
            print("No rows found matching the regex pattern")

    # Create procedure to insert new user and update phone if user exists
    def create_insert_or_update_user_proc(self):
        self.cursor.execute("""
            CREATE OR REPLACE PROCEDURE insert_or_update_user(
                IN p_first_name VARCHAR(50),
                IN p_phone VARCHAR(20)
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF EXISTS (SELECT 1 FROM users WHERE first_name = p_first_name) THEN
                    UPDATE users SET phone = p_phone WHERE first_name = p_first_name;
                    RAISE NOTICE 'Updated phone number for user %', p_first_name;
                ELSE
                    INSERT INTO users (first_name, phone) VALUES (p_first_name, p_phone);
                    RAISE NOTICE 'Inserted new user %', p_first_name;
                END IF;
            END;
            $$;
        """)
        self.db_connection.commit()
        print("Stored procedure created successfully")

    # Call the insert_or_update_user procedure
    # WARNING: Only works with first name
    def call_insert_or_update_procedure(self, data: tuple):
        self.cursor.callproc('insert_or_update_user', data)
        self.db_connection.commit()
        print("Successfully called insert_or_update_user procedure")

    # Insert or update multiple rows of data
    def call_insert_or_update_procedure_list(self, data: list):
        for tup in data:
            self.call_insert_or_update_procedure(tup)

    # Define a function to query data with pagination using limit and offset
    def query_data_with_pagination(self, query, limit, offset):
        self.cursor.execute(query + f" LIMIT {limit} OFFSET {offset}")
        rows = self.cursor.fetchall()
        return rows
