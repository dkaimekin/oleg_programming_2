# Psycopg to work with the Postgres database
# Pylance ignore because of import problems
import psycopg2  # type: ignore
# CSV to update and insert values from a CSV table
import csv


class PostgresWorker():

    greet_message = "\
    1 - SELECT query (select * from <table_name>) |\n\
    2 - Insert from file (path of CSV) |\n\
    3 - Insert data manually |\n\
    4 - Query data (with parameters) |\n\
    5 - Delete data (with parameters) |\n\
    6 - Exit"

    def __init__(self, host, database, user, password):
        self.db_connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password)
        print("Postgres worker initiated!")

    def cursor_init(self):
        self.cursor = self.db_connection.cursor()

    def select_from_table(self, table_name):
        self.cursor.execute(f"SELECT * from {table_name}")

        records = self.cursor.fetchall()

        return records

    # Insert data from a CSV file

    def insert_from_csv(self, file_path, table_name):
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip header row
            for row in reader:
                # Check if phone number already exists in the table
                self.cursor.execute(
                    f"SELECT * FROM {table_name} WHERE phone=%s",
                    (row[2],)
                )

                existing_row = self.cursor.fetchone()
                if existing_row:
                    # If phone number exists, update the row with new values
                    self.cursor.execute(
                        f"UPDATE {table_name} SET name=%s, email=%s WHERE phone=%s",
                        (row[0], row[1], row[2])
                    )
                    print("CSV update successful")
                else:
                    # If phone number doesn't exist, insert a new row
                    self.cursor.execute(
                        f"INSERT INTO {table_name} VALUES ({','.join(['%s']*len(row))})",
                        row
                    )
                    print("CSV insert successful")
        self.db_connection.commit()
        print("Data inserted successfully from CSV")

    # Insert data manually through the terminal

    def insert_manually(self, table_name):
        while True:
            input_str = input(
                f"Enter values for {table_name} (comma-separated): (or exit)\n")
            if input_str.lower() == 'exit':
                break
            row = input_str.split(',')
            # Check if phone number already exists in the table
            self.cursor.execute(
                f"SELECT * FROM {table_name} WHERE phone=%s",
                (row[2],)
            )
            existing_row = self.cursor.fetchone()
            if existing_row:
                # If phone number exists, update the row with new values
                self.cursor.execute(
                    f"UPDATE {table_name} SET name=%s, email=%s WHERE phone=%s",
                    (row[0], row[1], row[2])
                )
                print("Data updated successfully")
            else:
                # If phone number doesn't exist, insert a new row
                self.cursor.execute(
                    f"INSERT INTO {table_name} VALUES ({','.join(['%s']*len(row))})",
                    row
                )
                print("Data inserted successfully")
            self.db_connection.commit()
            print("Data inserted/updated successfully")

    # Query data based on filters
    def query_data(self, table_name, filter_column, filter_value):
        self.cursor.execute(
            f"SELECT * FROM {table_name} WHERE {filter_column}=%s", (filter_value,))
        rows = self.cursor.fetchall()
        if rows:
            print(f"Found {len(rows)} rows matching the filter:")
            for row in rows:
                print(row)
        else:
            print("No rows found matching the filter")

    # Delete data based on filters
    def delete_data(self, table_name, filter_column, filter_value):
        self.cursor.execute(
            f"DELETE FROM {table_name} WHERE {filter_column}=%s", (filter_value,))
        num_deleted = self.cur.rowcount
        self.db_connection.commit()
        print(
            f"Deleted {num_deleted} rows from {table_name} where {filter_column} = '{filter_value}'")


if __name__ == "__main__":
    worker = PostgresWorker(
        host='localhost', database="aluanapp2db", user='admin', password='admin')
    worker.cursor_init()
    # Example usage
    while True:
        print("Please select the option:")
        print(worker.greet_message)
        _input = int(input())
        if _input == 1:
            print(worker.select_from_table(table_name="first_phonebook"))
        elif _input == 2:
            print(
                "Please provide the path of the file (or 'back' to return to main page): ")
            filepath = input()
            try:
                if filepath == "back":
                    continue
                else:
                    worker.insert_from_csv(
                        file_path=filepath, table_name="first_phonebook")
            except FileNotFoundError:
                print("The file could not be found, going back to main page...")
                continue
        elif _input == 3:
            print("Ready to provide info manually? y - yes / n - no")
            is_ready = input()
            ready = True if is_ready == 'y' or is_ready == 'Y' else False
            if ready:
                worker.insert_manually('first_phonebook')
                continue
        elif _input == 6:
            break

    print("Goodbye!")

    # Close communication with the database
    worker.cursor.close()
    worker.db_connection.close()
