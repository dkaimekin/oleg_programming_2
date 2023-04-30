# Psycopg to work with the Postgres database
import psycopg2
# CSV to update and insert values from a CSV table
import csv


class PostgresWorker():

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
    table_name = "first_phonebook"
    worker.insert_manually(table_name)

    worker.query_data('first_phonebook', 'last_name', ' Markarth')
    # Close communication with the database
    worker.cursor.close()
    worker.db_connection.close()
