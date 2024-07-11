import psycopg2
import sys
import logging
import datetime
import requests ## for API Calls
import airflow ###communicate wit airflow

class DatabaseManager:
    def __init__(self, hostname, database, username, password, port_id):
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.port_id = port_id
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.password,
                port=self.port_id
            )
            self.cur = self.conn.cursor()
            logging.debug("Connected to the database successfully.")
        except Exception as e:
            logging.error(f"Error while connecting to the database: {e}")
            raise

    def create_table(self):
        create_table_script = """
            CREATE TABLE IF NOT EXISTS employee (
                id SERIAL PRIMARY KEY,
                name VARCHAR(40) NOT NULL,
                salary INT,
                dept_id VARCHAR(30)
            )
        """
        try:
            self.cur.execute(create_table_script)
            logging.debug("Table 'employee' created successfully.")
        except Exception as e:
            logging.error(f"Error while creating table: {e}")
            raise

    def execute_sql_file(self, sql_file):
        try:
            with open('insert.sql', 'r') as f:
                sql_queries = f.read()
                self.cur.execute(sql_queries)
            logging.debug(f"SQL queries executed from {sql_file}.")
        except Exception as e:
            logging.error(f"Error while executing SQL queries: {e}")
            raise

    def fetch_employee_data(self):
        try:
            self.cur.execute('SELECT * FROM employee')
            data = self.cur.fetchall()
            logging.debug("Fetched employee data:")
            logging.debug(data)
            return data
        except Exception as e:
            logging.error(f"Error while fetching employee data: {e}")
            raise

    def commit(self):
        try:
            self.conn.commit()
            logging.debug("Changes committed to the database.")
        except Exception as e:
            logging.error(f"Error while committing changes: {e}")
            raise

    def close_connection(self):
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
        logging.debug("Connection to the database closed.")

if __name__ == "__main__":
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_filename = f"pg_{current_date}.log"
    logging.basicConfig(filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    hostname = ''
    database = ''
    username = ''
    pwd = ''
    port_id = 

    db_manager = DatabaseManager(hostname, database, username, pwd, port_id)
    db_manager.connect()
    db_manager.create_table()

    try:
        sql_file = sys.argv[1]
        db_manager.execute_sql_file(sql_file)
    except IndexError:
        logging.warning("No SQL file provided.")

    db_manager.fetch_employee_data()
    db_manager.commit()
    db_manager.close_connection()
