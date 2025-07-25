import psycopg2
from psycopg2 import extras, OperationalError, Error
import time
import logging
import random

# Configuring the logging
logging.basicConfig(
    filename='postgres_client.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

class BasePostgresClient:
    def __init__(self, host, dbname, user, password, port=5432, 
                 max_retries=3, initial_delay=1, max_delay=32, exponential_base=2):
        self.connection_params = {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port
        }
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def calculate_delay(self, attempt):
        # Exponential Backoff
        delay = min(
            self.max_delay,
            self.initial_delay * (self.exponential_base ** (attempt - 1))
        )
        # Adding Jitter 
        jitter = random.uniform(-0.1, 0.1) * delay
        return delay + jitter

    def connect(self):
        attempt = 1
        while True:
            try:
                self.conn = psycopg2.connect(**self.connection_params)
                self.cursor = self.conn.cursor(cursor_factory=extras.RealDictCursor)
                logging.info("Connected to the database.")
                break
            except OperationalError as e:
                if attempt >= self.max_retries:
                    logging.error(f"Connection failed after {self.max_retries} attempts: {e}")
                    raise
                delay = self.calculate_delay(attempt)
                logging.warning(f"Connection attempt {attempt} failed. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
                attempt += 1

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logging.info("Connection closed.")

    def execute(self, query, params=None, fetch=False):
        attempt = 1
        while True:
            try:
                self.cursor.execute(query, params)
                if fetch:
                    result = self.cursor.fetchall()
                else:
                    result = None
                self.conn.commit()
                return result
            except OperationalError as e:
                if attempt >= self.max_retries:
                    logging.error(f"Query failed after {self.max_retries} attempts: {e}")
                    raise
                delay = self.calculate_delay(attempt)
                logging.warning(f"Query attempt {attempt} failed. Retrying in {delay:.2f} seconds...")
                self.conn.rollback()
                time.sleep(delay)
                self.connect()  # Re-connecting
                attempt += 1
            except Error as e:
                logging.error(f"Query failed: {e}")
                self.conn.rollback()
                raise

    def execute_batch(self, query, param_list):
        attempt = 1
        while True:
            try:
                extras.execute_batch(self.cursor, query, param_list)
                self.conn.commit()
                break
            except OperationalError as e:
                if attempt >= self.max_retries:
                    logging.error(f"Batch execution failed after {self.max_retries} attempts: {e}")
                    raise
                delay = self.calculate_delay(attempt)
                logging.warning(f"Batch execution attempt {attempt} failed. Retrying in {delay:.2f} seconds...")
                self.conn.rollback()
                time.sleep(delay)
                self.connect()  # Re-connecting
                attempt += 1
            except Error as e:
                logging.error(f"Batch execution failed: {e}")
                self.conn.rollback()
                raise
