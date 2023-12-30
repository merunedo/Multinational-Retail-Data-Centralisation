# psycopg2 for connecting to PostgreSQL databases
# yaml for parsing YAML files to read database credentials
import psycopg2
import yaml

class DataExtractor:

    # Constructor that initializes the class with database configuration
    def __init__(self, db_config_path):
        # Load database credentials from the provided YAML file path
        with open(db_config_path, 'r') as config_file:
            self.db_config = yaml.safe_load(config_file)
        # Establish a database connection
        self.db_connection = self._connect_to_db()

    # Private method to establish connection to the database
    def _connect_to_db(self):
        # Connect to the database using credentials
        return psycopg2.connect(
            host=self.db_config['RDS_HOST'],
            user=self.db_config['RDS_USER'],
            password=self.db_config['RDS_PASSWORD'],
            database=self.db_config['RDS_DATABASE'],
            port=self.db_config['RDS_PORT']
        )

    # Method to extract data from the database using a SQL query
    def extract_from_db(self, query):
        # Execute the query and return the results
        with self.db_connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    # Method to close the database connection
    def close_db_connection(self):
        # Close the database connection if it's open
        if self.db_connection:
            self.db_connection.close()

# Example usage:
# Provide the path to your YAML file that contains database credentials
db_config_path = 'db_creds.yaml'
extractor = DataExtractor(db_config_path=db_config_path)

# Here you will write your SQL query to extract data
query = 'SELECT * FROM your_table_name'
db_data = extractor.extract_from_db(query)

# Print the data to see if it's working
print(db_data)

# Don't forget to close the connection when done
extractor.close_db_connection()
