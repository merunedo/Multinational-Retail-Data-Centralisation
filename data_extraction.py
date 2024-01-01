import yaml
import pandas as pd
from sqlalchemy import create_engine, MetaData

# DatabaseConnector class handles connecting to the database
class DatabaseConnector:
    def __init__(self, creds_file):
        self.db_creds = self.read_db_creds(creds_file)  # Read the database credentials
        self.engine = self.init_db_engine()  # Initialize the database engine

    # This static method reads the credentials from a YAML file
    @staticmethod
    def read_db_creds(creds_file):
        with open(creds_file, 'r') as file:
            credentials = yaml.safe_load(file)  # yaml.safe_load converts YAML into a Python dictionary
        return credentials

    # This method initializes the database engine using credentials from the YAML file
    def init_db_engine(self):
        user = self.db_creds['RDS_USER']
        password = self.db_creds['RDS_PASSWORD']
        host = self.db_creds['RDS_HOST']
        port = self.db_creds['RDS_PORT']
        db = self.db_creds['RDS_DATABASE']
        connection_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
        engine = create_engine(connection_string)
        return engine

    # This method retrieves a list of all table names in the database
    def list_db_tables(self):
        meta = MetaData()
        meta.reflect(bind=self.engine)
        return meta.tables.keys()

# DataExtractor class is used to fetch data from the database
class DataExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector

    # This method is used to execute a query and return the results
    def extract_data(self, query):
        with self.database_connector.engine.connect() as connection:
            result = connection.execute(query)
            return [dict(row) for row in result]

    # New method to read a table and return it as a pandas DataFrame
    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name};"
        with self.database_connector.engine.connect() as connection:
            data_frame = pd.read_sql(query, connection)
        return data_frame

# Example usage of the classes above
creds_file = r'C:\Users\mudi\OneDrive\Documents\Dataset\Multinational Retail Data Centralisation\db_creds.yaml'  # Path to your credentials YAML file
connector = DatabaseConnector(creds_file)  # Creates a DatabaseConnector object with the credentials file
extractor = DataExtractor(connector)  # Creates a DataExtractor object with the connector

# Replace 'your_table_name' with the actual table name you want to query
table_name = "your_table_name"  # TODO: Replace with your actual table name
# Use the read_rds_table method to read the table and store it in a DataFrame
data_frame = extractor.read_rds_table(table_name)
# Print out the DataFrame to the console
print(data_frame)
