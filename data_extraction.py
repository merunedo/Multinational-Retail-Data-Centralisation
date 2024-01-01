import yaml
import pandas as pd
from sqlalchemy import create_engine, MetaData, inspect 


class DataCleaning:
    
    @staticmethod
    def clean_user_data(df):
        # Replace NULL values or fill them with default values
        # For instance, if 'age' column has NULL values, you could set them to the median age
        if 'age' in df.columns:
            df['age'] = df['age'].fillna(df['age'].median())

        # Correct errors with dates
        if 'signup_date' in df.columns:
            df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')

        # Handle incorrectly typed values
        if 'user_id' in df.columns:
            df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce', downcast='integer')

        # Remove rows filled with the wrong information
        if 'email' in df.columns:
            df = df[df['email'].str.contains('@')]

        # Remove rows where the date conversion failed
        df = df.dropna(subset=['signup_date'])

        # Remove rows where the user_id conversion failed (e.g., negative values are not valid)
        df = df[df['user_id'] > 0]

        # More cleaning rules can be added as needed based on the data issues

        return df

    
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
    
    # Add this new method to the DatabaseConnector class 
    def print_table_names(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        print(table_names)
        return table_names
    
    def upload_to_db(self, df, table_name):
        '''
        This method uploads a pandas DataFrame to the specified table in the database.

        Parameters:
        - df: pandas DataFrame to be uploaded.
        - table_name: Name of the table where the DataFrame should be uploaded.

        Returns:
        None
        '''
        # Create database engine using the init_db_engine method
        engine = self.init_db_engine()

        # Use pandas to_sql method to upload the DataFrame
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Close the connection
        engine.dispose()

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
table_name = connector.print_table_names()  # TODO: Replace with your actual table name

# Let's say you want to fetch data from the first table in the list
# Make sure there is at least one table name in the list before trying to access it
if table_name:
    table_name = table_name[0]  # Select the first table name

    # Use the read_rds_table method to read the table and store it in a DataFrame
    data_frame = extractor.read_rds_table(table_name)
    # Print out the DataFrame to the console
    print(data_frame)
else:
    print("No tables found in the database.")
