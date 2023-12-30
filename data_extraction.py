import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, creds_file):
        self.db_creds = self.read_db_creds(creds_file)
        self.engine = self.init_db_engine()

    @staticmethod
    def read_db_creds(creds_file):
        with open(creds_file, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def init_db_engine(self):
        user = self.db_creds['RDS_USER']
        password = self.db_creds['RDS_PASSWORD']
        host = self.db_creds['RDS_HOST']
        port = self.db_creds['RDS_PORT']
        db = self.db_creds['RDS_DATABASE']
        # For PostgreSQL, the connection string is formatted as follows:
        connection_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
        engine = create_engine(connection_string)
        return engine

# Example usage
creds_file = 'db_creds.yaml'  # Path to your credentials YAML file
connector = DatabaseConnector(creds_file)

# Now you can use connector.engine to interact with your database using SQLAlchemy
