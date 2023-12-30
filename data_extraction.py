# Import the required modules
# requests for making HTTP requests to APIs
# csv for handling CSV file operations

import requests
import csv

# Define a class named DataExtractor
class DataExtractor:

    # This is a special method called a constructor, which initializes the class
    def __init__(self):
        # You can initialize variables here if needed
        pass

    # Method to extract data from a CSV file
    def extract_from_csv(self, file_path):
        # Create an empty list to store the data
        data = []
        # Open the CSV file
        with open(file_path, mode='r', encoding='utf-8') as file:
            # Use the csv.DictReader to read the CSV into a dictionary format
            csv_reader = csv.DictReader(file)
            # Loop through each row in the CSV
            for row in csv_reader:
                # Add each row to the data list
                data.append(row)
        # Return the list of data
        return data

    # Method to extract data from an API
    def extract_from_api(self, api_endpoint):
        # Make a GET request to the API endpoint
        response = requests.get(api_endpoint)
        # Convert the response to JSON format
        data = response.json()
        # Return the data
        return data

# Example usage of the class:
# Create an instance of the DataExtractor class
extractor = DataExtractor()
# Call the method to extract data from a CSV file
csv_data = extractor.extract_from_csv('path/to/your/file.csv')
# Call the method to extract data from an API
api_data = extractor.extract_from_api('https://api.yourservice.com/data')

# Print the data to see if it's working
print(csv_data)
print(api_data)
