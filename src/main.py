import requests
import os
import csv
import json
import dotenv
from datetime import datetime
import logging
from src.validate_csv import ValidateCSV

class HubspotAPI():
    def __init__(self, base_path="", api_key=None, input_file_path="../input_data.csv"):
        self.base_path = base_path
        self.input_data_file = input_file_path
        self.url = 'https://api.hubapi.com/crm-associations/v1/associations'
        if api_key:
            self.access_token = api_key
        else:
            env_file = "env_prod.env"
            dotenv.load_dotenv(env_file)
            self.access_token = os.getenv("HUBSPOT_ACCESS_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def load_and_group_data(self):
            with open(self.input_data_file, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                data = list(csv_reader)
                for row in data:
                    row['action'] = row['action'].lower() # Lowercase the action column
            try:
                ValidateCSV(data)
            except ValueError as e:
                error_message = f"Validation error: {e}"
                logging.error(error_message)
                raise Exception(error_message)
                
            grouped_data = {}
            for row in data:
                key = row['key']
                if key not in grouped_data:
                    grouped_data[key] = []
                grouped_data[key].append(row)

            return grouped_data
    
    def write_to_json(self, data, output_path):
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2)

        
    def check_company_exists(self, company_id):
        url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200 and company_id == response.json()["id"]:
            return True
        else:
            error_message = f"Error fetching company with id {company_id}. Status code: {response.status_code}"
            logging.error(error_message)
            return False
    
    def update_parent_company(self, company_id, parent_company_id):
        payload = {
            "fromObjectId": company_id,
            "toObjectId": parent_company_id,
            "category": "HUBSPOT_DEFINED",
            "definitionId": 13
        }
        response = requests.post(url=self.url, headers=self.headers, json=payload)
        if response.status_code != 200:
            error_message = f"Error associating Parent company {parent_company_id} with {company_id}: {response.status_code} - {response.text}"
            logging.error(error_message)
            raise Exception(error_message)
        else:
            logging.info(f"Associated Parent company {parent_company_id} with {company_id}")
            self.update_results.append({
                "company_id": company_id,
                "parent_company_id": parent_company_id
            })

    def run_merge(self, grouped_data):
        self.missing = []
        self.results = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        for company in grouped_data.items():
            logging.info(f"Processing company: {company}")

            # Check that companies exist and have not been processed
            company_missing = False
            if not self.check_company_exists(company["company_id"]) or not self.check_company_exists(company["parent_company_id"]):
                company_missing = True
                missing_dict = {
                    "company_id": company["company_id"],
                    "parent_company_id": company["parent_company_id"],
                    "error": "Company not found"
                }
                self.missing.append(missing_dict)
                break

            if company_missing:
                logging.info(f"Skipping company {company}: The company_id or parent_company_id does not exist")
                continue

        updated_companies = self.update_parent_company(company['company_id'], company['parent_company_id'])

        self.results.append(updated_companies)


        if self.missing:
            self.write_to_json(self.missing, f"./data/errors/missing_{timestamp}.json")
        self.write_to_json(self.results, f"./data/outputs/merged_{timestamp}.json")
        return self.results



def run_hubspot_merge(test=False):
    # Create directories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/outputs', exist_ok=True)
    os.makedirs('data/errors', exist_ok=True)
    
    # Logging
    logging.basicConfig(filename='./logs/merge_operations.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    
    # Create and configure a StreamHandler for console output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Set the logging level for console
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_formatter)

    # Add the console handler to the root logger
    logging.getLogger().addHandler(console_handler)
    
    try:
        api_key = None
        if not test:
            # API key provision
            api_key = input("Please provide your Hubspot API key and press Enter: ")
            if not api_key:
                logging.info("Update operation aborted.")
                return
        
        # Start client, load & validate data
        hubspot_client = HubspotAPI(test=test, api_key=api_key, input_file_path="input_data.csv")
        grouped_data = hubspot_client.load_and_group_data()

        # User confirmation
        user_input = input("The data input file has been successfully validated. Do you want to continue with the update? (y/n) ")
        if user_input.lower() != 'y':
            logging.info("Update operation aborted.")
            return
        
        # Run merge
        logging.info('Update operation started')
        update_results = hubspot_client.run_merge(grouped_data)
        return update_results

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    # Run program
    run_hubspot_merge()

    # Finish
    logging.info('Merge operation finished')