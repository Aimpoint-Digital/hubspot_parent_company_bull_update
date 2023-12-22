import requests
import os
import csv
import json
from datetime import datetime
import logging
from validate_csv import ValidateCSV

class HubspotAPI():
    def __init__(self, test=False, base_path="", api_key=None, input_file_path="../input_data.csv"):
        self.base_path = base_path
        self.input_data_file = input_file_path
        self.access_token = api_key
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def load_and_validate_data(self):
            with open(self.input_data_file, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                data = list(csv_reader)

            try:
                ValidateCSV(data)
            except ValueError as e:
                error_message = f"Validation error: {e}"
                logging.error(error_message)
                raise Exception(error_message)

            return data
    
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
        url = 'https://api.hubapi.com/crm-associations/v1/associations'
        payload = {
            "fromObjectId": parent_company_id,
            "toObjectId": company_id,
            "category": "HUBSPOT_DEFINED",
            "definitionId": 13
        }
        response = requests.put(url=url, headers=self.headers, json=payload)
        if response.status_code != 204:
            error_message = f"Error associating Parent company {parent_company_id} with {company_id}: {response.status_code} - {response.text}"
            logging.error(error_message)
            raise Exception(error_message)
        else:
            logging.info(f"Associated Parent company {parent_company_id} with {company_id}")
            self.update_results.append({
                "company_id": company_id,
                "parent_company_id": parent_company_id
            })

    def run_update(self, data):
        self.errors = []
        self.results = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        for company in data:
            logging.info(f"Processing company: {company}")

            # Check that companies exist and have not been processed
            company_missing = False
            if not self.check_company_exists(company["company_id"]) or not self.check_company_exists(company["parent_company_id"]):
                company_missing = True
                error_dict = {
                    "company_id": company["company_id"],
                    "parent_company_id": company["parent_company_id"],
                    "error": "Company not found"
                }
                self.errors.append(error_dict)
                break

            if company_missing:
                logging.info(f"Skipping company {company}: The company_id or parent_company_id does not exist")
                continue

            try:
                update_error = False
                updated_companies = self.update_parent_company(company['company_id'], company['parent_company_id'])
            except Exception as e:
                update_error = True
                update_error_dict = {
                        "company_id": company["company_id"],
                        "parent_company_id": company["parent_company_id"],
                        "error": f"{e}"
                    }
                self.errors.append(update_error_dict)

            if update_error:
                continue
            
            self.results.append(updated_companies)

        if self.errors:
            self.write_to_json(self.errors, f"./data/errors/missing_{timestamp}.json")
        self.write_to_json(self.results, f"./data/outputs/updated_{timestamp}.json")
        return self.results



def run_hubspot_parent_update(test=False):
    # Create directories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/outputs', exist_ok=True)
    os.makedirs('data/errors', exist_ok=True)
    
    # Logging
    logging.basicConfig(filename='./logs/update_operations.log',
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
        valid_data = hubspot_client.load_and_validate_data()

        # User confirmation
        user_input = input("The data input file has been successfully validated. Do you want to continue with the update? (y/n) ")
        if user_input.lower() != 'y':
            logging.info("Update operation aborted.")
            return
        
        # Run update
        logging.info('Update operation started')
        update_results = hubspot_client.run_update(valid_data)
        return update_results

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    # Run program
    run_hubspot_parent_update()

    # Finish
    logging.info('Update operation finished')