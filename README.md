# Merge Hubspot Companies

## Description
This application allows you to update the Parent Company of a batch of Hubspot Companies.

## Pre-requirements
Python needs to be installed on the machine that is running this application. You can download Python at https://www.python.org/downloads/release/python-3121/

**IMPORTANT**: Make sure you check the checkbox that says "Add Python 3.# to PATH" on the first window of the installer.

After installing Python, open the Windows Command Prompt, navigate to the application folder (e.g. `cd path/to/folder/`) and run `pip install -r src/requirements.txt`

## Preparing the data input file
To use this application, you need to provide a data input CSV file. This file should be called `input_data.csv`, and should be stored in the root of the application folder. The file should contain two columns:
- `company_id`: The Hubspot identifier of the company
- `parent_company_id`: The identifier of the desired parent company

An example `input_data.csv` is included in the application folder, and looks like:
```
company_id,parent_company_id
9480469647,949996907
9882846998,949996907
9364338255,949996907
```

## Data input file validation
There is a check at the beginning of the application to ensure that the CSV is valid. These checks include:
- Checks for duplicate records accross the file

If these validation checks are breached, you will be notified. The CSV file will need to pass validation before the application will run.

## Running the application
Double click the `run.bat` file to start the application. You will be prompted for your Hubspot API key. The data input file will then be validated, and you will be asked if you wish to continue with the merge. Enter `y` and then press Enter to continue.