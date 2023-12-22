import logging

class ValidateCSV():
    def __init__(self, input_data):
        self.input_data = input_data
        self.validate_csv()

    def validate_csv(self):
        self.validate_csv__no_duplicate_records(self.input_data)
        logging.info('Input data has been validated')

    def validate_csv__no_duplicate_records(self, data):
        seen = set()
        for record in data:
            record_tuple = tuple(record.items())
            if record_tuple in seen:
                error_message = f"Duplicate record found: {record}"
                logging.error(error_message) 
                raise ValueError(error_message)
            seen.add(record_tuple)