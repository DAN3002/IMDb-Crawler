import json
import csv
import pandas as pd
from datetime import datetime
from utils.logger import setup_logger
import os

class JsonToCsvConverter:
    def __init__(self):
        # Setup logger
        log_file = f'./logs/converter_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        self.logger = setup_logger('JsonToCsvConverter', log_file)

    def convert_json_to_csv(self, input_file, output_file):
        """Convert JSON file to CSV format"""
        try:
            self.logger.info(f"Reading JSON file: {input_file}")
            
            # Read JSON file
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not data:
                self.logger.warning("No data found in JSON file")
                return False
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add index column starting from 1
            df.insert(0, '#', range(1, len(df) + 1))
            
            # Process array columns
            for column in df.columns:
                if isinstance(df[column].iloc[0], list):
                    self.logger.info(f"Converting array column: {column}")
                    df[column] = df[column].apply(lambda x: ','.join(str(item) for item in x) if x else '')
            
            # Save to CSV
            df.to_csv(output_file, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compatibility
            
            self.logger.info(f"Successfully converted {input_file} to {output_file}")
            self.logger.info(f"Number of records processed: {len(df)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error converting file: {str(e)}")
            return False

def main():
    # Create required directories
    for directory in ['./logs', './output']:
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    converter = JsonToCsvConverter()
    
    # List of files to convert
    files_to_convert = [
        # {
        #     'input': 'output/movie_details.json',
        #     'output': 'output/movie_details.csv'
        # },
        {
            'input': 'output/filtered_movies.json',
            'output': 'output/filtered_movies.csv'
        },
        {
            'input': 'output/movie_reviews.json',
            'output': 'output/movie_reviews.csv'
        }
    ]
    
    # Convert each file
    for file_pair in files_to_convert:
        input_file = file_pair['input']
        output_file = file_pair['output']
        
        if os.path.exists(input_file):
            print(f"Converting {input_file} to {output_file}...")
            success = converter.convert_json_to_csv(input_file, output_file)
            if success:
                print(f"Successfully converted {input_file}")
            else:
                print(f"Failed to convert {input_file}")
        else:
            print(f"Input file not found: {input_file}")

if __name__ == "__main__":
    main() 