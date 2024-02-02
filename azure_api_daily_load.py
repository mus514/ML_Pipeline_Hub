#!/miniconda3/envs/ml-ai
import requests
import json
from datetime import datetime

# Replace 'YOUR_ALPHA_VANTAGE_API_KEY' with your actual API key
api_key = 'KO4Q75QIO0DULFR5'

# Specify the stock symbol and API endpoint for time series data
symbol = ['AAPL', 'MSFT', 'AMZN', 'TSLA'] 

# folder to save data
folder_path = 'Mon_disque/ML_projects/ML_Pipeline_Hub/data/'

# today date
today = datetime.now().date()

# upload daily stock file
def load_data(api_key, symbol):
    endpoint = 'https://www.alphavantage.co/query'
    function = 'TIME_SERIES_DAILY'
    datatype = 'json'

    # Build the API request URL
    url = f'{endpoint}?function={function}&symbol={symbol}&apikey={api_key}&datatype={datatype}&outputsize=full'
    # Make the API request
    
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse and filter the data for the specified date range
        data = response.json()
        return data
    
    #print(filtered_data)
    else:
        print(f"Error: {response.status_code}, {response.text}") 


# Daily process
def daily_process():
    try:
        load_files_list = []
        for stock in symbol:
            data = load_data(api_key, stock)
            file_path = f'{folder_path}{stock}.json'

            if list(data.keys())[0] != 'Information':
                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file)
                
                load_files_list.append(stock)
                print(f"JSON data saved to {file_path}")
        
        text_path = 'Mon_disque/ML_projects/ML_Pipeline_Hub/output.txt'
        with open(text_path, 'w') as file:
            file.write(f'{len(load_files_list)}' + '\n')
            # Write the intro string
            file.write(f'The loaded stock files for {today} are :' + '\n')

            # Write each stock file on a new line
            for stock_file in load_files_list:
                file.write("----- " + stock_file + '\n')
    
    except Exception as error:
        print(f'An unexpected error occurred: {error}')
   

# daily runing
daily_process()
