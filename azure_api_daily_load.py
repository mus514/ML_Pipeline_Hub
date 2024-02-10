#!/miniconda3/envs/ml-ai
import requests
import json
from datetime import datetime
import yfinance as yf

# Replace 'YOUR_ALPHA_VANTAGE_API_KEY' with your actual API key
api_key = 'KO4Q75QIO0DULFR5'

# Specify the stock symbol and API endpoint for time series data
symbol = ['AAPL', 'MSFT', 'AMZN', 'GOOGL'] 

# folder to save data
folder_path = 'Mon_disque/ML_projects/ML_Pipeline_Hub/data/'

# today date
today = datetime.now().date()

# upload daily stock file
def load_data(stock_symbol):
    # Start date
    start_date = '2004-01-01'
    
    # Use yfinance to download stock data
    stock_data = yf.download(stock_symbol, start=start_date, end=today)

    return stock_data

# Daily process
def daily_process():
    try:
        load_files_list = []
        for stock in symbol:
            df = load_data(stock)
            df = df.reset_index()
            file_path = f'{folder_path}{stock}.csv'
            df.to_csv(file_path, index=False)               
            load_files_list.append(stock)
            print(f"csv file saved to {file_path}")
        
        text_path = 'Mon_disque/ML_projects/ML_Pipeline_Hub/logs/output.txt'
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
