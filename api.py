import requests

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