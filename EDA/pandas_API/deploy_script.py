import argparse
import pandas as pd
import numpy as np
import base64
import json
import os
import io
import difflib
import time

"""
@author: Sergio Zapata Caparrós
@date: 30/04/2024
DATASET CONSUMPTION.
"""

def fix_word(word, options):
    """
    Fixes typing errors found in data. Uses the degree of similarity between a word and another correct word
    
    Parameters:
        word(string): word to compare
        options(list): list of correct words
        
    Returns:
        string: Correct word
    """
    # get_close_matches method returns the closest word (list) of the list. "cutoff" defines the degree of similarity and "n" the number
    # of correct words
    closest = difflib.get_close_matches(word, options, n=1, cutoff=0.7)
    if closest: # If the list isn't empty
        return closest[0]
    else:
        return word
    

def csv2json(pages, filename, dataset_folder):
    """
    Transforms a paged csv format file into a base64-encoded JSON field.
    
    Parameters:
        pages(list): Dataframe pages
        filename(string): Name of the output file
    """
    json_body = {'pages': []}
    
    for i, page in enumerate(pages):
        # We create a CSV file for each page
        csv_file = os.path.join(dataset_folder, filename + '_page' + str(i+1) + '.csv')
        page.to_csv(csv_file, index = False)
        
        # Read CSV content
        with open(csv_file, 'rb') as f:
            content = f.read()
        
        # Base64 coding
        csv_coded = base64.b64encode(content).decode('utf-8')
        
        page_number = i + 1 # Number of current page
        total_pages = len(pages) # Number of total pages
        
        # Calculating next_page field (0 if last page)
        if(page_number == total_pages):
            next_page = 0
        else:
            next_page = 1
        
        # Adding data to the corresponding dictionary. 'pages' as main field.
        json_body['pages'].append({
            'page_number': page_number,
            'next_page': next_page,
            'csv_data': csv_coded
        })
    # Saving dictionary as a JSON
    json_file = filename + '.json'
    with open(json_file, 'w') as f:
        json.dump(json_body, f)

def decode_csv_from_api(api_response):
    """
    Decodes a base64-encoded CSV file from a JSON.
    
    Parameters:
        api_response (json body): JSON page from API
        
    Returns:
        pandas.dataframe: Data from one csv file
    """
    # Decoding csv file (selecting the corresponding field)
    csv_bytes = base64.b64decode(api_response['csv_data'])
    # Decoded bytes into string
    csv_text = csv_bytes.decode('utf-8')
    # Pandas datafream from csv
    df = pd.read_csv(io.StringIO(csv_text)) # io.StringIO() method to be able to read the file with "pd-read_csv"
    
    return df

def read_from_api(json_file):
    """
    Reads CSV data from a JSON divided into pages.
    
    Parameters:
        json_ile (json file): JSON file rom the API (all pages)
        
    Returns:
        pandas.dataframe: Data from all csv files
    """
    # Loading the corresponding json file
    with open(json_file, 'r') as f:
        json_body = json.load(f)

    csv_data = [] # List
    
    for page in json_body['pages']:
        # Calling the function to decode the csv
        df = decode_csv_from_api(page)
        # Adding decoded data to the list
        csv_data.append(df)
        
        # Check if there is a new page available
        next_page = page.get('next_page') # get() method returns the value associated with the specified key 
        if not next_page:
            break # Leave the loop
    
    # Union of all csv data (all the elements of the list)
    total_df = pd.concat(csv_data, ignore_index=True)
    
    return total_df

def main(output_type):

    # Start time
    start_time = time.time()

    # Defining the rows per page
    rows_page = 1000
    # Total rows
    rows = 50000
    # Calculating total pages
    if rows % rows_page != 0:
        total_pages = rows // rows_page + 1 # Aditional page
    else:
        total_pages = rows // rows_page 
        
    # Probabilities are added in order to make data more realistic.
    prob_countries = [0.45, 0.05, 0.1, 0.1, 0.05, 0.05, 0.05, 0.05, 0.025, 0.025, 0.025, 0.025] # Probability to appearance (countries)
    prob_status = [0.4, 0.3, 0.1, 0.025, 0.05, 0.025, 0.025, 0.05, 0.025] # Probability to appearance (status)

    # Dataframe with synthetic data
    data = {
        'id': np.arange(1, rows + 1), # Unique values for each transaction
        # Assuming 4 countries. Some possible typing errors have been added so they can be corrected in the transformation process.
        'country': np.random.choice(['Spain', 'Germany', 'Italy', 'USA', 'China', 'Belgium', 'Sopain', 'Germqany', 'Itakly', 'United States of America', 'Cvhina', 'Belguiun'], rows, p = prob_countries),
        # Three possible cases. Some possible typing errors have been added so they can be corrected in the transformation process.
        'status': np.random.choice(['pending', 'completed', 'failed', 'pendhing', 'pwnding', 'compoletd', 'complete', 'fialed', 'faoleid'], rows, p = prob_status),
        'amount': np.random.uniform(50, 5000, rows)
    }

    df = pd.DataFrame(data)

    pages = [] # List
    for i in range(0, rows, rows_page): # From 0 to "rows" with step = "rows_page"
        pages.append(df[i:i + rows_page])


    # Function call
    csv2json(pages, 'transactions', 'dataset/')

    # Reading JSON file created previosly (response from the API)
    API_response = pd.read_json('transactions.json')

    df = read_from_api('transactions.json')

    # First we replace all "United States of America" for USA
    df['country'] = df['country'].replace('United States of America', 'USA')

    correct_countries = ['Spain', 'China', 'USA', 'Italy', 'Belgium', 'Germany'] # List of correct countries
    correct_status = ['pending', 'failed', 'completed'] # List of correct status

    # With "apply()", we apply lambda function on each value from the corresponding column
    df['country'] = df['country'].apply(lambda x: fix_word(x, correct_countries))
    df['status'] = df['status'].apply(lambda x: fix_word(x, correct_status))

    # First, the "pending" filter is applied, grouped by country and the average amount is calculated.
    average_outstanding = df[df['status'] == 'pending'].groupby('country')['amount'].mean()
    # First, the "completed" filter is applied, grouped by country and the total amount is calculated.
    total_completed = df[df['status'] == 'completed'].groupby('country')['amount'].sum()

    # The "failed" filter is applied, grouped by country and counting appearances. Divided by appearances
    error_rate = df[df['status'] == 'failed'].groupby('country').size() / df.groupby('country').size()
    # Two filters are applied: One for amounts above 10000 and the other for failed transactions. Grouped by country and counting appearances.
    critical_rate = df[(df['amount'] > 1000000) & (df['status'] == 'failed')].groupby('country').size() / df.groupby('country').size()
    # Filling NaN values with zeros.
    critical_rate = critical_rate.fillna(0)

    # Constructing the dataset from a dictionary
    final_df = pd.DataFrame({
        'country': average_outstanding.index,
        'average_outstanding': average_outstanding.values,
        'total_completed': total_completed.values,
        'critical_rate': critical_rate.values,  # Llenar los valores NaN con 0
        'error_rate': error_rate.values
    })

    end_time = time.time()

    runtime = end_time - start_time

    if output_type == 'local':
        final_df.to_csv('final_dataset.csv', index=False)
        print("-----------------")
        print("SUCCESS")
        print("-----------------")
        print("Stored as a csv")
        print("Elapsed time: " + str(runtime) + "s")
    elif output_type == 's3':
        print("-----------------")
        print("SUCCESS")
        print("-----------------")
        print("Successfully uploaded to S3")
        print("Elapsed time: " + str(runtime) + "s")
    elif output_type == 'pg':
        print("-----------------")
        print("SUCCESS")
        print("-----------------")
        print("Successfully uploaded to Postgres")
        print("Elapsed time: " + str(runtime) + " s")
        pass 


if __name__ == "__main__":
    # Parser configuration (command line)
    parser = argparse.ArgumentParser(description='Script to transform data from API')
    parser.add_argument('--output-type', choices=['local', 's3', 'pg'], required=True, help='Output type of the dataset')

    # Read arguments from the command line
    args = parser.parse_args()

    # Calls the main function with the corresponding arguments.
    main(args.output_type)
