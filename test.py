import os
import requests
import pandas as pd
import chardet
import csv

def get_separator(filename):
    # Read the first few lines of the file
    print('OK')
    with open(filename, 'r') as file:
        lines = [file.readline() for line in range(5)]

    # Check for common separators
    for separator in [',', ';']:
        if all(separator in line for line in lines):
            return separator
    # If no common separator is found, return None
    return None

 


def get_encoding(filename):
    # Read the first few lines of the file
    with open(filename, 'rb') as file:
        rawdata = file.read()

    # Detect the encoding of the raw data
    result = chardet.detect(rawdata)

    # Return the detected encoding
    return result['encoding']
# Get the separator of the CSV file
filename = './data/staging/vaccination_vs_appointment_ds.csv'
separator = get_separator(filename)
 

# pd.read_csv('./data/staging/vaccination_centers_ds.csv', sep=';')

def process_all_csv():
    staging_folder_path = 'data/staging'
    process_folder_path ='data/processed'

# Get the list of CSV files in the folder
    csv_files = [file for file in os.listdir(staging_folder_path) if file.endswith('.csv')]
 
    # Print the content of each CSV file
    for file in csv_files:
        staging_file_path = os.path.join(staging_folder_path, file)
        process_file_path = os.path.join(process_folder_path,file)
   
        df = pd.read_csv(staging_file_path,sep=get_separator(staging_file_path),on_bad_lines='skip',low_memory=False)
        df.to_csv(process_file_path,sep=',',encoding='utf-8',quoting=csv.QUOTE_MINIMAL,index=True )

def get_csv_filename(folder_path):
    csv_filenames = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            csv_filenames.append(filename.split('_ds.csv')[0])
    print(csv_filenames)


get_csv_filename('data/processed/')