import os
import requests

def _fetch_data(**context):
    # Function to retrieve data from the CSV file
    csv_url = "https://www.data.gouv.fr/fr/datasets/r/2b1523f0-d8bf-426f-bf1f-9974c9cccb53"
    header = {'Content-Type': 'text/csv; charset=utf-8'}
    response = requests.get(csv_url, headers=header)
    response.encoding = 'utf-8'
    csv_content = response.text

    # Write the content to a file in a specific folder
    folder_path = 'data'
    file_name = "vaccination_stock_ds.csv"
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(csv_content)

    return file_path

_fetch_data()

# A adapter à la task