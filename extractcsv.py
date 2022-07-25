
import csv
import pandas as pd

COLUMNS = [
    'timestamp','store','customer_name','basket_items','total_price','cash_or_card','card_number'
]

def read_csvfile_into_list(file_name: str):
    try:
        list_dicts = []
        with open(file_name, "r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                list_dicts.append(row)
    except FileNotFoundError:
        list_dicts = []
    return list_dicts

# print(read_csvfile_into_list("test.csv"))

def read_csvfile_into_dataframe(file_name: str):
    try:
        df = pd.read_csv(file_name, names=COLUMNS)
        def reformat_timestamp(old_timestamp):
            [date, time] = old_timestamp.split(" ")
            [day, month, year] = date.split("/")
            return f"{year}-{month}-{day} {time}"
        df["timestamp"] = df["timestamp"].apply(reformat_timestamp)
        # df['timestamp'] = pd.to_datetime(df['timestamp'], format = '%Y/%m/%d %H:%M')  #TODO     
        return df
    except FileNotFoundError:
        
        return None

# print(read_csvfile_into_dataframe("test.csv"))
