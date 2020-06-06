import requests
import os.path, time
import sys
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

def csv_exists():
  return os.path.isfile(csv_file_name)

def last_modified_date():
  return datetime.strptime(time.ctime(os.path.getmtime(csv_file_name)), "%a %b %d %H:%M:%S %Y").date()

def fetch_exchange_rate(from_date, to_date, currency):
  URL = f'https://api.exchangeratesapi.io/history?start_at={from_date}&end_at={to_date}&base={currency}'
  response_object = requests.get(URL)
  data = response_object.json()
  check_data_presense(data)
  return data

def check_data_presense(data):
  if bool(data["rates"]) == False:
    raise Exception("Data is empty")

def enhance_json_to_dataframe(data, currency):
  data = pd.DataFrame([(k,k1,v1) for k,v in data["rates"].items() for k1, v1 in v.items()], columns = ['Date', 'To Currency', 'Rate'])
  data = data[data["To Currency"].isin(currencies)]
  data['From Currency'] = currency
  data = data[["Date", "From Currency", "To Currency", "Rate"]]
  return data

def formated_date(date):
  return date.strftime("%Y-%m-%d")

def a_year_before(date):
  return date - relativedelta(years=1)

def a_day_after(date):
  return date + timedelta(days=1)


'''This function is written to hit the limit of historic data available in the website
   currently it fetches yearly data and appends to csv before fetching again
   Todo: make this configurable so user can input how much data to hold in memory before writing to the file
'''
def scrape_historic_data_batch_wise(a_year_ago_from_current_date, current_date):
  for currency in currencies:
    flag = True
    while flag:
      try:
        data = fetch_historic_data(formated_date(a_year_ago_from_current_date), formated_date(current_date), currency)
        header = False if csv_exists() else True
        store_historic_date(data, header)
      except:
        flag = False
      current_date = a_year_before(current_date)
      a_year_ago_from_current_date = a_year_before(a_year_ago_from_current_date)

def string_to_date(date):
  return datetime.strptime(date, "%Y-%m-%d").date()

def fetch_historic_data(from_date, to_date, currency):
  data = fetch_exchange_rate(from_date, to_date, currency)
  data = enhance_json_to_dataframe(data, currency)
  return data

def store_historic_date(data, header):
  mode = 'w' if header else 'a'
  data.to_csv(csv_file_name, mode = mode, header = header, index = False)

currencies = ["USD", "INR", "USD", "GBP", "USD", "EUR"]
current_date = date.today()
a_year_ago_from_current_date = current_date - relativedelta(years=1)
csv_file_name = "historic_conversions.csv"

if __name__ == "__main__":
  if csv_exists():
    if last_modified_date() != current_date:
      last_modified = formated_date(a_day_after(last_modified_date()))
      for currency in currencies:
        data = fetch_historic_data(last_modified, formated_date(current_date), currency)
        store_historic_date(data, False)
  else:
    scrape_historic_data_batch_wise(a_year_ago_from_current_date, current_date)
