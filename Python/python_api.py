# Homework API project - Thai Baht Exchange Rates
# Created by Natchapol Laowiwatkasem - 2022-08-13
# https://colab.research.google.com/drive/189LXrcH1XP7YpFCEL1s_M1PQ68NHt_BS?usp=sharing
# API from https://github.com/fawazahmed0/currency-api#readme

import requests
import pandas as pd
from datetime import date
from tabulate import tabulate

## 1. Get currency pair from user
# list of currency pairs
input_list = []
# get the number of currency pairs
number_of_input = int(input("Please enter the number of currency pairs: "))
# get the name of the currency pair
for i in range(0, number_of_input):
    input_list.append(input("Please enter the name of the currency pair: ").lower())

## 2. Get values of currency pairs through API
# list of currency pairs values
values = []
# get values of each currency through API
for i in input_list:
    url = "https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies/thb/" + str(i) + ".json"
    response = requests.get(url)
    values.append(response.json()[i])

## 3. Create dataframe for report
df = pd.DataFrame({"currencies": input_list,
            "values": values})
# shift index to start at 1
df.index = df.index + 1

## 4. Print output
# get date of today
today = date.today()
# print output table
headers = ["currencies", "values"]
print(today.strftime("%d %B %Y"), "- 1 Thai Baht equals")
print(tabulate(df, headers, tablefmt="pretty"))
