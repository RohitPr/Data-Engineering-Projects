from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import engine


def movie_extract():
    base_url = "https://boxofficecollection.in/"
    tail_url = "-box-office-collection-day-wise"

    movie_details = {"Luka Chuppi": "2019-03-01", "Badla": "2019-03-08",
                     "Kesari": "2019-03-21", "Gully Boy": "2019-02-14", "Total Dhamaal": "2019-02-22"}

    movie_name = []
    day_from_release = []
    box_office_collection = []
    date_data = []

    for key in movie_details:
        movie_input = key.replace(" ", "-")
        start_date = datetime.strptime(movie_details[key], "%Y-%m-%d").date()

        response = requests.get(base_url+movie_input+tail_url)
        details_page = response.text
        soup = BeautifulSoup(details_page, "html.parser")
        data = soup.find("tbody", {"class": "row-hover"}).getText()

        raw_data = data.split("Cr")
        raw_data.pop()

        for row in raw_data:
            split_data = row.split("₹")
            day_from_release.append(split_data[0])
            box_office_collection.append(
                int(float(split_data[1])*10000000))
            movie_name.append(key)
            date_value = int(split_data[0].replace(
                "Day ", "").split("-")[0].strip())
            date_data.append(
                (start_date + timedelta(days=date_value)).strftime("%Y-%m-%d"))


def 
