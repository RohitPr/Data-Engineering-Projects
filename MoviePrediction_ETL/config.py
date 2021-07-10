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

    # Requesting Box Office page with the requested movie names

    for key in movie_details:
        movie_input = key.replace(" ", "-")
        start_date = datetime.strptime(movie_details[key], "%Y-%m-%d").date()

        response = requests.get(base_url+movie_input+tail_url)
        details_page = response.text
        soup = BeautifulSoup(details_page, "html.parser")
        data = soup.find("tbody", {"class": "row-hover"}).getText()

    # Splitting the Data to collect the Day and Collection for the specific Date

        raw_data = data.split("Cr")
        raw_data.pop()

    # Appending the extracted data to different list objects

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

    # Creating a Pandas DF using the Lists created above

    movie_dict = {"Movie_Name": movie_name,
                  "Days_from_Release": day_from_release,
                  "Date": date_data,
                  "Box_Office_Collection": box_office_collection
                  }

    movie_df = pd.DataFrame(movie_dict, columns=[
                            "Movie_Name", "Days_from_Release", "Date", "Box_Office_Collection"])

    # Creating SQL Connect object to connect and execute the Query

    connection = sqlite3.connect('MovieData.db')
    cursor = connection.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS MovieData(
            Movie_Name VARCHAR(200),
            Days_from_Release INT,
            Date DATE,
            Box_Office_Collection INT,
        )
    """

    cursor.execute(sql_query)
    print("SQL DB Created")

    # Saving the Data to our SQL Database

    try:
        movie_df.to_sql("MovieData", index=False)
    except:
        print("Data already exists in the Database")

    connection.close()
    print("Connection Closed")
