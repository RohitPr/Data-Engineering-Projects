from bs4 import BeautifulSoup
import requests


def movie_extract():
    base_url = "https://boxofficecollection.in/"
    tail_url = "-box-office-collection-day-wise"

    movie_details = {"Luka Chuppi": "2019-03-01", "Badla": "2019-03-08",
                     "Kesari": "2019-03-21", "Gully Boy": "2019-02-14", "Total Dhamaal": "2019-02-22"}

    for key in movie_details:
        movie_name = key.replace(" ", "-")
        response = requests.get(base_url+movie_name+tail_url)
        details_page = response.text
        soup = BeautifulSoup(details_page, "html.parser")
        data = soup.find("table", {"class": "tablepress"})
        print(data)


movie_extract()
