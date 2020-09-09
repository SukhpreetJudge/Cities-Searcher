import wikipedia
import random
import sqlite3
import whoosh
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser

connection = sqlite3.connect('cities2.db')
cursor = connection.cursor()

def get_countries():
    # type: () -> List[str]

    file_object = open('../indexing/list_of_countries.txt', 'r')
    l = file_object.readlines()
    return list(map(lambda s: s.strip(), l))

list_of_countries = get_countries()

def filter_non_cities(l):
    # type: (List[str]) -> bool
    #
    # Helper function to filter out links that are not cities.
    # Note: This does filter out every non-city when the dataset is large.

    if "List" in l or " " in l or "." in l:
        return False

def create_table():
    cursor.execute('CREATE TABLE IF NOT EXISTS cities(cities TEXT, summaries TEXT, latitude REAL,\
                    longitude REAL, image1 TEXT, image2 Text, image3 TEXT, image4 TEXT,\
                    image5 TEXT, countries TEXT)')

def create_database():
    # Creates a sqlite database.
    # Each row represents a city.
    # [city_name, summary, latitude, longitude, image, image, image, image, image, country]

    create_table()
    count = 0
    for country in list_of_countries:
        list_page = wikipedia.page('List of cities in '+country)
        links = list_page.links
        filtered_links = list(filter(lambda x: filter_non_cities(x) != False, links))
        print('-------------------------')
        print(country)
        count += len(filtered_links)
        print(count)
        for city in filtered_links:
            try:
                page = wikipedia.page(city)
                coordinates = page.coordinates
                if coordinates != None:
                    summary = page.summary
                    print('Summary for '+city)
                    print(summary)
                    print("Coordinates for "+city)
                    print(coordinates)
                    images = page.images[0:5]
                    print(images)
                    cursor.execute('INSERT INTO cities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',\
                                    (city, summary, float(coordinates[0]), float(coordinates[1]), images[0],\
                                    images[1], images[2], images[3], images[4], country))
                    connection.commit()
            except (wikipedia.exceptions.DisambiguationError, wikipedia.exceptions.PageError, IndexError, KeyError):
                print("Could not get wikipedia page for "+city)
    cursor.close()
    connection.close()

def main():
    create_database()

if __name__ == '__main__':
    main()
