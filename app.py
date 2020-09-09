#usr/bin/python -tt
import os
import sys
import pickle
from flask import Flask, request, redirect, render_template, url_for
from indexing.index import index, search, get_nearby_cities, suggest_query

# API key for google maps
# AIzaSyCSeEZo1sEFs5Hnsxy3GNQrtBaxU2no7_A

app = Flask(__name__)
app.config['DEBUG'] = True

prev_city_loc = None

@app.route('/')
def home_page():
    return render_template('home.html')

@app.route('/results', methods=['GET', 'POST'])
def results():
    indexer = index()
    location = request.form['query']
    results = search(indexer, location)
    global prev_results
    prev_results = results

    distance = float(request.form['distance'])

    # Search query was not in the whoosh index
    if len(results) == 0:
        try:
            suggest_term = suggest_query(indexer,location)
            results = search(indexer,suggest_term[0])
            prev_results = results

            nearby_cities = get_nearby_cities(results, distance)

            global prev_city_loc
            prev_city_loc = nearby_cities
            return render_template('suggest.html',suggested=suggest_term)
        except IndexError:
            return render_template('suggest.html', suggested=None)

    nearby_cities = get_nearby_cities(results, distance)
    prev_city_loc = nearby_cities
    return render_template('search_results.html', search_query=results[0]['cities'],\
                            first_result=results[0], loc_cities=nearby_cities)

@app.route('/<location>')
def location(location):
    indexer = index()
    global prev_results
    global prev_city_loc
    results = search(indexer, location)
    return render_template('search_results.html', search_query=results[0]['cities'],\
                            first_result=results[0], loc_cities=prev_city_loc)

if __name__ == '__main__':
    app.run()
