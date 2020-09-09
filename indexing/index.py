#usr/bin/python -tt
import os, os.path
import sys
import sqlite3
from sqlite3 import Error
import whoosh
import math
import pickle
from whoosh.index import create_in, open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
from whoosh.filedb.filestore import FileStorage

def create_connection(db_file):
	# type: (str) -> sqlite.connect
	try:
		connection = sqlite3.connect(db_file)
		return connection
	except Error as e:
		print(e)
	return None

def select_all_rows(conn):
	# type: (sqlite.connect) -> [[ sqlite schema ]]
	#
	# The scheme consists of 10 strings per row. More info can be found
	# in the search() function or .db file
	l = []
	cursor = conn.cursor()
	cursor.execute('SELECT * FROM cities')
	rows = cursor.fetchall()

	for row in rows:
		l.append(row)
	return l

def create_coordinate_dictionary(lst):
	# type: ([city_schema]) -> {}
	#
	# Returns a dictionary with keys for each location in the database.
	# Values are appended to a key when the distance between the key and
	# value are 200 miles away or less.

	dist = dict()
	for i in range(len(lst)-1):
		print('-----'+lst[i][0]+'-----')
		for j in range(len(lst)-1):
			d = get_distance(lst[i], lst[j])
			if 0 < d < 200:
				city_key = lst[i][0]
				neighboring_city = lst[j]
				if city_key in dist:
					dist[city_key].append(list(neighboring_city))
				else:
					dist[city_key] = list(neighboring_city)
				print(d)
	with open('coordinate_dict.pickle', 'wb') as handle:
		pickle.dump(dist, handle, protocol=pickle.HIGHEST_PROTOCOL)

def get_distance(l1, l2):
	# type: ([city_schema], [city_schema]) -> float
	#
	# Returns a distance in miles between two locations.
	# Distances are calculated using the haversine Formula.
	# More info can be found here: https://www.movable-type.co.uk/scripts/latlong.html

	radius = 6371000

	if type(l1) == list:
		lat1 = l1[2]
		long1 = l1[3]
	else:
		lat1 = float(l1['latitude'])
		print(lat1)
		long1 = float(l1['longitude'])
		print(long1)

	print(type(lat1))
	lat2 = l2[2]
	long2 = l2[3]

	s1 = math.radians(lat1)
	s2 = math.radians(lat2)
	r1 = math.radians((lat2 - lat1))
	r2 = math.radians((long2 - long1))
	a = math.sin(r1/2) * math.sin(r1/2) + math.cos(s1) * math.cos(s2) * \
	 	math.sin(r2/2) * math.sin(r2/2)
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
	d = radius * c
	return d/1609.344

def filter_repeats(l):
	# type: ([city_schema]) -> [city_schema]
	#
	# Filters out cities in the scehma that are filter_repeats

	new_list_cities = []
	new_list = []

	for i in l:
		if i[0] not in new_list_cities:
			new_list_cities.append(i[0])
			new_list.append(i)
	return new_list

def filter_location_by_distance(queried_loc, list_locations, distance):
	# type: ([whoosh.searcher.Hit], [city_schema]) -> [city_schema]
	#
	# Return a filtered list of cities within a given distance of the
	# queried city.

	new_list = []

	for loc in list_locations:
		if get_distance(queried_loc, loc) < distance:
			new_list.append(loc)
	return new_list

def get_nearby_cities(results, distance):
	# type: ([city_schema], float) -> [[city_schema]]

	with open("coordinate_dict.pickle", 'rb') as handle:
		try:
			b = pickle.load(handle)
			b = b[results[0]['cities']]

			# Filters out non utf-8 city titles.
			#
			# Note: Find a way to display non utf-8 titles on the webpage.
			nearby_cities = list(filter(lambda a: type(a) == list, b))
			nearby_cities = filter_repeats(nearby_cities)
			nearby_cities = filter_location_by_distance(results[0], nearby_cities, distance)
			return nearby_cities
		except KeyError:
			return None

def search(indexer, searchTerm):
	# type: (Whoosh.indexer, str) -> [Whoosh.Searcher]
	#
	# Returns a list of schemas (sqliteDB rows) specfied in index().

	searcher = indexer.searcher()
	query = MultifieldParser(["cities", "summaries",
		 "latitude", "longitude",\
		"image1", "image2", "image3", "image4", "image5", "countries"], \
				schema=indexer.schema).parse(searchTerm)
	results = searcher.search(query)
	return results

def index():
	# type: () -> Whoosh.indexer
	#
	# Constructs an indexer using the specified schema.

	database = '/Users/sonny/Desktop/citydb-master 2/indexing/cities2.db'
	connection = create_connection(database)
	with connection:
			cities = select_all_rows(connection)

	schema = Schema(cities=TEXT(stored=True), summaries=TEXT(stored=True), latitude=TEXT(stored=True),\
					longitude=TEXT(stored=True), image1=TEXT(stored=True), image2=TEXT(stored=True),\
					image3=TEXT(stored=True), image4=TEXT(stored=True), image5=TEXT(stored=True),\
					countries=TEXT(stored=True))

	if not os.path.exists('indexDir'):
		os.mkdir('indexDir')
	indexer = index.create_in('indexDir', schema)

	ix = open_dir('indexDir')
	if ix != None:
		return ix



	writer = indexer.writer()

	for i in cities:
		writer.add_document(cities=i[0], summaries=i[1], latitude=str(i[2]),\
						longitude=str(i[3]), image1=i[4], image2=i[5], image3=i[6], \
						image4=i[7], image5=i[8], countries=i[9])
	writer.commit()
	return indexer

def suggest_query(indexer, query):
	# type: (Whoosh.Indexer, str) -> [Whoosh.Searcher]

	with indexer.searcher() as s:
		corrector = s.corrector("cities")
		print(corrector.suggest(query, limit=10, maxdist=2))
		return corrector.suggest(query, limit=10, maxdist=2)

def user_input():
	x = input("\n\nEnter a search term:")
	return x

def main():
	indexer = index()

	while True:
		term = user_input()
		results = search(indexer, term)
		if len(results) == 0:
			suggest_query(indexer,term)
		print(results)

if __name__ == '__main__':
	main()
