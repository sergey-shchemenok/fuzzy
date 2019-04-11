# -*- coding: utf-8 -*-
from flask import Flask,jsonify,request
import mysql.connector

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

application = app = Flask(__name__)

def implement_fuzzy_search(fuzzy_query, relevance_treshold, fuzzy_limit, row_limit):
    cnx = mysql.connector.connect(user='root', password='Keplerf!',
                                  host='35.238.188.215',
                                  database='forecasts')
    cursor = cnx.cursor()
    
    # get unique names
    get_object_names_query = ("SELECT DISTINCT Object FROM Actuals")
    cursor.execute(get_object_names_query)
    object_names = cursor.fetchall()
    
    # preprocessing data to make a correct array
    for i in range(len(object_names)):
        object_names[i] = object_names[i][0].strip()    
    
    # make fuzzy search
    fs_result = process.extract(fuzzy_query, object_names, scorer=fuzz.ratio, limit=fuzzy_limit)
    
    # leave relevant data
    fsr_result = []
    for i in range(len(fs_result)):
        if fs_result[i][1] >= relevance_treshold:
            fsr_result.append(fs_result[i][0])
    
    if len(fsr_result) == 0:
        return "no results"
    
    # form query
    row_list = ""
    for i in range(len(fsr_result)):
        row_list += "'" + fsr_result[i] + "', "
    row_list = row_list[0:-2]
    get_fsr_query = ("SELECT * FROM All_Forecasts WHERE Forecast_Object IN (" + row_list +") LIMIT " + str(row_limit))
    
    cursor.execute(get_fsr_query)
    data = cursor.fetchall()
    
    cursor.close()
    cnx.close()
    
    return data
     
@app.route('/')
def home():
    return 'flask online'
    
@app.route('/fuzzysearch', methods=['POST'])
def fuzzysearch():
    req_data = request.get_json()
   
    fuzzy_query = req_data['fuzzy_query'] 
    relevance_treshold = req_data['relevance_treshold']
    fuzzy_limit = req_data['fuzzy_limit']
    row_limit = req_data['row_limit']
    
    res_data = implement_fuzzy_search(fuzzy_query, relevance_treshold, fuzzy_limit, row_limit)
    
    return jsonify(res_data)


app.run()
