#!/usr/bin/env python

import csv, json, sys
import csv
import re

import requests

import xml.etree.ElementTree as ET

def Read_file(URL1,label):    
    #URL1 = "https://df-dev.bk.rw/interview01/transactions"
    try:
        response = requests.get(URL1)
        with open(label, 'wb') as file:
            #writtting data to the file on the disk
            file.write(response.content)
    except :
        print ('The server couldn\'t fulfill the request.')

def load_json(josn_file):
    Read_file("https://df-dev.bk.rw/interview01/transactions","transactions.json")
    #reteiving data from the json file on the disk   
    with open(josn_file, 'r') as f:
        my_data = json.load(f)
    return my_data

def load_xml(xml_file):
    Read_file(" https://df-dev.bk.rw/interview01/customers","customers.xml")
    #parsing the xml file
    tree = ET.parse(xml_file)
    my_root = tree.getroot()
    return my_root

#loading data
def getData():
    print("API started!!")
    transactions= load_json("transactions.json")
    customers= load_xml("customers.xml")
    return transactions,customers
