from InitialCrawl import parseAddr, requestAndParse, mapPostalToAddr, getFileName
import pandas as pd
import numpy as np
import requests
import json
import time
import os

def subsequentCrawl(df, loaded_json, outputFileName):
    noPostal=[]
    unvisitedAddr = []
    print("Finding Existing Addr")
    for i, row in df.iterrows():
        addr = parseAddr(row['block'], row['street_name'])
        if addr in loaded_json: # Look for the address in existing dataset
            continue
        else: #If address not found, add to unvisitedAddr list.
            unvisitedAddr.append(addr)
    unvisitedAddr=list(set(list(unvisitedAddr)))
    print("Searching Addr")
    temp_key = {}
    for i in unvisitedAddr:
        print(i)
        temp_value=requestAndParse(i)
        loaded_json[i]=temp_value
        if temp_value['Postal Code']=="":
            noPostal.append(i)

    noPostal = list(set(noPostal))
    if(len(noPostal)>0):
        print("\nThe following address have no postal code found:")
        for i in noPostal:
            print(i)
        print("Consider manually adding them inside", outputFileName, "by using a text editor to search for the above keywords, if block still exists")

    with open(outputFileName, "w") as outfile: #Dump into addressmap.json
        json.dump(loaded_json, outfile)    
    return loaded_json


if __name__ == "__main__":
    print("Select CSV File To Generate Postal Code:")
    fileNameCSV = getFileName(".csv")
    df = pd.read_csv(fileNameCSV)

    print("Select Existing Postal Code Mapping (.json file)")
    fileNameJSON = getFileName(".json")

    with open(fileNameJSON, "r") as infile: 
        loaded_json=json.load(infile)

    outputFileName = str(input("Provide a name for the output file (ending with .json): "))
    while outputFileName[-5:] != '.json' or outputFileName[0]=='.':
        print("Please enter a valid input: ")
        outputFileName = str(input())
        
    print("\nProcessing...")
    updatedJSON = subsequentCrawl(df, loaded_json, outputFileName)


    print("\nUpdated Address-Coordinates/Postal Mapping (JSON) exported to", outputFileName)

    labeledDf = mapPostalToAddr(df, updatedJSON)


    labeledDf['Postal Code'].replace('', np.nan, inplace=True)
    labeledDf.dropna(subset=['Postal Code'], inplace=True)
    deduplicatedLabeledDf = labeledDf[['town', 'block', 'street_name', 'Longitude', 'Latitude', 'Postal Code']].drop_duplicates()
    deduplicatedLabeledDf.reset_index(drop=True, inplace=True)
    addressPostalFileName = outputFileName[:-5]+".csv"
    deduplicatedLabeledDf.to_csv(addressPostalFileName)
    print("Address-Coordinates/Postal Mapping (CSV) exported to", addressPostalFileName)

    outputFileName = fileNameCSV[:-4]+"-labeled.csv"
    labeledDf.to_csv(outputFileName)
    print("Resale Price Dataset with Postal Code & Coordinates exported to", outputFileName)