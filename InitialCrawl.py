import pandas as pd
import numpy as np
import requests
import json
import time
import os

def parseAddr(block, street_name): # HTML Formatting
    addr = str(block)+' '+street_name
    addr = addr.replace(' ', '%20')
    addr = addr.replace('.', '')
    addr = addr.replace('\'S', '')
    return addr

def requestAndParse(addr):
    temp_value={}
    data = requests.get("https://developers.onemap.sg/commonapi/search?searchVal="+addr+"&returnGeom=Y&getAddrDetails=Y&pageNum=1")
    time.sleep(0.245) # OneMap API limits to 250 calls per minute
    data = json.loads(data.text)
    if data['found']==0:
        temp_value['Longitude']=""
        temp_value['Latitude']=""
        temp_value['Postal Code']=str("")
    else:
        temp_value['Longitude']=data['results'][0]['LONGITUDE']
        temp_value['Latitude']=data['results'][0]['LATITUDE']
        if data['results'][0]['POSTAL']=='NIL': #If first result does not have postal code
            if data['found']>1: #If there are more than 1 results
                temp_value['Postal Code']=str(data['results'][1]['POSTAL'])
            else: #Else, just leave postal code as blank.
                temp_value['Postal Code']=str("")
        else:
            temp_value['Postal Code']=str(data['results'][0]['POSTAL'])
    return temp_value

def initialCrawl(df, outputFileName):
    uniqueAddr = []
    for i, row in df.iterrows():
        addr = parseAddr(row['block'], row['street_name'])
        uniqueAddr.append(addr)
    uniqueAddr = list(set(list(uniqueAddr))) # Deduplicate addresses
    
    temp_key = {}
    for i in uniqueAddr:
        print(i)
        temp_value=requestAndParse(i) # Get data from OneMap API & Process it
        temp_key[i]=temp_value
        
        
    with open(outputFileName, "w") as outfile: #Dump into addressmap.json
        json.dump(temp_key, outfile)
    return temp_key

def mapPostalToAddr(df, loaded_json):
    for i, row in df.iterrows():
        addr = parseAddr(row['block'], row['street_name'])
        lookupVal = loaded_json[addr]
        df.at[i, 'Longitude']=lookupVal['Longitude']
        df.at[i, 'Latitude']=lookupVal['Latitude']
        df.at[i, 'Postal Code']=str(lookupVal['Postal Code'])
    return df

def getFileName(fileType):
    arr = os.listdir()
    arr.sort()
    count = 1
    fileMap = []
    for i in arr:
        if(i[-len(fileType):]!=fileType):
            continue
        print(str(count)+")", i)
        fileMap.append(i)
        count+=1
    print("0) Exit")
    choice = int(input())
    while choice < 0 or choice > len(fileMap):
        print("Please enter a valid input: ")
        choice = int(input())
    if choice == 0:
        exit()
    fileName = fileMap[choice-1]
    return fileName

if __name__ == "__main__":
    print("Select CSV File To Generate Postal Code:")
    fileName = getFileName(".csv")
    df = pd.read_csv(fileName)

    outputFileName = str(input("Provide a name for the output file (ending with .json): "))
    while outputFileName[-5:] != '.json' or outputFileName[0]=='.':
        print("Please enter a valid input: ")
        outputFileName = str(input())

    print("\nProcessing...")
    outputJson = initialCrawl(df, outputFileName)
    print("\nAddress-Coordinates/Postal Mapping (JSON) exported to", outputFileName)

    labeledDf = mapPostalToAddr(df, outputJson)

    tempDf = labeledDf.copy()
    labeledDf['Postal Code'].replace('', np.nan, inplace=True)
    labeledDf.dropna(subset=['Postal Code'], inplace=True)
    deduplicatedLabeledDf = labeledDf[['town', 'block', 'street_name', 'Longitude', 'Latitude', 'Postal Code']].drop_duplicates()
    deduplicatedLabeledDf.reset_index(drop=True, inplace=True)
    addressPostalFileName = outputFileName[:-5]+".csv"
    deduplicatedLabeledDf.to_csv(addressPostalFileName)
    print("Address-Coordinates/Postal Mapping (CSV) exported to", addressPostalFileName)

    outputFileName = fileName[:-4]+"-labeled.csv"
    tempDf.to_csv(outputFileName)
    print("Resale Price Dataset with Postal Code & Coordinates exported to", outputFileName)