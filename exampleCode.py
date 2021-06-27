import requests, os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import matplotlib
import matplotlib.pyplot as plt

#############################
#                           #
#    Methods used for       #
# interacting with the API  #
#                           #
#############################

def pullDataFromAPIintoPandasDF():
    #API docs: https://github.com/HackerNews/API
    
    #create dataframe
    col_names = ['Story_ID', 'Title', 'Author', 'Score', 'Link']
    dataframe  = pd.DataFrame(columns = col_names)
    
    #get most recent post ID
    response = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty")
    mostRecent = response.json()

    #grab last 1000 data points from API
    dataframe = getDataFromAPI(dataframe, int(mostRecent)-5000, int(mostRecent)-500)
    
    print(dataframe)
    return dataframe
    
def loadNewData(dataframe):
    #get the newest story in old dataset
    dataframe.sort_values(by='Story_ID', inplace=True, ascending=False)
    newestStorySaved = dataframe.iloc[0,0]
    
    #get most recent post ID from API
    response = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty")
    mostRecent = response.json()
    
    #grab data from newestStorySaved to mostRecent from API
    dataframe = getDataFromAPI(dataframe, int(newestStorySaved), int(mostRecent)-1)
      
    dataframe.sort_values(by='Story_ID', inplace=True, ascending=False)
    return dataframe

def getDataFromAPI(dataframe, id_start, id_end):
    response = requests.get("https://hacker-news.firebaseio.com/v0/item/"+str(id_start)+".json?print=pretty")
    data = response.json()
    
    #grab story data from API
    for i in range(id_start, id_end):
        if data is not None and data['type'] == 'story' and 'title' in data and 'by' in data and 'score' in data and 'url' in data:
            dataframe.loc[len(dataframe.index)] = [data['id'], data['title'].encode('utf-8'), data['by'].encode('utf-8'), data['score'], data['url'].encode('utf-8')]
            print("Added " + data['title'])
        response = requests.get("https://hacker-news.firebaseio.com/v0/item/"+str(i)+".json?print=pretty")
        data = response.json()
      
    return dataframe
  
############################
#                          #
#    Generalized methods   #
#    for migrating data    #
#                          #
############################

def saveSQLtoFile(filename, database_name):
    os.system('mysqldump -u root -pcodio '+database_name+' > '+ filename)

def loadSQLfromFile(filename, database_name):
    #create database if it does not exist
    os.system('mysql -u root -pcodio -e "CREATE DATABASE IF NOT EXISTS '+database_name+';"')
    os.system("mysql -u root -pcodio "+database_name+" < " + filename)
  
def createEngine(database_name):
    return create_engine('mysql://root:codio@localhost/'+database_name+'?charset=utf8', encoding='utf-8')
    
def updateDataset(database_name, table_name, filename):
    loadSQLfromFile(filename, database_name)
    df = pd.read_sql_table(table_name, con=createEngine(database_name))
    return loadNewData(df)
    
def saveDatasetToFile(database_name, table_name, filename, dataframe):
    dataframe.to_sql(table_name, con=createEngine(database_name), if_exists='replace', index=False)
    saveSQLtoFile(filename, database_name)

    
############################
#                          #
#     Main Driver Code     #
#                          #
############################    
    
database_name = 'hackernews'
table_name = 'stories'
filename = 'data-dump.sql'

#creating dataset from scratch
df = pullDataFromAPIintoPandasDF()
saveDatasetToFile(database_name, table_name, filename, df)

# updating existing dataset
# df = updateDataset(database_name, table_name, filename)
# saveDatasetToFile(database_name, table_name, filename, df)



