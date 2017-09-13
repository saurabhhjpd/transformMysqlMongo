from pymongo import MongoClient
from datetime import datetime


import MySQLdb
import array
import json

# Open database connection
db = MySQLdb.connect("localhost", "root", "jpd123", "elocker")

# prepare a cursor object using cursor() method
cursor = db.cursor()

def mysqlStructure():
    sql = "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = 'elocker'  "
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        return results
    
    except:
        print "Error: unable to fecth data from schema"
        
def mysqlTableStructure(tableName):
    try:
#         sqlTable = "select * from " + tableName + " limit 2"
        #aadhaar_token
        sqlTable = "select * from aadhaar_token limit 200"
        query = cursor.execute(sqlTable)
        resultsTable = cursor.fetchall()
        num_fields = len(cursor.description)
        field_names = [i[0] for i in cursor.description]
        lenEle = len(field_names)
               
        tableData = []
        counterTable = 0;
        for val in resultsTable:
            counterColumns = 0
            rowData = []
            while counterColumns < lenEle:              
                rowData.append({field_names[counterColumns]:val[counterColumns]}) 
                counterColumns += 1
                if(counterColumns == lenEle):
                    dataInsert = rowData
                    mongoinsert(tableName, rowData)
#                     tableData.append({tableName : rowData})
#                     mongoinsert(tableName, dataInsert)
#         return tableData
    
    except:
        print "Error: unable to fecth data"
        
def mongoinsert(tablename, data):
    client = MongoClient("mongodb://localhost:27017")
    dbs = client.check
    coll = dbs[tablename]
    result = coll.insert_one({tablename : data})          
 
    print result

        
def transform():
    tables = mysqlStructure()
    for tablename in tables:
        tableContents = mysqlTableStructure(tablename[0])  
        print "---*********-----------"
        print tablename[0]
        print "---*********-----------"
#             
# #         print rows
#         break
        
#         print "------------------------ // "
# mysqlTableStructure("aadhaar_token")    
transform()   
exit           

# sql = "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = 'elocker'  "
# try:
#    # Execute the SQL command
#    cursor.execute(sql)
#    # Fetch all the rows in a list of lists.
#    results = cursor.fetchall()
#    for row in results:
#       sqlTable = "select * from "+ row[0]
#       query = cursor.execute(sqlTable)
#       resultsTable = cursor.fetchall()
#       num_fields = len(cursor.description)
#       field_names = [i[0] for i in cursor.description]
#       lenEle = len(field_names)
#       
#       tableName = []
#       
#       for val in resultsTable:
#           
#           counterColumns = 0
#           rowData = []
#           while counterColumns < lenEle:              
#               rowData.append({field_names[counterColumns]:val[counterColumns]}) 
#               counterColumns += 1
#               
#           tableName = {row[0] : rowData}
#          
#           
#           client = MongoClient("mongodb://localhost:27017")
#           dbs = client.check
#           coll = dbs[row[0]]
#           print coll
#                                 
#           result = coll.insert_one({row[0] : tableName})
#           
# 
#           print result.inserted_id
#               
#       
# except:
#    print "Error: unable to fecth data"

# disconnect from server
db.close()

exit

