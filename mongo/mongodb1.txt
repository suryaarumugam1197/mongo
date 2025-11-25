import pymongo
from pymongo import MongoClient, UpdateOne, UpdateMany
from pprint import pprint
import pandas as pd
from pandas import DataFrame
import json
import numpy as np
from datetime import datetime
#import Consonants as cs
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["repo"]
mydb2 = myclient["master_repo"]
mycol2 = mydb2["master_collection"]
mycol = mydb["golden_repo"]
mycol1=mydb["guid_repo"]
cur=mycol1.find()
list_cur=list(cur)
#Before unmerge operation
guid_df=pd.DataFrame(list_cur)
print(guid_df)
# getting the input file from the collection ##

def getfilepath():
    a=mycol2.find({},{'domain_id':1,'IBUCKETPATH':1})
    path=[]
    for data in a:
        c=((data['IBUCKETPATH']))
        path.append(str(c))
        print(type(path))
        path1=' '.join(map(str, path))
        print(type(path1))
        return path1
a=getfilepath()

def inputid(a):
    data=open(a,'r')
    input_df = pd.read_csv(data)
    print(input_df)
    a = input_df['guid'].tolist()
    print(a)
    golden_df = pd.DataFrame(list_cur)
    print(golden_df)
    b = golden_df['GUID'].tolist()
    print(b)
    c = [int(x) for x in b]
    #Comparing two lists#
    merge_guid = []
    for i in a:
        if i in c:
            print('Matched records with GR which needs to be unmerged', (i))
            merge_guid.append(str(i))
        else:
            print('Records which doesnot match', (i))
    print(type(merge_guid))
    print(merge_guid)
    return merge_guid
#
#
# # #Calling the input function with file as a parameter
# # #file = "D:/Users/input/guid_unmerge.txt"
res=inputid(a)
# print(res)
# #
# #
class unmerge:
    def __init__(self,id):
        self.id=id

    #Function for adding the flag field  for the GUIDS unmerge
    def unmerge_golden(self):
        for item in self.id:
            filter, update = {"GUID": item}, {"$set": {"unmerge": "yes"}}
            mycol.update_many(filter, update)
            print("Unmerged Record {} Updated in Mongodb".format(item))

# Function for removing  the  indicator fields and updating them with respect to the unmergedGUID
    def unmerge_remove_ind_guid(self):
        # col_name = 'Consolidation_Ind'
        for item in self.id:
            filter, update = {"GUID": item}, {'$unset': {'Consolidation_Ind': " "}}
            # db.testcollection.update_many({}, {"$unset": {f"{col_name}": 1}})
            mycol1.update_many(filter, update)
            # { $unset: {name: "", weight: ""}}
            print("Removed the consolidationindicatorof {} and Updated in Mongodb".format(item))

# Function for adding the OLD GUID fields and updating unmerged GUIDS
    def unmerge_guidrepo_update_oldguid(self):
        for item in self.id:
            filter, update = {"GUID": item}, {"$set": {"OLDGUID": item}}
            mycol1.update_many(filter, update)
            print("Updated oldguid {} in Mongodb".format(item))
# Function for emptying the  GUID fields and updating them with respect to the unmergedGUID
    def unmerge_empty_guid(self):
        for item in self.id:
            filter, update = {"GUID": item}, {"$set": {"GUID": " "}}
            mycol1.update_many(filter, update)
            print("Removed the oldguid {} and Updated in Mongodb".format(item))
def main():
    b = unmerge(inputid(getfilepath()))
    b.unmerge_golden()
    b.unmerge_remove_ind_guid()
    b.unmerge_guidrepo_update_oldguid()
    b.unmerge_empty_guid()
main()