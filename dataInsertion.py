from pymongo import MongoClient
import argparse
import json
import os
from time import perf_counter

def main(input):
    
    # This is the default spot that mongoDB runs at.
    # If this doesn't work then check where MongoDB Compass is running the database.
    client = MongoClient("mongodb://localhost:27017")
    
    # The database to use
    db = client["YoutubeData"]
    
    # The specific collection we want in the database
    collection = db["Video"]
    
    
    # Clear prior data in collection for now.
    collection.delete_many({})
    
    for path in input:
        jsonPath = os.getcwd() + "\\cleanData\\" + path
        documents = []
        with open(jsonPath, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    try:
                        documents.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"skipping line due to JSON error: {e}")
       
    # Assuming that document sis not an empty array, insert all of its data into the current collection
    if documents:
        start_time = perf_counter()
        result = collection.insert_many(documents) 
        end_time = perf_counter()
        duration = end_time - start_time
        print(f"inserted {len(result.inserted_ids)} documents into collection")
        print(f"ingestion took {duration:.4f} seconds")
    else:
        print("No valid data found in file")
        
    
    # This line ensures that the data has been properly added to the collection by querying
    # the database to fetch the number of documents inside the video collection.
    print(f"There are {collection.count_documents({})} documents in the video collection")
        
if __name__ == "__main__":
    paths = os.listdir(".\\cleanData")
    main(paths);

""" 
Pseudocode:

BEGIN PROGRAM

1. DEFINE FUNCTION main(input_files):

   // Step 1: Connect to MongoDB
   connect to MongoDB at "mongodb://localhost:27017"
   select database "YoutubeData"
   select collection "Video"

   // Step 2: Reset collection
   delete all documents from "Video" collection

   // Step 3: Process each input file
   FOR each filename in input_files:
       build full path = current working directory + "\cleanData\" + filename
       create empty list 'documents'

       OPEN file at full path
       FOR each line in file:
           trim whitespace
           IF line is not empty:
               TRY
                   parse line as JSON
                   append parsed object to 'documents'
               CATCH JSON error:
                   print "skipping line due to JSON error"

   // Step 4: Insert into MongoDB
   IF 'documents' list is not empty:
       record start time
       insert all documents into collection
       record end time
       calculate duration = end - start
       print "inserted N documents"
       print "ingestion took X seconds"
   ELSE:
       print "No valid data found in file"

   // Step 5: Verify ingestion
   count = number of documents in collection
   print "There are count documents in the video collection"

2. MAIN EXECUTION:
   list all files in ".\cleanData" directory
   call main(list_of_files)

END PROGRAM """