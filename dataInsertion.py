from pymongo import MongoClient
import argparse
import json
from time import perf_counter

def main(input):
    
    # This is the default spot that mongoDB runs at.
    # If this doesn't work then check where MongoDB Compass is running the database.
    client = MongoClient("mongodb://localhost:27017")
    
    # The database to use
    db = client["YoutubeData"]
    
    # The specific clooction we want in the database
    collection = db["Video"]
    
    
    # Clear prior data in collection for now.
    collection.delete_many({})
    
    
    # Read the input json file line by line to build an array to pass to MongoDB.
    jsonPath = input
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
    parser = argparse.ArgumentParser(description="Reduce dataset to manageable size")
    parser.add_argument("inputFile")
    args = parser.parse_args()
    main(args.inputFile)