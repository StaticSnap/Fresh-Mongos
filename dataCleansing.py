# Creates a cleaned up and smaller subset for testing in a json file. maxRows allows users to set a limit on how many lines to put into the output file. (Cleansing/Transformation)
import csv
import json
import os

def cleanRow(row):
    if len(row) < 9: # Clean out empty/incomplete rows
        return None
    try:
        # Transforms the row into data that is normalized & can be used.
        return {
            "videoID": row[0].strip(),
            "uploader": row[1].strip(),
            "category": row[3].strip(), # trim out whitespaces
            "duration": int(row[4]) if row[4].isdigit() else None,
            "views": int(row[5]) if row[5].isdigit() else None,
            "rating": float(row[6]) if row[6].replace('.', '', 1).isdigit() else None,
            "related": [r.strip() for r in row[9:] if r.strip()]
        }
    except Exception: # error 
        return None

def processRows(inp, outp, maxRows=None): # maxRows provides a max amount to parse to keep within a data limit, but can be deleted if you want to parse the entire file
    with open(inp, 'r', encoding='utf-8') as f, open(outp, 'w', encoding='utf-8') as out:
        reader = csv.reader(f, delimiter='\t')
        for i, row in enumerate(reader):
            if maxRows and i >= maxRows: # If reached limit, stop there
                break
            clean = cleanRow(row)
            # If row is correctly cleaned by cleanRow(row)
            if clean:
                out.write(json.dumps(clean) + '\n') 

if __name__ == "__main__":
    contents = os.listdir(".\\allData")
    print(contents)
    for item in contents:
        inputPath = os.getcwd() + "\\allData\\" + item + "\\" + item + "\\" + "3.txt"
        outputPath = os.getcwd() + "\\cleanData\\" + item + ".json";
        print(inputPath);
        processRows(inputPath,outputPath)


""" 
Pseudocode:

open input file for reading
open output file for writing

for each line (i, row) in input file:
    if maxRows is set and i >= maxRows:
        stop loop
    clean = cleanRow(row)
    if clean is valid:
        write clean object to output file

"""