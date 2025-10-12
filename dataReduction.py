# Reduces dataset by randomly finding a subset of lines. This reduction is done to work with a smaller dataset during milestones 1-3.
import csv
import argparse
import random
from collections import defaultdict

# Random sample
random.seed(42)

def reduceData(inp, outp, targetNum):
    lines = []
    with open(inp, 'r', encoding='utf-8') as f:
        for line in f:
            lines.append(line.strip())
    # If number of lines in file is less than targetNum, use the whole file
    if len(lines) <= targetNum:
        print("File smaller than target size. Copying all lines.")
        sampled = lines
    else:
        # Random sample 
        sampled = random.sample(lines, targetNum)
    # Saved dataset
    with open(outp, 'w', encoding='utf-8', newline='') as out:
        for line in sampled:
            out.write(line + '\n')

    print(f"Data reduction complete. {len(sampled)} lines saved to {outp}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reduce dataset to manageable size")
    parser.add_argument("inp")
    parser.add_argument("outp")
    parser.add_argument("--n", type=int, default=100000)
    args = parser.parse_args()

    reduceData(args.inp, args.outp, args.n)


"""
Pseudocode:

Algorithm sampleData(input, output, targetNum)
    # reduce dataset size by randomly selecting a subset of lines

    Initialize empty list lines

    Open input for reading
        For each line in input:
            Remove newline characters
            Append line to lines
    Close input

    If number of lines ≤ targetNum then
        sampled ← lines
    Else
        sampled ← randomly select targetNum lines from lines
    End If

    Open output for writing
        For each line in sampled:
            Write line followed by newline
    Close output

    Print "Data reduction complete. Saved", number of sampled, "lines."
End Algorithm
"""