import pandas as pd


processed_lines = []
# with open('twcs.csv', 'r') as file:
#     for line in file:
#         parts = line.strip().split(',')
#         processed_line = ','.join(parts[4:-2]).strip(',')
#         # print(processed_line)
#         if processed_line and processed_line[0] == '"' and processed_line[-1] == '"':
#             processed_line = processed_line[1:-1]
        
#         processed_lines.append(processed_line)
        
        
print("File read complete")

# Read the processed CSV file using pandas
df = pd.read_csv('twcs.csv', header=None, names=['text'])

# Iterate through rows and compare with the list
for index, row in df.iterrows():
    print(index)
    if row['text'] != processed_lines[index]:
        print(f"Row Number: {index + 1}, Original Text: {processed_lines[index]}, Data in DataFrame: {row['text']}")

