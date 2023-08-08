import random
import pandas as pd


if __name__ == "__main__":
    csv_file = "twcs.csv"
    split_count = 80
    df = pd.read_csv(csv_file)
    number_of_rows = df.shape[0]
    unique_numbers = random.sample(range(0, number_of_rows), split_count-1)
    unique_numbers.sort()

    unique_numbers.append(number_of_rows)
    unique_numbers.insert(0, 0)
    for i in range(len(unique_numbers) - 1):
        start = unique_numbers[i]
        end = unique_numbers[i + 1]
        # print(start, end)
        df[start:end].to_csv(f"./Test2/split_{i}.csv", index=False)

    
        