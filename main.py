import os
import pandas as pd
import numpy as np
from threading import Thread
from queue import Queue

def compute_stats(filepath, output_queue):
    data = pd.read_csv(filepath)
    expected_columns = ['Open', 'High', 'Low', 'Close']
    
    if not all(col in data.columns for col in expected_columns):
        output_queue.put((filepath, "Error: Missing one or more required columns"))
        return

    for col in expected_columns:
        data[col] = pd.to_numeric(data[col], errors='coerce')

    stats = {
        'count': data[expected_columns].count(),
        'mean': data[expected_columns].mean(),
        'std': data[expected_columns].std(),
        'min': data[expected_columns].min(),
        'max': data[expected_columns].max()
    }
    output_queue.put((filepath, stats))

def print_stats(filepath, stats):
    if isinstance(stats, dict):
        print(f"\nStats for {os.path.basename(filepath)}:")
        for stat_name, values in stats.items():
            print(f"{stat_name.title()}:")
            for col, value in values.items():
                print(f"  {col}: {value:.2f}")
    else:
        print(f"Error received for {filepath}: {stats}")

def main():

    threads = []
    output_queue = Queue()
    directory_path = './so_data'
    if not os.path.exists(directory_path):
        print(f"The directory does not exist: {directory_path}")
        return

    files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    if not files:
        print("No CSV files found in the directory.")
        return

    for file in files:
        filepath = os.path.join(directory_path, file)
        thread = Thread(target=compute_stats, args=(filepath, output_queue))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    while not output_queue.empty():
        filename, stats = output_queue.get()
        print_stats(filename, stats)

if __name__ == "__main__":
    main()