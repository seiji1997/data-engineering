import pandas as pd
import numpy as np
import os

# ディレクトリ作成
os.makedirs('data_manipulation/we3000', exist_ok=True)
os.makedirs('data_manipulation/we10000', exist_ok=True)

# 3000_0.csvの作成
units = range(448)
segs = [0, 1, 2, 3]
shift_indexes = range(-7, 8)
shift_values = {
    'shiftA': range(-29, 14, 3),
    'shiftB': range(-25, 18, 3),
    'shiftC': range(-25, 18, 3),
    'shiftD': range(-20, 23, 3),
    'shiftE': range(-24, 19, 3),
    'shiftF': range(-23, 20, 3),
    'shiftG': range(-22, 21, 3),
}

# データフレームの作成
rows = []
for unit in units:
    for seg in segs:
        for shift_index in shift_indexes:
            row = {
                'Unit': unit,
                'seg': seg,
                'shiftIndex': shift_index
            }
            for shift_name, values in shift_values.items():
                row[shift_name] = values[shift_index + 7]
            row['fbcA'] = np.random.randint(0, 101) if shift_index == 0 else np.random.randint(10, 1001)
            row['fbcB'] = np.random.randint(0, 101) if shift_index == 0 else np.random.randint(10, 1001)
            row['fbcC'] = np.random.randint(0, 1001) if shift_index == 0 else np.random.randint(10, 10001)
            row['fbcD'] = np.random.randint(0, 1001) if shift_index == 0 else np.random.randint(10, 10001)
            row['fbcE'] = np.random.randint(0, 1001) if shift_index == 0 else np.random.randint(10, 10001)
            row['fbcF'] = np.random.randint(0, 1001) if shift_index == 0 else np.random.randint(10, 10001)
            row['fbcG'] = np.random.randint(0, 1001) if shift_index == 0 else np.random.randint(10, 10001)
            rows.append(row)

df_3000_0 = pd.DataFrame(rows)
df_3000_0.to_csv('data_manipulation/we3000/3000_0.csv', index=False)

# ファイルの保存先を指定して再保存
df_3000_0.to_csv('/mnt/data/3000_0.csv', index=False)
