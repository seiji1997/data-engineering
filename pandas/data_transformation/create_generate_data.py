import pandas as pd
import numpy as np
import os

# カラムの設定
dr_values = np.arange(0, 481, 3)
wecyc_values = [100, 1500, 3000]
page_values = ['Lower', 'Middle', 'Upper', 'Top']
block_values = np.random.randint(10000000, 99999999, 48)
wl_values = np.arange(1, 163)
string_values = np.arange(5)
uid_values = np.random.randint(10000000, 99999999, 8)
fbc_range = np.random.randint(10, 7001, 6000)

# データの作成
data = []

for dr in dr_values:
    for wecyc in wecyc_values:
        for page in page_values:
            for block in block_values:
                for wl in wl_values:
                    for string in string_values:
                        for uid in uid_values:
                            fbc = np.random.choice(fbc_range)
                            data.append([dr, wecyc, page, block, wl, string, uid, fbc])

# データフレームの作成
df = pd.DataFrame(data, columns=['DR', 'WECyc', 'Page', 'Block', 'WL', 'String', 'Uid', 'FBC'])

# CSVファイルに保存
os.makedirs('data_manipulation', exist_ok=True)
df.to_csv('data_manipulation/processed.csv', index=False)

print(df.head())
