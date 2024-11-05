
import pandas as pd

# サンプルの関数定義（numを使ってデータフレームを生成）
def def1(num):
    # numを使ってデータフレームを作成（例としてnumに基づいた列を追加）
    return pd.DataFrame({'value': [num] * 3})  # サンプルとして3行分のデータフレーム

# 辞書型データ
data_dict = {'df1': 1000, 'df2': 5000}

# 辞書のキーと値を使ってデータフレームを生成
result_dict = {}
for df_name, num in data_dict.items():
    result_dict[df_name] = def1(num)

# 各データフレームの内容を確認
for name, df in result_dict.items():
    print(f"DataFrame {name}:\n{df}\n")