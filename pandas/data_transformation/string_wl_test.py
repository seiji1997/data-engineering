import pandas as pd

# テストデータの作成
units = list(range(448))
data = {
    'Unit': units,
}

df = pd.DataFrame(data)

# ステップ10: stringを作成する
df['String'] = df.groupby('Unit').cumcount() % 4

# ステップ11: WLを作成する
df['WL'] = df['Unit'] // 4

import ace_tools as tools; tools.display_dataframe_to_user(name="String and WL Data", dataframe=df)
