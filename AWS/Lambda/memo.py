import plotly.express as px
import pandas as pd

# データフレームを作成
df = pd.DataFrame({
    'x': [1, 2, 3, 4],
    'y': [10, 11, 12, 13],
    'color': ['A', 'B', 'A', 'B']
})

# px.scatter でプロット
fig = px.scatter(df, x='x', y='y', color='color')
fig.show()


import plotly.graph_objects as go

# カテゴリごとにプロットを追加
fig = go.Figure()

# ユニークな色のカテゴリを取得
for category in df['color'].unique():
    filtered_df = df[df['color'] == category]  # カテゴリごとにフィルタリング

    # go.Scatter でデータを追加
    fig.add_trace(go.Scatter(
        x=filtered_df['x'],
        y=filtered_df['y'],
        mode='markers',
        name=category  # 凡例にカテゴリを表示
    ))

fig.show()