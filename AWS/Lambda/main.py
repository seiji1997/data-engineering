import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# データセットの作成例
data1 = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
data2 = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [15, 25, 35, 45]})

# 1つ目のプロット
fig = px.line(data1, x='x', y='y', title='重ね合わせプロットの例')

# 2つ目のプロットを重ねてグレーに設定
fig.add_trace(go.Scatter(x=data2['x'], y=data2['y'], mode='lines', name='データ2', line=dict(color='gray')))

# 表示
fig.show()

# ---------------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# データセットの作成例
data1 = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
data2 = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [15, 25, 35, 45]})
data3 = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [20, 30, 40, 50]})
data4 = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [5, 15, 25, 35]})

# サブプロットの作成
fig = make_subplots(rows=2, cols=2, subplot_titles=('Plot 1', 'Plot 2', 'Plot 3', 'Plot 4'))

# 各サブプロットにデータを追加
fig.add_trace(go.Scatter(x=data1['x'], y=data1['y'], mode='lines', name='データ1'), row=1, col=1)
fig.add_trace(go.Scatter(x=data2['x'], y=data2['y'], mode='lines', name='データ2'), row=1, col=2)
fig.add_trace(go.Scatter(x=data3['x'], y=data3['y'], mode='lines', name='データ3'), row=2, col=1)
fig.add_trace(go.Scatter(x=data4['x'], y=data4['y'], mode='lines', name='データ4'), row=2, col=2)

# 全体のレイアウトを調整
fig.update_layout(title='4つのサブプロットを並べて表示', height=600, width=800)

# 表示
fig.show()