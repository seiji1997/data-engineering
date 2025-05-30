# NumPy: 2 つの配列に対して四則演算
import numpy as np

a = np.array([1.0, 2.0, 3.0])
b = np.array([4.0, 5.0, 6.0])

add = a + b      # [5. 7. 9.]
sub = a - b      # [-3. -3. -3.]
mul = a * b      # [ 4. 10. 18.]
div = a / b      # [0.25 0.4  0.5 ]

# pandas: 2 つの DataFrame に対して四則演算
import pandas as pd

df1 = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
df2 = pd.DataFrame({"x": [7, 8, 9], "y": [10, 11, 12]})

add = df1 + df2      # 各列ごとに足し算
sub = df1 - df2      # 各列ごとに引き算
mul = df1 * df2      # 各列ごとに掛け算
div = df1 / df2      # 各列ごとに割り算

# Polars: 2 つの DataFrame に対して四則演算
import polars as pl

df1 = pl.DataFrame({"x": [1.0, 2.0, 3.0], "y": [4.0, 5.0, 6.0]})
df2 = pl.DataFrame({"x": [7.0, 8.0, 9.0], "y": [10.0, 11.0, 12.0]})

# 各演算は select の中で列同士を指定しても OK
add = df1.select([
    (pl.col("x") + df2["x"]).alias("x"),
    (pl.col("y") + df2["y"]).alias("y"),
])
sub = df1.select([
    (pl.col("x") - df2["x"]).alias("x"),
    (pl.col("y") - df2["y"]).alias("y"),
])
mul = df1.select([
    (pl.col("x") * df2["x"]).alias("x"),
    (pl.col("y") * df2["y"]).alias("y"),
])
div = df1.select([
    (pl.col("x") / df2["x"]).alias("x"),
    (pl.col("y") / df2["y"]).alias("y"),
])