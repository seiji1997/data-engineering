#!/usr/bin/env python3
"""
検証スクリプト:
1) A/B の結果を小数点以下1〜20桁で Pandas と Polars で比較
2) 要素数 64,127,128,129,256 のときの合計(sum)を比較
"""

import numpy as np
import pandas as pd
import polars as pl

# 計算対象の値
A = 12345.12345
B = 39370

# 要素数リスト
COUNTS = [64, 127, 128, 129, 256]

def compare_decimal_precision(A: float, B: float, max_dp: int = 20):
    # Pandas (Python float) 側
    pd_ratio = A / B
    # Polars 側
    df_pl = pl.DataFrame({"A": [A], "B": [B]})
    pl_ratio = df_pl.select((pl.col("A") / pl.col("B")).alias("ratio")).to_series()[0]

    print("=== Decimal precision test (A / B) ===")
    print(" dp |          pandas          |          polars          | equal")
    print("----+--------------------------+--------------------------+-------")
    for dp in range(1, max_dp + 1):
        pd_val = round(pd_ratio, dp)
        pl_val = round(pl_ratio, dp)
        # 文字列比較で桁揃えも確認
        pd_s = format(pd_val, f".{dp}f")
        pl_s = format(pl_val, f".{dp}f")
        equal = (pd_s == pl_s)
        print(f"{dp:3d} | {pd_s:>24s} | {pl_s:>24s} | {equal}")

def compare_element_counts(counts):
    print("\n=== Element count test (sum) ===")
    print("   n |         pandas_sum         |         polars_sum         |    diff")
    print("-----+----------------------------+----------------------------+-----------")
    for n in counts:
        # データ生成: 大きい値 1 つ + 小さい値 (1e-8) を n-1 個
        big   = np.array([1e8], dtype=np.float64)
        small = np.array([1e-8] * (n - 1), dtype=np.float64)
        data  = np.concatenate((big, small))

        pd_sum = pd.Series(data).sum()
        pl_sum = pl.DataFrame({"x": data}).select(pl.col("x").sum()).to_series()[0]
        diff   = pl_sum - pd_sum

        print(f"{n:5d} | {pd_sum:>26.12e} | {pl_sum:>26.12e} | {diff:> .2e}")

if __name__ == "__main__":
    compare_decimal_precision(A, B, max_dp=20)
    compare_element_counts(COUNTS)