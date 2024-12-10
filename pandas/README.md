# data-engineering
learn data engineering, such as SQL on BigQuery and Python for big data using pandas.

### data science 100 for pandas


### pandas 100
https://qiita.com/kunishou/items/bd5fad9a334f4f5be51c


以下にPandasのメソッドに対応するPolarsでの記述例や概念上の相違点、注意点などをまとめます。

メソッド対応一覧
	1.	pd.read_csv
Pandas: df = pd.read_csv("data.csv")
Polars: df = pl.read_csv("data.csv")
	•	Polarsではpl.scan_csv("data.csv")でlazyな読み込みも可能で、大規模データに対するパフォーマンス向上が期待できます。
	2.	.apply
Pandas: df["col"].apply(func)
Polars: df.with_columns(pl.col("col").apply(func))
	•	Polarsではapplyはできるだけ避け、Polarsの式（expression）ベースで記述することが推奨されます。applyはPython関数呼び出しとなり、ベクトル化や並列化が効きにくいため性能が低下します。
	•	代替としてpl.col("col").map(lambda x: ...)や組み込み関数、UDF表現を使うと良いでしょう。
	3.	.explode
Pandas: df.explode("list_col")
Polars: df.explode("list_col")
	•	概ね同様の挙動で、リスト型の列を行方向に拡張します。
	4.	.drop
Pandas: df.drop(columns=["col"])
Polars: df.drop("col")
	•	Polarsでは行方向へのdropよりも列方向の操作が基本です。df.drop(["col1", "col2"])のように複数列も同様に指定できます。
	5.	.set_index
Pandas: df.set_index("col")
Polars: Polarsには「行インデックス」の概念がありません。
	•	Polarsは常に0基準の行ラベル（内部的な単純なrow count）を利用します。
	•	「インデックス列」に相当する列を持ちたい場合は、単にその列を残しておくだけになります。行インデックスによる結合や検索を行いたい場合はjoinのキー列として用いるか、with_row_countで行番号列を明示的に付与する方法があります。
	6.	.join
Pandas: df.join(other_df, on="key")またはpd.merge()
Polars: df.join(other_df, on="key", how="inner")
	•	Polarsではmergeはなく、joinを使用します。
	•	df.join(df2, on="key", how="left")などと書きます。結合方法はinner, left, outer, semi, antiなどから指定可能。
	7.	.reset_index
Pandas: df.reset_index(drop=True)
Polars: Polarsは行インデックスがないためreset_indexは不要
	•	必要な場合はwith_row_count("new_id")で行番号列を追加するなどします。
	8.	.astype
Pandas: df["col"] = df["col"].astype("int32")
Polars: df = df.with_columns(pl.col("col").cast(pl.Int32))
	•	Polarsでは.cast()メソッドで型変換を行います。df.with_columns(...)パターンで新たに変換後の列を生成して返すイミュータブルな操作が基本です。
	9.	.groupby
Pandas: df.groupby("col").agg(...)
Polars: df.groupby("col").agg([...])
	•	基本的な構文は似ていますが、Polarsは列指向であり、aggでは列ごとの集約処理をオブジェクトメソッドチェーンで指定します。

データ保持の仕方・注意点
	•	インデックスの概念がない
Pandasは行インデックスがあり、set_indexやreset_indexがしばしば使われますが、Polarsはすべてのデータをカラム指向で保持し、行インデックスは持ちません。そのため、インデックス操作を念頭に置いた処理ロジックはPolarsでは直接利用できません。
	•	イミュータブルなDataFrame
PolarsのDataFrameは基本的にイミュータブルであり、メソッドを呼び出すたびに新たなDataFrameを返すため、inplace=Trueのような操作は存在しません。その分、チェーン操作（メソッドチェーン）による宣言的なデータ変換が推奨されます。
	•	パフォーマンスとメソッド選択
applyのようにPython側の関数を直接当てるメソッドはPolarsでは性能劣化を招きます。可能な限りPolars組み込みの表現（expressions）やUDFを使うことで、Polarsの並列化・SIMD最適化などが最大限生かされ、高速化できます。
	•	Lazyモード vs Eagerモード
PolarsにはLazyモードがあり、pl.scan_csv()でデータを遅延読み込みし、最後にcollect()で実行することで、クエリプラン最適化を自動的に行います。大規模データや複雑な前処理ではLazyモードを活用することでメモリ使用量の削減や計算時間の短縮が可能です。

対策・考え方
	•	インデックスに依存しないデータ操作ロジックを考える（Key列を常に明示的に保持する）。
	•	applyやPythonループを使わずPolarsの式を活用することを心がける。
	•	Lazy APIで巨大データセットの処理を最適化する。
	•	カラム単位の操作を基本とし、Polarsのメソッドチェーンを利用して明確で高速なデータ変換パイプラインを築く。

以上の点を踏まえることで、PandasからPolarsへの移行時に生じるメソッド呼び出しや概念的な差異をスムーズに克服できます。