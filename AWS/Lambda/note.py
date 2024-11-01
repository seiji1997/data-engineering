import polars as pl

df = pl.read_parquet("your_file.parquet")

df = df.with_columns(
    pl.col("column_name")
    .str.replace_all(r'^"(.*)"$', r'\1')
    .cast(pl.Utf8)
)

print(df)