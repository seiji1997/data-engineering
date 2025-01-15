#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ruler glue calculation (glue1 & glue2), with improved error handling:
  - Use sheet_id=1 instead of sheet_name="Sheet1"
  - If Excel has 0 or >=2 sheets, raise error
  - Check columns & dtypes at each file read
  - If files or columns are missing, raise clear errors
  - Output directory must not exist previously
"""

import sys
from pathlib import Path
from typing import Union
import yaml
import polars as pl
import numpy as np


def load_config(config_path: Union[str, Path]) -> dict:
    """
    1) config.yaml の存在チェック
    2) YAML を読み込み
    """
    if not Path(config_path).exists():
        raise FileNotFoundError(f"configファイルが見つかりません: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def check_columns_and_dtypes(
    df: pl.DataFrame,
    expected_cols: list[str],
    expected_dtypes: dict[str, pl.PolarsDataType],
    desc: str,
) -> None:
    """
    DataFrame のカラム名・型を検証し、不足や型不一致があれば ValueError を投げる。
    """
    actual_cols = df.columns

    for col in expected_cols:
        if col not in actual_cols:
            raise ValueError(
                f"{desc}: カラム '{col}' が見つかりません。実際のカラム: {actual_cols}"
            )

    for col, dt in expected_dtypes.items():
        if col not in df.columns:
            continue
        actual_dt = df.schema[col]
        if actual_dt != dt:
            raise ValueError(
                f"{desc}: カラム '{col}' の型が想定({dt})と異なります。実際の型: {actual_dt}"
            )


def read_excel_with_sheet_id(file: Union[str, Path], sheet_id: int) -> pl.DataFrame:
    """
    Polars で Excel を読み込む際、
    - sheet_name=None で全シートを取得 (dict)
    - シート数が 1枚 以外ならエラー
    - sheet_id=1 のみサポート。2以上ならエラー。
    """
    file_path = Path(file)
    if not file_path.exists():
        raise FileNotFoundError(f"Excelファイルが見つかりません: {file_path}")

    all_sheets = pl.read_excel(file=file_path, sheet_name=None)
    sheet_names = list(all_sheets.keys())

    if len(sheet_names) == 0:
        raise ValueError(f"Excelファイルにシートが存在しません: {file_path}")
    if len(sheet_names) > 1:
        raise ValueError(
            f"Excelファイルにシートが2つ以上あります。1シートのみ想定: {sheet_names}"
        )
    if sheet_id != 1:
        raise ValueError(
            f"sheet_id={sheet_id} が指定されましたが、シートは1枚しかありません。"
        )

    first_sheet = sheet_names[0]
    df = all_sheets[first_sheet]
    return df


def load_quotation_table_excel(xlsx_path: Path, sheet_id: int = 1) -> pl.DataFrame:
    """
    Excel(quotation) を読み込み、必要カラムを取り出して rename し、返す
    1) sheet_id=1 シート数確認
    2) カラムチェック+型チェック
    3) select + rename
    """
    df = read_excel_with_sheet_id(xlsx_path, sheet_id=sheet_id)

    expected_cols = [
        "ID",
        "force_refresh",
        "stapler_time",
        "tape",
        "ink_cycle",
        "tracking_marker",
        "read_crinkle",
    ]
    expected_types = {
        "ID": pl.Int64,
        "force_refresh": pl.Int64,
        "stapler_time": pl.Utf8,
        "tape": pl.Utf8,
        "ink_cycle": pl.Float64,
        "tracking_marker": pl.Float64,
        "read_crinkle": pl.Float64,
    }
    check_columns_and_dtypes(df, expected_cols, expected_types, desc="Excel(quotation)")
    df = df.select(expected_cols)

    rename_map = {
        "force_refresh": "stapler",
        "stapler_time": "condition_temp",
        "tape": "condition_time",
        "ink_cycle": "ink_cycle",
    }
    df = df.rename(rename_map)
    return df


def load_csv_condition_table(csv_path: Path) -> pl.DataFrame:
    """
    condition_table_test.csv の読み込み & カラムチェック
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSVファイルが見つかりません: {csv_path}")

    df = pl.read_csv(
        str(csv_path),
        dtypes={
            "condition_temp": pl.Int64,
            "condition_time": pl.Utf8,
            "scotch_temp": pl.Int64,
            "scotch_time": pl.Float64,
        },
    )
    expected_cols = ["condition_temp", "condition_time", "scotch_temp", "scotch_time"]
    expected_types = {
        "condition_temp": pl.Int64,
        "condition_time": pl.Utf8,
        "scotch_temp": pl.Int64,
        "scotch_time": pl.Float64,
    }
    check_columns_and_dtypes(df, expected_cols, expected_types, desc="CSV(condition_table)")
    return df


def load_csv_lut_table(csv_path: Path) -> pl.DataFrame:
    """
    ruler_pencilcase_table.csv 読み込み & カラムチェック
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"LUT CSVファイルが見つかりません: {csv_path}")
    df = pl.read_csv(
        str(csv_path),
        dtypes={
            "inkcycle_start": pl.Float64,
            "inkcycle_end": pl.Float64,
            "readmode": pl.Utf8,
            "tape": pl.Float64,
            "stapler": pl.Int64,
            "stapler_temp": pl.Float64,
            "filename": pl.Utf8,
        },
    )
    expected_cols = [
        "inkcycle_start",
        "inkcycle_end",
        "readmode",
        "tape",
        "stapler",
        "stapler_temp",
        "filename",
    ]
    expected_types = {
        "inkcycle_start": pl.Float64,
        "inkcycle_end": pl.Float64,
        "readmode": pl.Utf8,
        "tape": pl.Float64,
        "stapler": pl.Int64,
        "stapler_temp": pl.Float64,
        "filename": pl.Utf8,
    }
    check_columns_and_dtypes(df, expected_cols, expected_types, desc="CSV(LUT)")

    readmodes = set(df["readmode"].unique())
    allowed = {"Fast", "LNA"}
    if not readmodes.issubset(allowed):
        invalid = readmodes - allowed
        raise RuntimeError(f"未知のreadmodeが含まれています: {invalid}")
    return df


def load_parquet_fbc(parquet_path: Path) -> pl.DataFrame:
    """
    ruler_data/*.parquet を読み込み & カラムチェック
    sheet, stripe, ink, ruler が必須
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquetファイルが見つかりません: {parquet_path}")
    df = pl.read_parquet(str(parquet_path))

    required_cols = ["sheet", "stripe", "ink", "ruler"]
    for rc in required_cols:
        if rc not in df.columns:
            raise ValueError(f"Parquet({parquet_path}): '{rc}' がありません。")
    return df


def decide_mode_by_page(parquet_dir: Path, tlc_size: int, qlc_size: int) -> tuple[str, int]:
    """
    ruler_data/ の最初の *.parquet => sheet col unique => (tlc or qlc)
    """
    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"指定ディレクトリにParquetファイルがありません: {parquet_dir}")

    sample_path = parquet_files[0]
    df_sample = load_parquet_fbc(sample_path)

    unique_sheets = df_sample["sheet"].unique().to_list()
    unique_count = len(unique_sheets)

    if unique_count == 3:
        return ("tlc", tlc_size)
    elif unique_count == 4:
        return ("qlc", qlc_size)
    else:
        raise ValueError(f"sheetユニーク数({unique_count})が想定外(3/4以外未対応)")


def merge_condition_info(
    quotation_df: pl.DataFrame,
    condition_df: pl.DataFrame
) -> pl.DataFrame:
    merged = quotation_df.join(
        condition_df,
        how="left",
        left_on=["condition_temp", "condition_time"],
        right_on=["condition_temp", "condition_time"],
    )
    merged = merged.rename(
        {
            "scotch_temp": "tape",
            "scotch_time": "stapler_temp",
        }
    )
    merged = merged.drop(["condition_temp", "condition_time"])
    return merged


def extend_offdr_zero(merged_df: pl.DataFrame) -> pl.DataFrame:
    df_zero = merged_df.with_columns(pl.lit(0.0).alias("tape"))
    return pl.concat([merged_df, df_zero], how="vertical")


def _find_filename_for_readmode(
    lut_df: pl.DataFrame,
    readmode: str,
    we_cycle_val: float,
    offdr_val: float,
    ondr_val: int,
    ondr_temp_val: float,
    idx: int,
) -> str | None:
    matched = lut_df.filter(
        (pl.col("inkcycle_start") <= we_cycle_val)
        & (pl.col("inkcycle_end") >= we_cycle_val)
        & (pl.col("tape") == offdr_val)
        & (pl.col("stapler") == ondr_val)
        & (pl.col("stapler_temp") == ondr_temp_val)
        & (pl.col("readmode") == readmode)
    )
    if matched.shape[0] > 1:
        raise RuntimeError(
            f"重複ファイル: ID={idx}, readmode={readmode}, we_cycle={we_cycle_val}"
        )
    if matched.is_empty():
        return None
    return matched[0, "filename"]


def find_fbc_lut_records(
    extended_df: pl.DataFrame, lut_df: pl.DataFrame
) -> pl.DataFrame:
    results: list[tuple[int, float, str | None, str | None, float, float]] = []
    for row in extended_df.iter_rows(named=True):
        idx = row["ID"]
        wcycle = row["WE_cycle"]
        offv = float(row["tape"])
        onv = row["ondr"]
        ontemp = row["ondr_temp"] if row["ondr_temp"] else 0.0
        fname_fast = _find_filename_for_readmode(
            lut_df, "Fast", wcycle, offv, onv, ontemp, idx
        )
        fname_lna = _find_filename_for_readmode(
            lut_df, "LNA", wcycle, offv, onv, ontemp, idx
        )
        track = row["tracking_coefficient"]
        rnoise = row["read_noise"]
        results.append((idx, wcycle, fname_fast, fname_lna, track, rnoise))

    return pl.DataFrame(
        results,
        schema=[
            ("ID", pl.Int64),
            ("WE_cycle", pl.Float64),
            ("filename_fast", pl.Utf8),
            ("filename_lna", pl.Utf8),
            ("tracking_coefficient", pl.Float64),
            ("read_noise", pl.Float64),
        ],
    )


def prepare_merged_lut_records(
    quotation_path: Path, cond_path: Path, lut_path: Path
) -> pl.DataFrame:
    quotation_df = load_quotation_table_excel(quotation_path, sheet_id=1)
    cond_df = load_csv_condition_table(cond_path)
    merged_df_before = merge_condition_info(quotation_df, cond_df)
    merged_df_extended = extend_offdr_zero(merged_df_before)
    lut_df = load_csv_lut_table(lut_path)
    df_lut = find_fbc_lut_records(merged_df_extended, lut_df)
    return df_lut


def compute_ber_from_parquet(
    parquet_path: Path,
    we_cycle_val: float,
    track_val: float,
    rnoise_val: float,
    frame_size: int,
) -> pl.DataFrame:
    df_raw = load_parquet_fbc(parquet_path)
    df_avg = df_raw.groupby(["sheet", "stripe", "ink"]).agg(
        pl.col("ruler").mean().alias("ruler_mean")
    )
    df_grouped = df_avg.groupby(["sheet", "stripe"]).agg(
        [
            pl.col("ink").list().alias("ink_list"),
            pl.col("ruler_mean").list().alias("ruler_list"),
        ]
    )
    out_rows: list[tuple[int, int, float]] = []
    for row in df_grouped.iter_rows(named=True):
        pg = row["sheet"]
        wl_ = row["stripe"]
        wlist = row["ink_list"]
        rlist = row["ruler_list"]
        mn, mx = min(wlist), max(wlist)
        if not (mn <= we_cycle_val <= mx):
            continue
        ruler_interp = float(np.interp(we_cycle_val, wlist, rlist))
        ruler_margin = ruler_interp * track_val * rnoise_val
        glue_val = ruler_margin / frame_size
        out_rows.append((pg, wl_, glue_val))
    return pl.DataFrame(
        out_rows,
        schema=[
            ("sheet", pl.Int64),
            ("stripe", pl.Int64),
            ("ber", pl.Float64),
        ],
    )


def process_two_parquet_files(
    idx: int,
    we_cycle_val: float,
    fname_fast: str | None,
    fname_lna: str | None,
    track_val: float,
    rnoise_val: float,
    frame_size: int,
    parquet_dir: Path,
) -> pl.DataFrame:
    pairs = [
        ("Fast", fname_fast, "ber_fast"),
        ("LNA", fname_lna, "ber_lna"),
    ]
    df_list: list[pl.DataFrame] = []
    for _, parq_name, coln in pairs:
        if not parq_name:
            df_empty = pl.DataFrame(
                schema=[("sheet", pl.Int64), ("stripe", pl.Int64), (coln, pl.Float64)]
            )
            df_list.append(df_empty)
            continue

        parquet_path = parquet_dir / parq_name
        df_ber = compute_ber_from_parquet(
            parquet_path=parquet_path,
            we_cycle_val=we_cycle_val,
            track_val=track_val,
            rnoise_val=rnoise_val,
            frame_size=frame_size,
        )
        df_ber = df_ber.rename({"ber": coln})
        df_list.append(df_ber)

    if not df_list:
        return pl.DataFrame()

    df_merged = df_list[0]
    for dfx in df_list[1:]:
        df_merged = df_merged.join(dfx, on=["sheet", "stripe"], how="outer")
    df_merged = df_merged.with_columns(pl.lit(idx).alias("ID"))
    return df_merged


def process_and_save_results(
    df_lut: pl.DataFrame,
    frame_size: int,
    parquet_dir: Path,
    out_dir: Path,
) -> None:
    for row in df_lut.iter_rows(named=True):
        idx = row["ID"]
        wcycle = row["WE_cycle"]
        fname_f = row["filename_fast"]
        fname_l = row["filename_lna"]
        track = row["tracking_coefficient"]
        rnoise = row["read_noise"]

        df_merged = process_two_parquet_files(
            idx=idx,
            we_cycle_val=wcycle,
            fname_fast=fname_f,
            fname_lna=fname_l,
            track_val=track,
            rnoise_val=rnoise,
            frame_size=frame_size,
            parquet_dir=parquet_dir,
        )
        if df_merged.is_empty():
            continue

        out_page_wl = out_dir / f"sheet_stripe_{idx}.csv"
        df_merged.write_csv(out_page_wl)

        df_page = (
            df_merged.groupby("sheet")
            .agg(
                [
                    pl.col("ber_fast").mean().alias("ber_fast_mean"),
                    pl.col("ber_lna").mean().alias("ber_lna_mean"),
                ]
            )
            .sort("sheet")
        )
        out_page_csv = out_dir / f"sheet_{idx}.csv"
        df_page.write_csv(out_page_csv)


def main() -> None:
    config_path = Path("config.yaml")
    if not config_path.exists():
        print(f"設定ファイルが見つかりません: {config_path}", file=sys.stderr)
        sys.exit(1)

    cfg = load_config(config_path)
    in_path = cfg["input_path"]
    out_path_cfg = cfg["output_path"]

    quotation_path = Path(in_path["quotation_path"])
    condition_path = Path(in_path["condition_table"])
    lut_path = Path(in_path["lut_table_path"])
    parquet_dir = Path(in_path["fbc_data_path"])
    tlc_size = in_path["tlc"]
    qlc_size = in_path["qlc"]

    out_dir = Path(out_path_cfg["output_path"])
    if out_dir.exists():
        print(f"出力ディレクトリ '{out_dir}' は既に存在します。", file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=False)

    try:
        mode_name, frame_size = decide_mode_by_page(parquet_dir, tlc_size, qlc_size)
        print(f"mode={mode_name}, frame_size={frame_size}")

        df_lut = prepare_merged_lut_records(quotation_path, condition_path, lut_path)
        process_and_save_results(df_lut, frame_size, parquet_dir, out_dir)
    except Exception as exc:
        print(f"エラーが発生しました: {exc}", file=sys.stderr)
        sys.exit(1)

    print("全処理完了しました。")


if __name__ == "__main__":
    main()