#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ruler GLUE calculation (GLUE1 & GLUE2), supporting:
  - mode: pen or book (default=pen) via argparse choices
  - set_frame_size() uses match/case
  - Excel (quotation_test.xlsx) with ID-specific tracking_marker, read_crinkle
  - condition_table_test.csv for tape/stapler_temp
  - merged_df extended with tape=0.0
  - ruler_pencilcase_table_test.csv -> find filename for Fast/LNA
  - ruler_data/*.parquet -> (sheet, stripe, ink)->ruler mean -> interpolation -> glue
  - output_{pen or book}/<ID>/sheet_stripe.csv & sheet.csv
"""

import argparse
import sys
from pathlib import Path
from typing import Union

import polars as pl
import numpy as np


# ------------------------------------------------------------------------------
# 1. argparse
# ------------------------------------------------------------------------------
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ruler GLUE calculation (GLUE1 & GLUE2).")

    # --mode の choices: ["pen","book"]、デフォルト: "pen"
    parser.add_argument(
        "--mode",
        choices=["pen", "book"],
        default="pen",
        help="動作モードを選択 (pen または book)。デフォルトは 'pen'",
    )

    parser.add_argument(
        "--input_data_path",
        type=str,
        default="data/",
        help="quotation_test.xlsxなどが入ったディレクトリのパス",
    )
    args = parser.parse_args()
    return args


def set_frame_size(mode: str) -> int:
    """
    Python 3.10 以降の match/case を使用:
      mode="pen" => 36672
      mode="book" => 37952
      それ以外 => 0
    """
    match mode:
        case "pen":
            return 36672
        case "book":
            return 37952
        case _:
            return 0


# ------------------------------------------------------------------------------
# 2. Excel (quotation) 読み込み
# ------------------------------------------------------------------------------
def load_quotation_table(input_data_path: Path) -> pl.DataFrame:
    """
    Excel (quotation_test.xlsx) 読み込み
    """
    excel_path = input_data_path / "quotation_test.xlsx"
    df = pl.read_excel(
        file=excel_path,
        sheet_name="Sheet1",
        read_csv_options={"has_header": True},
        dtypes={
            "ID": pl.Int64,
            "force_refresh": pl.Int64,
            "stapler_time": pl.Utf8,
            "tape": pl.Utf8,
            "ink_cycle": pl.Float64,
            "tracking_marker": pl.Float64,
            "read_crinkle": pl.Float64,
        },
    )

    # 必要な列だけ抽出
    df = df.select(
        [
            "ID",
            "force_refresh",
            "stapler_time",
            "tape",
            "ink_cycle",
            "tracking_marker",
            "read_crinkle",
        ]
    )

    # カラム名の変換
    rename_map = {
        "force_refresh": "stapler",
        "stapler_time": "condition_temp",
        "tape": "condition_time",
        "ink_cycle": "ink_cycle",
    }
    df = df.rename(rename_map)
    return df


# ------------------------------------------------------------------------------
# 3. condition_table.csv 読み込み
# ------------------------------------------------------------------------------
def load_condition_table(input_data_path: Path) -> pl.DataFrame:
    """
    condition_table_test.csv 読み込み
    """
    cond_file = input_data_path / "condition_table_test.csv"
    df = pl.read_csv(
        str(cond_file),
        dtypes={
            "condition_temp": pl.Int64,
            "condition_time": pl.Utf8,
            "scotch_temp": pl.Int64,
            "scotch_time": pl.Float64,
        },
    )
    return df


def merge_condition_info(
    quotation_df: pl.DataFrame, condition_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Left join => rename: scotch_temp->tape, scotch_time->stapler_temp
    Drop original condition_temp/time
    """
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


def extend_tape_zero(merged_df: pl.DataFrame) -> pl.DataFrame:
    """
    merged_df の tape を 0.0 に置き換えたレコードを追加 => (tape=実値 or 0)
    """
    df_zero = merged_df.with_columns(pl.lit(0.0).alias("tape"))
    df_extended = pl.concat([merged_df, df_zero], how="vertical")
    return df_extended


# ------------------------------------------------------------------------------
# 4. LUT (ruler_pencilcase_table.csv) 読み込み & 照合
# ------------------------------------------------------------------------------
def load_ruler_pencilcase_table(input_data_path: Path) -> pl.DataFrame:
    """
    LUT (ruler_pencilcase_table_test.csv) 読み込み & 照合用 DataFrame
    """
    lut_file = input_data_path / "ruler_pencilcase_table_test.csv"
    df = pl.read_csv(
        str(lut_file),
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
    readmodes = set(df["readmode"].unique())
    allowed = {"Fast", "LNA"}
    if not readmodes.issubset(allowed):
        invalid = readmodes - allowed
        raise RuntimeError(f"ruler_pencilcase_tableに未知のreadmodeあり: {invalid}")
    return df


def _find_filename_for_readmode(
    lut_df: pl.DataFrame,
    readmode: str,
    ink_cycle_val: float,
    tape_val: float,
    stapler_val: float,
    stapler_temp_val: float,
    _id: int,
) -> str:
    """
    LUT データフレームから該当するファイル名を検索
    """
    matched = lut_df.filter(
        (pl.col("inkcycle_start") <= ink_cycle_val)
        & (pl.col("inkcycle_end") >= ink_cycle_val)
        & (pl.col("tape") == tape_val)
        & (pl.col("stapler") == stapler_val)
        & (pl.col("stapler_temp") == stapler_temp_val)
        & (pl.col("readmode") == readmode)
    )
    if matched.shape[0] > 1:
        raise RuntimeError(
            f"重複ファイル: ID={_id}, readmode={readmode}, ink_cycle={ink_cycle_val}"
        )
    if matched.shape[0] == 0:
        return None
    return matched[0, "filename"]


def find_ruler_pencilcase_records(
    extended_df: pl.DataFrame, lut_df: pl.DataFrame
) -> pl.DataFrame:
    """
    extended_df: (tape=orig or 0), (tracking_marker, read_crinkle)
    LUT => filename_fast, filename_lna
    """
    results = []
    for row in extended_df.iter_rows(named=True):
        _id = row["ID"]
        wcycle = row["ink_cycle"]
        offv = float(row["tape"])
        onv = row["stapler"]
        ontemp = row["stapler_temp"] if row["stapler_temp"] else 0.0

        fname_fast = _find_filename_for_readmode(
            lut_df, "Fast", wcycle, offv, onv, ontemp, _id
        )
        fname_lna = _find_filename_for_readmode(
            lut_df, "LNA", wcycle, offv, onv, ontemp, _id
        )
        track = row["tracking_marker"]
        rnoise = row["read_crinkle"]
        results.append((_id, wcycle, fname_fast, fname_lna, track, rnoise))

    df_out = pl.DataFrame(
        results,
        schema=[
            "ID",
            "ink_cycle",
            "filename_fast",
            "filename_lna",
            "tracking_marker",
            "read_crinkle",
        ],
    )
    return df_out


# ------------------------------------------------------------------------------
# 5. Parquet 読み込み & (sheet, stripe, ink) 平均 → 補間
# ------------------------------------------------------------------------------
def load_ruler_data(parquet_path: Path) -> pl.DataFrame:
    """
    Parquet ファイルを読み込み
    """
    try:
        return pl.read_parquet(str(parquet_path))
    except FileNotFoundError:
        raise FileNotFoundError(f"Parquetファイルが見つかりません: {parquet_path}")


def compute_glue_from_parquet(
    parquet_path: Path,
    ink_cycle_val: float,
    track_val: float,
    rnoise_val: float,
    frame_size: int,
) -> pl.DataFrame:
    """
    (sheet, stripe, ink)->ruler mean => (sheet, stripe)->(ink_list,ruler_list) => np.interp()
    => margin => glue
    """
    df_raw = load_ruler_data(parquet_path)
    df_avg = df_raw.groupby(["sheet", "stripe", "ink"]).agg(
        pl.col("ruler").mean().alias("ruler_mean")
    )
    df_grouped = df_avg.groupby(["sheet", "stripe"]).agg(
        [
            pl.col("ink").list().alias("ink_list"),
            pl.col("ruler_mean").list().alias("ruler_list"),
        ]
    )

    results = []
    for row in df_grouped.iter_rows(named=True):
        pg = row["sheet"]
        wl_ = row["stripe"]
        wlist = row["ink_list"]
        ruler_list = row["ruler_list"]
        mn, mx = min(wlist), max(wlist)
        if not (mn <= ink_cycle_val <= mx):
            continue
        ruler_interp = float(np.interp(ink_cycle_val, wlist, ruler_list))
        ruler_margin = ruler_interp * track_val * rnoise_val
        glue_val = ruler_margin / frame_size
        results.append((pg, wl_, glue_val))

    return pl.DataFrame(results, schema=["sheet", "stripe", "glue"])


def process_two_parquet_files(
    _id: int,
    ink_cycle_val: float,
    fname_fast: str,
    fname_lna: str,
    track_val: float,
    rnoise_val: float,
    frame_size: int,
    input_data_path: Path,
) -> pl.DataFrame:
    """
    2種類の Parquet を処理し、それぞれの glue を算出した後に join して返す
    """
    pairs = [
        ("Fast", fname_fast, "glue_fast"),
        ("LNA", fname_lna, "glue_lna"),
    ]
    df_list = []
    for _, parq_name, coln in pairs:
        if not parq_name:
            df_empty = pl.DataFrame(schema=["sheet", "stripe", coln])
            df_list.append(df_empty)
            continue

        parquet_path = input_data_path / "ruler_data" / parq_name
        df_glue = compute_glue_from_parquet(
            parquet_path=parquet_path,
            ink_cycle_val=ink_cycle_val,
            track_val=track_val,
            rnoise_val=rnoise_val,
            frame_size=frame_size,
        )
        df_glue = df_glue.rename({"glue": coln})
        df_list.append(df_glue)

    df_merged = df_list[0].join(df_list[1], on=["sheet", "stripe"], how="outer")
    df_merged = df_merged.with_columns(pl.lit(_id).alias("ID"))
    return df_merged


# ------------------------------------------------------------------------------
# 6. データ読込〜LUT照合までの準備をまとめた関数
# ------------------------------------------------------------------------------
def prepare_merged_pencilcase_records(
    input_data_path: Path,
    frame_size: int,
) -> pl.DataFrame:
    """
    全体のデータをロードしてマージし、LUT ファイルを照合してファイル名を取得するまでを行う
    """
    if frame_size == 0:
        raise ValueError("frame_size が 0 です。mode が不正な可能性があります。")

    # (1) Excel 読み込み
    quotation_df = load_quotation_table(input_data_path)

    # (2) condition_table.csv 読み込み
    cond_df = load_condition_table(input_data_path)

    # (3) merge => merged_df
    merged_df_before = merge_condition_info(quotation_df, cond_df)

    # (4) tape=0 レコード追加
    merged_df_extended = extend_tape_zero(merged_df_before)

    # (5) ruler_pencilcase_table_test.csv 読み込み
    lut_df = load_ruler_pencilcase_table(input_data_path)

    # (6) LUT 照合 => (ID, ink_cycle, filename_fast, filename_lna, track, read_crinkle)
    df_lut = find_ruler_pencilcase_records(merged_df_extended, lut_df)
    return df_lut


# ------------------------------------------------------------------------------
# 7. ID単位の処理を行い、結果をCSVに書き出す関数
# ------------------------------------------------------------------------------
def process_and_save_results(
    df_lut: pl.DataFrame,
    frame_size: int,
    input_data_path: Path,
    out_dir: Path,
) -> None:
    """
    IDごとのデータを Parquet ファイルから読み込み、GLUE算出し、CSVへ書き出す
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    for row in df_lut.iter_rows(named=True):
        _id = row["ID"]
        wcycle = row["ink_cycle"]
        fname_f = row["filename_fast"]
        fname_l = row["filename_lna"]
        track = row["tracking_marker"]
        rnoise = row["read_crinkle"]

        df_merged = process_two_parquet_files(
            _id=_id,
            ink_cycle_val=wcycle,
            fname_fast=fname_f,
            fname_lna=fname_l,
            track_val=track,
            rnoise_val=rnoise,
            frame_size=frame_size,
            input_data_path=input_data_path,
        )
        if df_merged.shape[0] == 0:
            continue

        # (sheet, stripe) CSV
        id_dir = out_dir / f"{_id}"
        id_dir.mkdir(parents=True, exist_ok=True)

        out_sheet_stripe = id_dir / "sheet_stripe.csv"
        df_merged.write_csv(out_sheet_stripe)

        # (sheet)単位に平均
        df_sheet = (
            df_merged.groupby("sheet")
            .agg(
                [
                    pl.col("glue_fast").mean().alias("glue_fast_mean"),
                    pl.col("glue_lna").mean().alias("glue_lna_mean"),
                ]
            )
            .sort("sheet")
        )
        out_sheet_csv = id_dir / "sheet.csv"
        df_sheet.write_csv(out_sheet_csv)


# ------------------------------------------------------------------------------
# 8. メイン
# ------------------------------------------------------------------------------
def main():
    """
    メイン関数
    """
    args = parse_arguments()

    # mode=pen or book に応じた frame_size の取得
    frame_size = set_frame_size(args.mode)
    if frame_size == 0:
        print(f"不明なmode: {args.mode}")
        sys.exit(1)

    # 入力データパス
    input_data_path = Path(args.input_data_path)

    # 事前準備 (Excel, condition_table, LUT読み込み＆拡張など)
    df_lut = prepare_merged_pencilcase_records(input_data_path, frame_size)

    # 出力先のディレクトリ (mode 別)
    if args.mode == "pen":
        out_dir = Path("output_pen")
    else:
        out_dir = Path("output_book")

    # IDごとの処理を実行し、結果を書き出す
    process_and_save_results(df_lut, frame_size, input_data_path, out_dir)

    print("全処理完了しました。")


if __name__ == "__main__":
    main()