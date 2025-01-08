#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GLUE1(Fast), GLUE2(LNA) を両方計算するサンプル実装:
  - (sheet, stripe, ink) 単位で事前に ruler を平均
  - ink_cycle で線形補間
  - readmode = "Fast"/"LNA" に応じて2種類のファイルを処理
  - 出力先: output_pen/ または output_book/
"""

import argparse
import sys
from pathlib import Path
from typing import Union

import polars as pl
import numpy as np

# ------------------------------------------------------------------------------
# 1. 設定 (JSON を廃止し、コード内で定義)
# ------------------------------------------------------------------------------
CONFIG = {
    "quotation_excel": {
        "path": "quotation_test.xlsx",
        # Excel 側のカラム名をどう定義しているか要確認
        "columns_to_select": ["ID", "force_refresh", "stapler_time", "tape", "ink_cycle"],
        "sheet_name": "Sheet1",
        "dtypes": {
            "ID": pl.Int64,
            "force_refresh": pl.Int64,
            "stapler_time": pl.Utf8,
            "tape": pl.Utf8,
            "ink_cycle": pl.Float64,
        },
    },
    "condition_table_csv": {
        "path": "condition_table_test.csv",
        "dtypes": {
            "condition_temp": pl.Int64,
            "condition_time": pl.Utf8,
            "scotch_temp": pl.Int64,
            "scotch_time": pl.Float64,
        },
    },
    "ruler_pencilcase_table_csv": {
        "path": "ruler_pencilcase_table_test.csv",
        "dtypes": {
            "inkcycle_start": pl.Float64,
            "inkcycle_end": pl.Float64,
            "readmode": pl.Utf8,
            "tape": pl.Float64,
            "stapler": pl.Int64,
            "stapler_temp": pl.Float64,
            "filename": pl.Utf8,
        },
    },
    "tracking_marker": 1.1,
    "read_crinkle": 18,
}


def argument_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ruler glue calculation (GLUE1 & GLUE2).")
    parser.add_argument(
        "--input_data_path",
        type=str,
        default="data/",
        help="データを格納したディレクトリへのパス",
    )
    parser.add_argument(
        "--pen",
        action="store_true",
        help="penモード (旧TLC) で実行する場合に指定",
    )
    parser.add_argument(
        "--book",
        action="store_true",
        help="bookモード (旧QLC) で実行する場合に指定",
    )

    args = parser.parse_args()
    if (not args.pen and not args.book) or (args.pen and args.book):
        print("エラー: --pen または --book のどちらか一方のみ指定してください。")
        parser.print_help()
        sys.exit(1)
    return args


def set_frame_size(pen: bool, book: bool) -> int:
    if pen and not book:
        return 36672
    elif book and not pen:
        return 37952
    return 0


# ------------------------------------------------------------------------------
# Loader
# ------------------------------------------------------------------------------
def load_quotation_table(
    config: dict, input_data_path: Union[str, Path], root_path: Path
) -> pl.DataFrame:
    input_data_dir = root_path / input_data_path
    excel_info = config["quotation_excel"]
    excel_path = input_data_dir / excel_info["path"]

    df = pl.read_excel(
        file=excel_path,
        sheet_name=excel_info.get("sheet_name"),
        read_csv_options={"has_header": True},
        dtypes=excel_info.get("dtypes"),
    )

    use_cols = excel_info.get("columns_to_select", [])
    if use_cols:
        df = df.select(use_cols)

    # rename_map でカラムを変換
    rename_map = {
        "force_refresh": "force_refresh",  # 同じ名前の場合も一旦指定
        "stapler_time": "condition_temp",
        "tape": "condition_time",
        "ink_cycle": "ink_cycle",
    }
    df = df.rename(rename_map)
    return df


def load_condition_table(condition_table_path: Path, config: dict) -> pl.DataFrame:
    dtypes = config["condition_table_csv"].get("dtypes", None)
    df = pl.read_csv(str(condition_table_path), dtypes=dtypes)
    return df


def load_ruler_pencilcase_table(ruler_pencilcase_path: Path, config: dict) -> pl.DataFrame:
    dtypes = config["ruler_pencilcase_table_csv"].get("dtypes", None)
    df = pl.read_csv(str(ruler_pencilcase_path), dtypes=dtypes)

    readmode_vals = set(df["readmode"].unique())
    allowed = {"Fast", "LNA"}
    if not readmode_vals.issubset(allowed):
        invalid_modes = readmode_vals - allowed
        raise RuntimeError(f"サポート外の readmode が含まれています: {invalid_modes}")

    return df


def merge_condition_info(force_df: pl.DataFrame, condition_df: pl.DataFrame) -> pl.DataFrame:
    merged = force_df.join(
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

    # "condition_temp", "condition_time" をdrop
    merged = merged.drop(["condition_temp", "condition_time"])
    return merged


def _find_filename_for_readmode(
    ruler_pencilcase_df: pl.DataFrame,
    readmode: str,
    _id: int,
    ink_cycle: float,
    tape: float,
    force_refresh: float,
    stapler_temp: float,
) -> str:
    matched = ruler_pencilcase_df.filter(
        (pl.col("inkcycle_start") <= ink_cycle)
        & (pl.col("inkcycle_end") >= ink_cycle)
        & (pl.col("tape") == tape)
        & (pl.col("stapler") == force_refresh)
        & (pl.col("stapler_temp") == stapler_temp)
        & (pl.col("readmode") == readmode)
    )

    if matched.shape[0] > 1:
        raise RuntimeError(
            f"重複({readmode})が検出されました ID={_id}, ink_cycle={ink_cycle}"
        )
    elif matched.shape[0] == 0:
        raise RuntimeError(
            f"{readmode} ファイルが見つかりません ID={_id}, ink_cycle={ink_cycle}"
        )

    return matched[0, "filename"]


def find_ruler_pencilcase_records(merged_df: pl.DataFrame, ruler_pencilcase_df: pl.DataFrame) -> pl.DataFrame:
    results = []
    for row in merged_df.iter_rows(named=True):
        _id = row["ID"]
        ink_cycle_val = row["ink_cycle"]
        tape_val = row["tape"]
        force_val = row["force_refresh"]  # 旧 ondr
        stapler_temp_val = row["stapler_temp"]

        filename_fast = _find_filename_for_readmode(
            ruler_pencilcase_df,
            "Fast",
            _id,
            ink_cycle_val,
            tape_val,
            force_val,
            stapler_temp_val,
        )
        filename_lna = _find_filename_for_readmode(
            ruler_pencilcase_df,
            "LNA",
            _id,
            ink_cycle_val,
            tape_val,
            force_val,
            stapler_temp_val,
        )

        results.append((_id, ink_cycle_val, filename_fast, filename_lna))

    output_df = pl.DataFrame(
        results,
        schema=["ID", "ink_cycle", "filename1", "filename2"],
    )
    return output_df


def run_loader_process(
    config: dict, input_data_path: Union[str, Path], root_path: Path
) -> pl.DataFrame:
    force_df = load_quotation_table(config, input_data_path, root_path)

    cond_table_path = Path(input_data_path) / config["condition_table_csv"]["path"]
    condition_df = load_condition_table(cond_table_path, config)
    merged_df = merge_condition_info(force_df, condition_df)

    ruler_pencilcase_path = Path(input_data_path) / config["ruler_pencilcase_table_csv"]["path"]
    ruler_pencilcase_df = load_ruler_pencilcase_table(ruler_pencilcase_path, config)

    final_df = find_ruler_pencilcase_records(merged_df, ruler_pencilcase_df)
    return final_df


# ------------------------------------------------------------------------------
# 5. ruler/glue計算 (線形補間) + (sheet, stripe, ink) 事前平均
# ------------------------------------------------------------------------------
def load_ruler_data(parquet_file: Path) -> pl.DataFrame:
    try:
        return pl.read_parquet(str(parquet_file))
    except FileNotFoundError:
        raise FileNotFoundError(f"Parquetが見つかりません: {parquet_file}")


def process_parquet_for_glue_strict_interp_with_averaging(
    parquet_file: Path,
    tracking_marker: float,
    read_crinkle: int,
    frame_size: int,
    ink_cycle: float,
) -> pl.DataFrame:
    """
    1) ruler_data を読み込み
    2) (sheet, stripe, ink) 単位で ruler を平均 -> 1レコード1点
    3) (sheet, stripe) で groupby -> ink_list, ruler_list
    4) ink_cycle で補間
    5) margin & GLUE 算出
    6) (sheet, stripe, glue) を返す
    """
    df_raw = load_ruler_data(parquet_file)

    # (sheet, stripe, ink) で ruler 平均
    df_avg = (
        df_raw.groupby(["sheet", "stripe", "ink"])
        .agg(pl.col("ruler").mean().alias("ruler_mean"))
    )

    # (sheet, stripe) 単位で list化
    df_grouped = (
        df_avg.groupby(["sheet", "stripe"])
        .agg([
            pl.col("ink").list().alias("ink_list"),
            pl.col("ruler_mean").list().alias("ruler_list"),
        ])
    )

    # 線形補間
    results = []
    for row in df_grouped.iter_rows(named=True):
        sheet_val = row["sheet"]
        stripe_val = row["stripe"]
        ink_vals = row["ink_list"]
        ruler_vals = row["ruler_list"]

        min_ink, max_ink = min(ink_vals), max(ink_vals)
        if not (min_ink <= ink_cycle <= max_ink):
            raise RuntimeError(
                f"ink_cycle={ink_cycle} が補間範囲({min_ink}～{max_ink})外です。"
            )

        # 補間
        ruler_interp = float(np.interp(ink_cycle, ink_vals, ruler_vals))

        # margin & GLUE
        ruler_with_margin = ruler_interp * tracking_marker * read_crinkle
        glue_val = ruler_with_margin / frame_size

        results.append({
            "sheet": sheet_val,
            "stripe": stripe_val,
            "glue": glue_val,
        })

    return pl.DataFrame(results)


def process_by_sheet_stripe(
    filename_fast: str,
    filename_lna: str,
    input_data_path: Union[str, Path],
    config: dict,
    frame_size: int,
    out_dir: Path,
    _id: int,
    ink_cycle: float,
) -> None:
    tracking_marker = config["tracking_marker"]
    read_crinkle = config["read_crinkle"]

    id_dir = out_dir / f"{_id}"
    id_dir.mkdir(parents=True, exist_ok=True)

    readmodes = [("Fast", filename_fast, "glue_fast"), ("LNA", filename_lna, "glue_lna")]
    df_list = []

    for mode_name, fname, glue_colname in readmodes:
        parquet_path = Path(input_data_path) / "ruler_data" / fname

        df_glue = process_parquet_for_glue_strict_interp_with_averaging(
            parquet_file=parquet_path,
            tracking_marker=tracking_marker,
            read_crinkle=read_crinkle,
            frame_size=frame_size,
            ink_cycle=ink_cycle,
        )

        df_glue = df_glue.rename({"glue": glue_colname})
        df_list.append(df_glue)

    df_merged = df_list[0].join(df_list[1], on=["sheet", "stripe"], how="inner")

    out_sheet_stripe = id_dir / "sheet_stripe.csv"
    df_merged.write_csv(out_sheet_stripe)

    df_sheet = (
        df_merged.groupby("sheet")
        .agg([
            pl.col("glue_fast").mean().alias("glue_fast_mean"),
            pl.col("glue_lna").mean().alias("glue_lna_mean"),
        ])
        .sort("sheet")
    )
    out_sheet = id_dir / "sheet.csv"
    df_sheet.write_csv(out_sheet)


# ------------------------------------------------------------------------------
# 6. メイン
# ------------------------------------------------------------------------------
def main() -> None:
    args = argument_parse()
    frame_size = set_frame_size(pen=args.pen, book=args.book)

    if args.pen:
        out_dir = Path("output_pen")
    else:
        out_dir = Path("output_book")
    out_dir.mkdir(parents=True, exist_ok=True)

    root_path = Path(".")
    df_id_ink_cycle = run_loader_process(CONFIG, args.input_data_path, root_path)

    print("\n--- Loader結果 (ID, ink_cycle, filename1, filename2) ---")
    print(df_id_ink_cycle)

    for row in df_id_ink_cycle.iter_rows(named=True):
        _id = row["ID"]
        ink_cycle_val = row["ink_cycle"]
        fname_fast = row["filename1"]
        fname_lna = row["filename2"]

        process_by_sheet_stripe(
            filename_fast=fname_fast,
            filename_lna=fname_lna,
            input_data_path=args.input_data_path,
            config=CONFIG,
            frame_size=frame_size,
            out_dir=out_dir,
            _id=_id,
            ink_cycle=ink_cycle_val,
        )

    print(f"\n--- 全処理完了 (出力先: {out_dir}) ---")


if __name__ == "__main__":
    main()