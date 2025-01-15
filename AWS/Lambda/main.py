#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ruler glue calculation (glue1 & glue2) with class-based refactoring.
@dataclass Config
class Loader
class Calculator
def main
"""

import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Union, Optional

import yaml
import polars as pl
import numpy as np


@dataclass
class Config:
    quotation_path: str
    condition_table: str
    lut_table_path: str
    ruler_data_path: str
    pen: int
    book: int
    output_path: str

    @staticmethod
    def from_yaml(yaml_path: Union[str, Path]) -> "Config":
        config_file = Path(yaml_path)
        if not config_file.exists():
            raise FileNotFoundError(f"configファイルが見つかりません: {yaml_path}")
        with open(config_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        in_path = data["input_path"]
        out_path = data["output_path"]

        return Config(
            quotation_path=in_path["quotation_path"],
            condition_table=in_path["condition_table"],
            lut_table_path=in_path["lut_table_path"],
            ruler_data_path=in_path["fbc_data_path"],
            pen=in_path["tlc"],
            book=in_path["qlc"],
            output_path=out_path["output_path"],
        )


class Loader:
    def __init__(self, config: Config):
        self.config = config

    def _check_exists(self, path: Path, desc: str = "") -> None:
        if not path.exists():
            raise FileNotFoundError(f"{desc}ファイルが見つかりません: {path}")

    def read_excel_with_sheet_id(self, file: Path, sheet_id: int) -> pl.DataFrame:
        self._check_exists(file, desc="Excel")
        all_sheets = pl.read_excel(file=str(file), sheet_name=None)
        sheet_names = list(all_sheets.keys())
        if len(sheet_names) == 0:
            raise ValueError(f"Excelファイルにシートが存在しません: {file}")
        if len(sheet_names) > 1:
            raise ValueError(f"Excelファイルにシートが2つ以上あります: {sheet_names}")
        if sheet_id != 1:
            raise ValueError(f"sheet_id={sheet_id} ですが、シートは1枚しかありません。")
        first_sheet = sheet_names[0]
        return all_sheets[first_sheet]

    def load_quotation_excel(self, sheet_id: int = 1) -> pl.DataFrame:
        xlsx_path = Path(self.config.quotation_path)
        df = self.read_excel_with_sheet_id(xlsx_path, sheet_id=sheet_id)
        expected_cols = [
            "ID",
            "force_refresh",
            "stapler_time",
            "tape",
            "ink_cycle",
            "tracking_marker",
            "read_crinkle",
        ]
        for col in expected_cols:
            if col not in df.columns:
                raise ValueError(f"Excel(quotation): 必須カラム '{col}' がありません。columns={df.columns}")
        df = df.select(expected_cols)
        rename_map = {
            "force_refresh": "stapler",
            "stapler_time": "condition_temp",
            "tape": "condition_time",
            "ink_cycle": "ink_cycle",
        }
        df = df.rename(rename_map)
        return df

    def load_condition_csv(self) -> pl.DataFrame:
        csv_path = Path(self.config.condition_table)
        self._check_exists(csv_path, desc="condition_table CSV")
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
        for col in expected_cols:
            if col not in df.columns:
                raise ValueError(f"CSV(condition_table): '{col}' 不足 columns={df.columns}")
        return df

    def load_lut_csv(self) -> pl.DataFrame:
        lut_path = Path(self.config.lut_table_path)
        self._check_exists(lut_path, desc="LUT CSV")
        df = pl.read_csv(
            str(lut_path),
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
        needed = ["inkcycle_start", "inkcycle_end", "readmode", "tape", "stapler", "stapler_temp", "filename"]
        for col in needed:
            if col not in df.columns:
                raise ValueError(f"LUT CSV: カラム '{col}' 不足 columns={df.columns}")
        readmodes = set(df["readmode"].unique())
        allowed = {"Fast", "LNA"}
        if not readmodes.issubset(allowed):
            invalid = readmodes - allowed
            raise RuntimeError(f"未知のreadmode: {invalid}")
        return df

    def load_fbc_parquet(self, parquet_file: Path) -> pl.DataFrame:
        self._check_exists(parquet_file, desc="Parquet")
        df = pl.read_parquet(str(parquet_file))
        required_cols = ["sheet", "stripe", "ink", "ruler"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Parquet({parquet_file}): '{col}' 不足 columns={df.columns}")
        return df


class Calculator:
    def __init__(self, config: Config, loader: Loader):
        self.config = config
        self.loader = loader

    def decide_mode_by_sheet(self) -> tuple[str, int]:
        data_dir = Path(self.config.ruler_data_path)
        parquet_files = list(data_dir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"ruler_data にParquetがありません: {data_dir}")
        df_sample = self.loader.load_fbc_parquet(parquet_files[0])
        sheets = df_sample["sheet"].unique().to_list()
        uniq_count = len(sheets)
        if uniq_count == 3:
            return "pen", self.config.pen
        elif uniq_count == 4:
            return "book", self.config.book
        else:
            raise ValueError(f"sheetユニーク数={uniq_count} 想定外")

    def merge_condition_info(self, df_q: pl.DataFrame, df_c: pl.DataFrame) -> pl.DataFrame:
        merged = df_q.join(
            df_c,
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

    def extend_tape_zero(self, df_merged: pl.DataFrame) -> pl.DataFrame:
        df_zero = df_merged.with_columns(pl.lit(0.0).alias("tape"))
        return pl.concat([df_merged, df_zero], how="vertical")

    def _find_filename_for_readmode(
        self,
        lut_df: pl.DataFrame,
        readmode: str,
        ink_cycle_val: float,
        tape_val: float,
        stapler_val: int,
        stapler_temp_val: float,
        idx: int,
    ) -> Optional[str]:
        matched = lut_df.filter(
            (pl.col("inkcycle_start") <= ink_cycle_val)
            & (pl.col("inkcycle_end") >= ink_cycle_val)
            & (pl.col("tape") == tape_val)
            & (pl.col("stapler") == stapler_val)
            & (pl.col("stapler_temp") == stapler_temp_val)
            & (pl.col("readmode") == readmode)
        )
        if matched.shape[0] > 1:
            raise RuntimeError(f"重複ファイル: ID={idx}, readmode={readmode}, ink_cycle={ink_cycle_val}")
        if matched.is_empty():
            return None
        return matched[0, "filename"]

    def find_ruler_pencilcase_records(self, extended_df: pl.DataFrame, lut_df: pl.DataFrame) -> pl.DataFrame:
        results = []
        for row in extended_df.iter_rows(named=True):
            idx = row["ID"]
            wcycle = row["ink_cycle"]
            offv = float(row["tape"])
            onv = row["stapler"]
            ontemp = float(row["stapler_temp"]) if row["stapler_temp"] else 0.0
            fname_fast = self._find_filename_for_readmode(lut_df, "Fast", wcycle, offv, onv, ontemp, idx)
            fname_lna = self._find_filename_for_readmode(lut_df, "LNA", wcycle, offv, onv, ontemp, idx)
            track = row["tracking_marker"]
            rnoise = row["read_crinkle"]
            results.append((idx, wcycle, fname_fast, fname_lna, track, rnoise))

        df_out = pl.DataFrame(
            results,
            schema=[
                ("ID", pl.Int64),
                ("ink_cycle", pl.Float64),
                ("filename_fast", pl.Utf8),
                ("filename_lna", pl.Utf8),
                ("tracking_marker", pl.Float64),
                ("read_crinkle", pl.Float64),
            ],
        )
        return df_out

    def prepare_lut_df(self) -> pl.DataFrame:
        df_q = self.loader.load_quotation_excel(sheet_id=1)
        df_c = self.loader.load_condition_csv()
        merged_df_before = self.merge_condition_info(df_q, df_c)
        merged_df_extended = self.extend_tape_zero(merged_df_before)
        lut_df = self.loader.load_lut_csv()
        df_out = self.find_ruler_pencilcase_records(merged_df_extended, lut_df)
        return df_out

    def compute_glue_from_parquet(
        self,
        parquet_path: Path,
        ink_cycle_val: float,
        track_val: float,
        rnoise_val: float,
        frame_size: int,
    ) -> pl.DataFrame:
        df_raw = self.loader.load_fbc_parquet(parquet_path)
        df_avg = df_raw.groupby(["sheet", "stripe", "ink"]).agg(
            pl.col("ruler").mean().alias("ruler_mean")
        )
        df_grouped = df_avg.groupby(["sheet", "stripe"]).agg(
            [
                pl.col("ink").list().alias("ink_list"),
                pl.col("ruler_mean").list().alias("ruler_list"),
            ]
        )
        out_rows = []
        for row in df_grouped.iter_rows(named=True):
            pg = row["sheet"]
            wl_ = row["stripe"]
            wlist = row["ink_list"]
            rlist = row["ruler_list"]
            mn, mx = min(wlist), max(wlist)
            if not (mn <= ink_cycle_val <= mx):
                continue
            ruler_interp = float(np.interp(ink_cycle_val, wlist, rlist))
            ruler_margin = ruler_interp * track_val * rnoise_val
            glue_val = ruler_margin / frame_size
            out_rows.append((pg, wl_, glue_val))
        df_out = pl.DataFrame(
            out_rows,
            schema=[
                ("sheet", pl.Int64),
                ("stripe", pl.Int64),
                ("glue", pl.Float64),
            ],
        )
        return df_out

    def process_two_parquet_files(
        self,
        idx: int,
        ink_cycle_val: float,
        fname_fast: Optional[str],
        fname_lna: Optional[str],
        track_val: float,
        rnoise_val: float,
        frame_size: int,
    ) -> pl.DataFrame:
        data_dir = Path(self.config.ruler_data_path)
        pairs = [
            ("Fast", fname_fast, "glue_fast"),
            ("LNA", fname_lna, "glue_lna"),
        ]
        df_list = []
        for _, parq_name, coln in pairs:
            if not parq_name:
                df_empty = pl.DataFrame(schema=[("sheet", pl.Int64), ("stripe", pl.Int64), (coln, pl.Float64)])
                df_list.append(df_empty)
                continue
            parquet_path = data_dir / parq_name
            df_glue = self.compute_glue_from_parquet(
                parquet_path=parquet_path,
                ink_cycle_val=ink_cycle_val,
                track_val=track_val,
                rnoise_val=rnoise_val,
                frame_size=frame_size,
            )
            df_glue = df_glue.rename({"glue": coln})
            df_list.append(df_glue)

        if not df_list:
            return pl.DataFrame()

        df_merged = df_list[0]
        for dfx in df_list[1:]:
            df_merged = df_merged.join(dfx, on=["sheet", "stripe"], how="outer")
        df_merged = df_merged.with_columns(pl.lit(idx).alias("ID"))
        return df_merged

    def process_and_save_results(
        self, df_lut: pl.DataFrame, frame_size: int, out_dir: Path
    ) -> None:
        for row in df_lut.iter_rows(named=True):
            idx = row["ID"]
            wcycle = row["ink_cycle"]
            fname_f = row["filename_fast"]
            fname_l = row["filename_lna"]
            track = row["tracking_marker"]
            rnoise = row["read_crinkle"]

            df_merged = self.process_two_parquet_files(
                idx=idx,
                ink_cycle_val=wcycle,
                fname_fast=fname_f,
                fname_lna=fname_l,
                track_val=track,
                rnoise_val=rnoise,
                frame_size=frame_size,
            )
            if df_merged.is_empty():
                continue

            out_page_wl = out_dir / f"sheet_stripe_{idx}.csv"
            df_merged.write_csv(out_page_wl)

            df_page = (
                df_merged.groupby("sheet")
                .agg([
                    pl.col("glue_fast").mean().alias("glue_fast_mean"),
                    pl.col("glue_lna").mean().alias("glue_lna_mean"),
                ])
                .sort("sheet")
            )
            out_page_csv = out_dir / f"sheet_{idx}.csv"
            df_page.write_csv(out_page_csv)


def main() -> None:
    try:
        config = Config.from_yaml("config.yaml")
    except Exception as e:
        print(f"設定ファイル読み込み時にエラー: {e}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(config.output_path)
    if out_dir.exists():
        print(f"出力先ディレクトリ '{out_dir}' は既に存在します。", file=sys.stderr)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=False)

    loader = Loader(config)
    calculator = Calculator(config, loader)

    try:
        mode_name, frame_size = calculator.decide_mode_by_sheet()
        print(f"mode={mode_name}, frame_size={frame_size}")

        df_lut = calculator.prepare_lut_df()
        calculator.process_and_save_results(df_lut, frame_size, out_dir)

    except Exception as e:
        print(f"エラーが発生しました: {e}", file=sys.stderr)
        sys.exit(1)

    print("全処理完了しました。")


if __name__ == "__main__":
    main()