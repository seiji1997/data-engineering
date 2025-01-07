#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ruler に基づく GLUE 計算を行うメインスクリプト（線形補間の要否も含む）。
今回のポイント:
  - argparse で --pen, --book を設定し、Loader に反映
  - Loader クラスで quotation.xlsx / condition_table.csv / ruler_pencilcase_table.csv を読み込み、ID/ink_cycle/filename を取得
  - 重複レコード検出時にエラーを吐いて処理を止める
  - Calculator クラス:
      1) 従来の process_by_id() は "ID & ink_cycle" 単位の処理
      2) process_by_page_wl() により "page & wl" 単位で平均→補間→マージン→GLUE
"""

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

import polars as pl
import numpy as np


def argument_parse() -> argparse.Namespace:
    """
    コマンドライン引数をパースする関数。
    """
    parser = argparse.ArgumentParser(description="ruler に基づく GLUE 計算スクリプト。")
    parser.add_argument(
        "--input_data_path",
        type=str,
        default="data/",
        help="入力データを格納したディレクトリへのパス。",
    )
    parser.add_argument(
        "--config_json",
        type=str,
        default="config.json",
        help="列選択などの設定が書かれた JSON ファイルへのパス。",
    )
    # 追加: pen/book を選択するフラグ
    parser.add_argument(
        "--pen",
        action="store_true",
        help="pen モードで実行する場合に指定",
    )
    parser.add_argument(
        "--book",
        action="store_true",
        help="book モードで実行する場合に指定",
    )

    args = parser.parse_args()
    return args


@dataclass
class Loader:
    """
    入力データのロードや前処理を行うクラス。

    主な処理:
        1. quotation.xlsx の読み込み (必要な列だけ選択 & カラム名リネーム)
        2. condition_table.csv との突き合わせ (leftjoin) によりカラム追加・リネーム
        3. ruler_pencilcase_table.csv から該当レコードを検索し、[ID, ink_cycle, filename] を返す

    Attributes:
        pen (bool): pen モードを使用するか。
        book (bool): book モードを使用するか。
        tracking_marker (float): ruler に追加入力を行う際の係数。
        read_crinkle (int): 読み込み時のノイズ量。
        rute (Path): データの入出力で使用するルートパス。
        frame_size (int): フレームサイズ（pen または book）。
    """

    pen: bool = True
    book: bool = False
    tracking_marker: float = 1.1
    read_crinkle: int = 18
    rute: Path = field(default_factory=lambda: Path("./"))
    frame_size: int = field(init=False)

    def __post_init__(self) -> None:
        """
        pen, book のいずれを使用するかに応じて frame_size を設定する。
        """
        if self.pen and not self.book:
            # pen モード
            self.frame_size = 36672
        elif self.book and not self.pen:
            # book モード
            self.frame_size = 37952
        else:
            # どちらも True もしくはどちらも False は想定外 -> 0 としておくか、エラーにしてもよい
            self.frame_size = 0

    def load_quotation_table(
        self, config: dict, input_data_path: Union[str, Path]
    ) -> pl.DataFrame:
        """
        quotation.xlsx を読み込み、JSON 設定で指定したカラムのみ取得しつつ、
        指示のあったカラム名へのリネームを行う。
        """
        input_data_dir = self.rute / input_data_path
        excel_info = config["quotation_excel"]  # JSON 側で "quotation_excel" に設定されている想定
        excel_path = input_data_dir / excel_info["path"]

        use_columns = excel_info.get("columns_to_select", [])

        # polars の read_excel
        df = pl.read_excel(
            file=excel_path,
            sheet_name=excel_info.get("sheet_name", None),
            read_csv_options={"has_header": True},
        )

        if use_columns:
            # ondrtime (列名) を stapler_time に
            df = df.select(use_columns)

        rename_map = {
            "強制リフレッシュ": "stapler",
            "ondrtime": "stapler_time",   # 置換
            "offdr": "tape",
            "WE cycle": "ink_cycle",
        }
        df = df.rename(rename_map)
        return df

    def merge_condition_info(
        self, quotation_df: pl.DataFrame, condition_table_path: Path
    ) -> pl.DataFrame:
        """
        quotation_df と condition_table.csv を leftjoin する。
        """
        condition_df = pl.read_csv(str(condition_table_path))

        merged = quotation_df.join(
            condition_df,
            how="left",
            left_on=["stapler_time", "tape"],
            right_on=["condition_temp", "condition_time"],
        )

        # measurement_temp -> scotch_temp, measurement_time -> scotch_time
        merged = merged.rename({"measurement_temp": "scotch_temp", "measurement_time": "scotch_time"})

        # 使い終わったカラムを drop
        merged = merged.drop(["condition_temp", "condition_time"])
        return merged

    def find_ruler_pencilcase_records(
        self, merged_df: pl.DataFrame, ruler_pencilcase_table_path: Path
    ) -> pl.DataFrame:
        """
        merged_df と ruler_pencilcase_table.csv を突き合わせて、該当する filename を探し、
        [ID, ink_cycle, filename] を返す。

        重複レコードが見つかった場合は RuntimeError を吐いて処理を止める。
        """
        ruler_pencilcase_df = pl.read_csv(str(ruler_pencilcase_table_path))

        results = []
        for row in merged_df.iter_rows(named=True):
            _id = row["ID"]
            ink_cycle_val = row["ink_cycle"]
            tape_val = row["tape"]
            stapler_val = row["stapler"]
            stapler_temp_val = row["ondr_temp"] if "ondr_temp" in row else None
            # もし "ondr_temp" という列を "stapler_temp" にしていたなら row["stapler_temp"] にする
            if "stapler_temp" in row:
                stapler_temp_val = row["stapler_temp"]

            matched = ruler_pencilcase_df.filter(
                (pl.col("inkcycle_start") <= ink_cycle_val)
                & (pl.col("inkcycle_end") >= ink_cycle_val)
                & (pl.col("tape") == tape_val)
                & (pl.col("stapler") == stapler_val)
                & (pl.col("stapler_temp") == stapler_temp_val)
            )

            # 重複チェック: matched が2件以上あるならエラー
            if matched.shape[0] > 1:
                raise RuntimeError(
                    f"重複レコードが検出されました。ID={_id}, ink_cycle={ink_cycle_val} に複数の filename が存在します。"
                )

            if matched.shape[0] == 1:
                filename = matched[0, "filename"]
                results.append((_id, ink_cycle_val, filename))
            else:
                # 見つからない場合は None でも可
                results.append((_id, ink_cycle_val, None))

        output_df = pl.DataFrame(results, schema=["ID", "ink_cycle", "filename"])

        return output_df

    def run_loader_process(
        self, config: dict, input_data_path: Union[str, Path]
    ) -> pl.DataFrame:
        """
        Loader 内で一連の処理をまとめて実行:
            1. quotation.xlsx 読込
            2. condition_table.csv 左結合
            3. ruler_pencilcase_table.csv で filename を取得
        """
        input_data_dir = self.rute / input_data_path

        # 1. quotation の読み込み
        quotation_df = self.load_quotation_table(config, input_data_path)

        # 2. condition_table とのマージ
        cond_table_info = config["condition_table_csv"]
        condition_table_path = input_data_dir / cond_table_info["path"]
        merged_df = self.merge_condition_info(quotation_df, condition_table_path)

        # 3. ruler_pencilcase_table.csv の検索
        ruler_pencilcase_info = config["fbc_lut_table_csv"]  # JSON キーを変えるなら注意
        ruler_pencilcase_table_path = input_data_dir / ruler_pencilcase_info["path"]
        final_df = self.find_ruler_pencilcase_records(merged_df, ruler_pencilcase_table_path)

        return final_df

    def make_output_data_path(self, output_dir: str = "output") -> Path:
        """
        出力先ディレクトリを作成し、その Path を返す。
        """
        out_path = self.rute / output_dir
        out_path.mkdir(parents=True, exist_ok=True)
        return out_path


class Calculator:
    """
    Loader が作成した DataFrame ([ID, ink_cycle, filename]) に対して、
    実際に Parquet を開いて計算処理を行うクラス。
    """

    def __init__(self, loader: Loader) -> None:
        self.loader = loader  # frame_size, tracking_marker などを参照可能

    def load_ruler_data(self, parquet_file: Path) -> pl.DataFrame:
        """
        指定された Parquet ファイルを読み込み、ruler データを取得する。
        (列: chip, block, page, wl, string, unit, ink, ruler などを想定)
        """
        try:
            df = pl.read_parquet(str(parquet_file))
            return df
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Parquet ファイルが見つかりません: {parquet_file}"
            ) from e
        except Exception as e:
            raise e

    def get_ruler_for_ink(self, df: pl.DataFrame, target_ink: float) -> float:
        """
        df["ink"] に対して target_ink が存在すればその ruler を返し、
        なければ np.interp で線形補間して返す。
        """
        ink_series = df["ink"]
        ruler_series = df["ruler"]

        if target_ink in ink_series.to_list():
            matched_rows = df.filter(pl.col("ink") == target_ink)
            return float(matched_rows[0, "ruler"])
        else:
            ink_vals = ink_series.to_list()
            ruler_vals = ruler_series.to_list()
            interpolated_ruler = float(np.interp(target_ink, ink_vals, ruler_vals))
            return interpolated_ruler

    def add_margin(self, ruler_value: float) -> float:
        """
        単一の ruler 値に (tracking_marker * read_crinkle) を掛けるサンプル。
        """
        ruler_with_tracking = ruler_value * self.loader.tracking_marker
        ruler_with_noise = ruler_with_tracking * self.loader.read_crinkle
        return ruler_with_noise

    def glue_calculate(self, ruler_value: float) -> float:
        """
        単一の ruler 値から glue を計算する。
        ruler / frame_size
        """
        if self.loader.frame_size == 0:
            raise ValueError("frame_size が 0 のため GLUE 計算ができません。pen/book の設定を確認してください。")
        return ruler_value / self.loader.frame_size

    def process_by_id(
        self, df: pl.DataFrame, input_data_path: Union[str, Path]
    ) -> pl.DataFrame:
        """
        従来のサンプル実装: 
        [ID, ink_cycle, filename] をループし、該当 parquet を開いて
        - ink_cycle に応じて ruler を補間取得
        - マージン付加
        - GLUE 計算
        する。
        """
        results = []
        for row in df.iter_rows(named=True):
            _id = row["ID"]
            ink_cycle_val = row["ink_cycle"]
            filename = row["filename"]

            if filename is None:
                continue

            parquet_path = Path(input_data_path) / "ruler_data" / filename
            ruler_df = self.load_ruler_data(parquet_path)

            ruler_value = self.get_ruler_for_ink(ruler_df, ink_cycle_val)
            ruler_with_margin = self.add_margin(ruler_value)
            glue_value = self.glue_calculate(ruler_with_margin)

            results.append(
                {
                    "ID": _id,
                    "ink_cycle": ink_cycle_val,
                    "filename": filename,
                    "ruler": ruler_value,
                    "ruler_margin": ruler_with_margin,
                    "glue": glue_value,
                }
            )
        return pl.DataFrame(results)

    # ------------------------------------------------------------------------------------
    # Page/Wl ごとの平均 → 補間 → マージン → GLUE → (page, wl) 出力 → page 単位再集約
    # ------------------------------------------------------------------------------------
    def process_by_page_wl(
        self,
        parquet_file: Path,
        output_dir: Path,
        do_interpolation: bool = False,
        target_ink: float = None
    ) -> None:
        """
        例: 1つの Parquet (= 1ファイル) を読込み、
        (page, wl) 単位で集約して ruler の平均 → 必要があれば線形補間 → マージン → GLUE → CSV 出力
        さらに (page) 単位に再集約して2段階目の出力。
        """
        # 1. データ読込
        df = self.load_ruler_data(parquet_file)
        # 想定列: chip, block, page, wl, ink, ruler, ...

        # 2. (page, wl) ごとに ruler の平均を算出
        df_grouped = (
            df.groupby(["page", "wl"])
              .agg([
                  pl.col("ruler").mean().alias("ruler_mean"),
                  # 必要なら ink の min/max などもまとめる
              ])
        )

        # 3. 線形補間 (例: target_ink をベースに np.interp する) 
        #    ここでは「補間なし or ダミー的にそのまま」程度の例にしておきます
        results = []
        for row in df_grouped.iter_rows(named=True):
            page_val = row["page"]
            wl_val   = row["wl"]
            ruler_val  = row["ruler_mean"]

            if do_interpolation and (target_ink is not None):
                # TODO: 実際は (page, wl) でさらに ink-ruler シリーズを見て補間するイメージ
                interpolated_ruler = ruler_val  # デモ用: 値を変えない
            else:
                interpolated_ruler = ruler_val

            results.append({
                "page": page_val,
                "wl": wl_val,
                "ruler_interpolated": interpolated_ruler
            })

        df_after_interp = pl.DataFrame(results)

        # 4. マージン追加 → GLUE 計算
        df_after_margin = df_after_interp.with_columns([
            (
                pl.col("ruler_interpolated") 
                * self.loader.tracking_marker 
                * self.loader.read_crinkle
            ).alias("ruler_with_margin")
        ])

        df_after_glue = df_after_margin.with_columns([
            (pl.col("ruler_with_margin") / self.loader.frame_size).alias("glue")
        ])

        # (page, wl) 単位の結果を出力
        out_page_wl = output_dir / "result_page_wl.csv"
        df_after_glue.write_csv(out_page_wl)

        # 5. wl は page 内の詳細ということで、page 単位に再集約
        df_page_only = (
            df_after_glue.groupby("page")
            .agg([
                pl.col("ruler_with_margin").mean().alias("ruler_with_margin_mean"),
                pl.col("glue").mean().alias("glue_mean")
            ])
            .sort("page")
        )

        # page 単位の結果を出力
        out_page_only = output_dir / "result_page_only.csv"
        df_page_only.write_csv(out_page_only)


def make_output(df: pl.DataFrame, output_dir: Path) -> None:
    """
    最終結果を CSV 等で出力する関数。
    """
    out_path = output_dir / "final_result.csv"
    df.write_csv(str(out_path))


def main() -> None:
    """
    メイン関数:
      1. 引数パース (--pen, --book など)
      2. Loader で ID/ink_cycle/filename のテーブルを取得
      3. Calculator で従来の ID & ink_cycle 処理 or Page/Wl 処理を行う
      4. 結果を出力
    """
    args = argument_parse()

    # JSON ファイルの読み込み
    config_path = Path(args.config_json)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Loader の初期化
    # --pen, --book のどちらかが True/False になる
    loader = Loader(pen=args.pen, book=args.book)

    # Loader で ID, ink_cycle, filename の一覧を作る
    id_ink_filename_df = loader.run_loader_process(config, args.input_data_path)
    print("\n--- Loader 結果 (ID, ink_cycle, filename) ---")
    print(id_ink_filename_df)

    # Calculator の初期化
    calculator = Calculator(loader=loader)

    # 例1: 従来の「ID & ink_cycle」処理を実行
    final_df = calculator.process_by_id(id_ink_filename_df, args.input_data_path)
    print("\n--- Calculator (ink_cycle) 処理結果 ---")
    print(final_df)

    # 出力ディレクトリ作成
    output_dir = loader.make_output_data_path("output")

    # 従来のIDごとの結果を CSV 出力
    make_output(final_df, output_dir)

    # 例2: 新しく追加した「(page, wl) 単位で集計→GLUE計算→(page, wl)とpage結果の2種類を出力」処理
    if final_df.shape[0] > 0:
        some_filename = final_df.select("filename")[0, 0]
        if some_filename is not None:
            parquet_path = Path(args.input_data_path) / "ruler_data" / some_filename
            calculator.process_by_page_wl(
                parquet_file=parquet_path,
                output_dir=output_dir,
                do_interpolation=False,  # 必要なら True
                target_ink=1000.0        # 必要なら補間に使う ink
            )
            print(f"\n(page, wl) 単位と page 単位の2種のCSVを出力しました -> {output_dir}")

    print("\n--- 全処理完了 ---")


if __name__ == "__main__":
    main()