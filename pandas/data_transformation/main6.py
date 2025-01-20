import os
import pandas as pd
import numpy as np
import glob
import logging
import yaml
from typing import List

# ログの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataProcessor:
    def __init__(self, df: pd.DataFrame, filename: str) -> None:
        """
        DataProcessorクラスの初期化

        Args:
            df (pd.DataFrame): 入力データフレーム
            filename (str): 処理対象のファイル名
        """
        self.df = df
        self.filename = filename

    def create_basic_data(self) -> None:
        """
        基本データの作成

        WECyc、DR、BlockID、uidカラムを追加します。
        """
        try:
            self.df['WECyc'] = int(self.filename.split('_')[0])
            self.df['DR'] = int(self.filename.split('_')[1].split('.')[0])
            self.df['BlockID'] = np.random.randint(1, 49)
            self.df['uid'] = '_'.join([str(np.random.randint(10000000, 99999999)) for _ in range(4)])
            logging.info("基本データ作成完了")
        except Exception as e:
            logging.error(f"基本データ作成中にエラーが発生しました: {e}")
        display(self.df.head())

    def aggregate_data(self) -> None:
        """
        データの集計

        fbcおよびshiftカラムを集計し、shiftIndexカラムを削除します。
        """
        try:
            agg_dict = {f'shift{i}': 'first' for i in 'ABCDEFG'}
            agg_dict.update({f'fbc{i}': 'sum' for i in 'ABCDEFG'})
            agg_dict.update({'WECyc': 'first', 'DR': 'first', 'BlockID': 'first', 'uid': 'first'})
            self.df = self.df.groupby(['Unit', 'shiftIndex']).agg(agg_dict).reset_index()
            logging.info("データ集計完了")
            display(self.df.head())
            
            if 'shiftIndex' in self.df.columns:
                self.df.drop(columns=['shiftIndex'], inplace=True)
                logging.info("shiftIndexカラム削除完了")
                display(self.df.head())
        except Exception as e:
            logging.error(f"データ集計中にエラーが発生しました: {e}")

    def select_fbc(self) -> None:
        """
        FBCを選択

        各shiftXカラムの値が0に最も近いfbcXの値を選択します。
        """
        try:
            def select_fbc_closest_to_zero(row):
                shifts = row[[f'shift{i}' for i in 'ABCDEFG']]
                fbc_values = row[[f'fbc{i}' for i in 'ABCDEFG']]
                closest_shift_index = shifts.abs().idxmin()
                return fbc_values[closest_shift_index]

            self.df['FBC'] = self.df.apply(select_fbc_closest_to_zero, axis=1)
            logging.info("FBC選択完了")
            display(self.df.head())
        except Exception as e:
            logging.error(f"FBC選択中にエラーが発生しました: {e}")

    def create_page_and_remove_shifts(self) -> None:
        """
        ページ作成とshiftカラムの削除

        Pageカラムを作成し、shiftXカラムを削除します。
        """
        try:
            self.df['Page'] = np.select(
                [True, True, True],
                ['Lower', 'Middle', 'Upper'],
                default='Top'
            )
            self.df['FBC'] = np.select(
                [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper'],
                [self.df['fbcD'], self.df['fbcA'] + self.df['fbcC'] + self.df['fbcF'], self.df['fbcB'] + self.df['fbcE'] + self.df['fbcG']]
            )
            self.df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
            logging.info("ページ作成とshiftカラム削除完了")
            display(self.df.head())
        except Exception as e:
            logging.error(f"ページ作成とshiftカラム削除中にエラーが発生しました: {e}")

    def create_address_info(self) -> None:
        """
        アドレス情報の作成

        StringとWLカラムを追加します。
        """
        try:
            self.df['String'] = self.df.groupby('Unit').cumcount() % 4
            self.df['WL'] = self.df['Unit'] // 4
            logging.info("アドレス情報作成完了")
            display(self.df.head())
        except Exception as e:
            logging.error(f"アドレス情報作成中にエラーが発生しました: {e}")

    def process(self) -> pd.DataFrame:
        """
        データの処理を実行

        Returns:
            pd.DataFrame: 処理後のデータフレーム
        """
        self.create_basic_data()
        self.aggregate_data()
        self.select_fbc()
        self.create_page_and_remove_shifts()
        self.create_address_info()
        return self.df

def process_all_files(pattern: str) -> pd.DataFrame:
    """
    ワイルドカードパターンに一致する全てのファイルを処理

    Args:
        pattern (str): ファイルパターン

    Returns:
        pd.DataFrame: 全ての処理済みデータを含むデータフレーム
    """
    all_processed_data = []
    for filepath in glob.glob(pattern, recursive=True):
        try:
            if filepath.endswith('.csv'):
                logging.info(f"ファイル処理中: {filepath}")
                df = pd.read_csv(filepath)
                processor = DataProcessor(df, os.path.basename(filepath))
                processed_df = processor.process()
                all_processed_data.append(processed_df)
        except Exception as e:
            logging.error(f"ファイル {filepath} の処理中にエラーが発生しました: {e}")
    
    return pd.concat(all_processed_data, ignore_index=True)

if __name__ == "__main__":
    with open('config.yaml') as file:
        config = yaml.safe_load(file)
    
    pattern = config['file_pattern']
    output_file = config['output_file']
    
    processed_data = process_all_files(pattern)
    processed_data.to_csv(output_file, index=False)
