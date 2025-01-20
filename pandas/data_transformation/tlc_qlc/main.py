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
        """
        # ステップ1: WECyc作成
        self.df['WECyc'] = int(self.filename.split('_')[0])
        logging.info("After Step 1: WECyc作成")
        display(self.df.head())
        
        # ステップ2: DR作成
        self.df['DR'] = int(self.filename.split('_')[1].split('.')[0])
        logging.info("After Step 2: DR作成")
        display(self.df.head())
        
        # ステップ3: WECycからBlockID作成
        self.df['BlockID'] = np.random.randint(1, 49)
        logging.info("After Step 3: WECycからBlockID作成")
        display(self.df.head())
        
        # ステップ4: uid作成
        self.df['uid'] = '_'.join([str(np.random.randint(10000000, 99999999)) for _ in range(4)])
        logging.info("After Step 4: uid作成")
        display(self.df.head())

    def create_page_data(self) -> None:
        """
        ページデータの作成
        """
        # ステップ5: seg合算
        agg_dict = {f'shift{i}': 'first' for i in 'ABCDEFG'}
        agg_dict.update({f'fbc{i}': 'sum' for i in 'ABCDEFG'})
        # 必要な列を保持
        agg_dict.update({'WECyc': 'first', 'DR': 'first', 'BlockID': 'first', 'uid': 'first'})
        self.df = self.df.groupby(['Unit', 'shiftIndex']).agg(agg_dict).reset_index()
        logging.info("After Step 5: seg合算")
        display(self.df.head())
        
        # ステップ6: shiftIndexを削除
        if 'shiftIndex' in self.df.columns:
            self.df.drop(columns=['shiftIndex'], inplace=True)
            logging.info("After Step 6: shiftIndexを削除")
            display(self.df.head())
        
        # ステップ7: fbcXを選択する
        def select_fbc_closest_to_zero(group: pd.DataFrame) -> pd.Series:
            closest_shift_index = group[[f'shift{i}' for i in 'ABCDEFG']].abs().idxmin(axis=1)
            return group.lookup(closest_shift_index.index, 'fbc' + closest_shift_index.str[-1])
        
        self.df['FBC'] = self.df.groupby('Unit').apply(select_fbc_closest_to_zero).reset_index(level=0, drop=True)
        logging.info("After Step 7: fbcXを選択する")
        display(self.df.head())
        
        # ステップ8: shiftXを削除する
        self.df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
        logging.info("After Step 8: shiftXを削除する")
        display(self.df.head())
        
        # ステップ9: pageを作成する
        self.df['Page'] = np.select(
            [True, True, True],
            ['Lower', 'Middle', 'Upper'],
            default='Top'
        )
        self.df['FBC'] = np.select(
            [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper'],
            [self.df['fbcD'], self.df['fbcA'] + self.df['fbcC'] + self.df['fbcF'], self.df['fbcB'] + self.df['fbcE'] + self.df['fbcG']]
        )
        logging.info("After Step 9: pageを作成する")
        display(self.df.head())

    def create_address_info(self) -> None:
        """
        アドレス情報の作成
        """
        # ステップ10: stringを作成する
        self.df['String'] = self.df.groupby('Unit').cumcount() % 4
        logging.info("After Step 10: Create String")
        display(self.df.head())
        
        # ステップ11: Create WL
        self.df['WL'] = self.df['Unit'] // 4
        logging.info("After Step 11: Create WL")
        display(self.df.head())

    def process(self) -> pd.DataFrame:
        """
        データの処理を実行

        Returns:
            pd.DataFrame: 処理後のデータフレーム
        """
        self.create_basic_data()
        self.create_page_data()
        self.create_address_info()
        return self.df

class TLCProcessor(DataProcessor):
    def create_page_data(self) -> None:
        """
        TLC用のページデータの作成
        """
        super().create_page_data()

class QLCProcessor(DataProcessor):
    def create_page_data(self) -> None:
        """
        QLC用のページデータの作成
        """
        # QLC特有の処理を追加
        super().create_page_data()
        # QLCの特定処理（例: self.df['FBC'] の計算）
        self.df['Page'] = np.select(
            [self.df['seg'].isin(range(1, 4))],
            ['Top'],
            default=self.df['Page']
        )

def process_all_files(pattern: str) -> pd.DataFrame:
    """
    ワイルドカードパターンに一致する全てのファイルを処理

    Args:
        pattern (str): ファイルパターン

    Returns:
        pd.DataFrame: 全ての処理済みデータを含むデータフレーム
    """
    all_processed_data: List[pd.DataFrame] = []
    for filepath in glob.glob(pattern, recursive=True):
        try:
            if filepath.endswith('.csv'):
                logging.info(f"Processing file: {filepath}")
                df = pd.read_csv(filepath)
                filename = os.path.basename(filepath)
                if 'TLC' in pattern:
                    processor = TLCProcessor(df, filename)
                elif 'QLC' in pattern:
                    processor = QLCProcessor(df, filename)
                else:
                    logging.error(f"Unknown file pattern: {pattern}")
                    continue
                processed_df = processor.process()
                all_processed_data.append(processed_df)
        except Exception as e:
            logging.error(f"Error processing file {filepath}: {e}")
    
    return pd.concat(all_processed_data, ignore_index=True)

if __name__ == "__main__":
    with open('config.yaml') as file:
        config = yaml.safe_load(file)
    
    pattern = config['file_pattern']
    output_file = config['output_file']
    
    processed_data = process_all_files(pattern)
    processed_data.to_csv(output_file, index=False)
