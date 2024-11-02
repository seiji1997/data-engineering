import os
import pandas as pd
import numpy as np
import glob
import logging
import yaml

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

        fbcおよびshiftカラムを集計し処理します。
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

        # ステップ7: FBCを選択
        def select_fbc_closest_to_zero(row):
            shifts = row[[f'shift{i}' for i in 'ABCDEFG']]
            fbc_values = row[[f'fbc{i}' for i in 'ABCDEFG']]
            closest_shift_index = shifts.abs().idxmin()
            return fbc_values[closest_shift_index]

        self.df['FBC'] = self.df.apply(select_fbc_closest_to_zero, axis=1)
        logging.info("After Step 7: FBCを選択")
        display(self.df.head())

        # ステップ8: shiftXカラムを削除
        self.df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
        logging.info("After Step 8: shiftXカラムを削除")
        display(self.df.head())

        # ステップ9: ページ作成
        self.df['Page'] = np.select(
            [True, True, True],
            ['Lower', 'Middle', 'Upper'],
            default='Top'
        )
        self.df['FBC'] = np.select(
            [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper'],
            [self.df['fbcD'], self.df['fbcA'] + self.df['fbcC'] + self.df['fbcF'], self.df['fbcB'] + self.df['fbcE'] + self.df['fbcG']]
        )
        logging.info("After Step 9: ページ作成")
        display(self.df.head())

    def create_address_info(self) -> None:
        """
        アドレス情報の作成

        StringとWLカラムを追加します。
        """
        # ステップ10: stringを作成する
        self.df['String'] = self.df.groupby('Unit').cumcount() % 4
        logging.info("After Step 10: stringを作成する")
        display(self.df.head())
        
        # ステップ11: WLを作成する
        self.df['WL'] = self.df['Unit'] // 4
        logging.info("After Step 11: WLを作成する")
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
                logging.info(f"Processing file: {filepath}")
                df = pd.read_csv(filepath)
                processor = DataProcessor(df, os.path.basename(filepath))
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
