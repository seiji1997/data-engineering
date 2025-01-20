

import pandas as pd
import numpy as np
import logging
import glob
import yaml
from typing import List

# ログの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_step1_to_6(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    ステップ1から6までの処理を行う

    Args:
        df (pd.DataFrame): 入力データフレーム
        filename (str): 処理対象のファイル名

    Returns:
        pd.DataFrame: ステップ6まで処理済みのデータフレーム
    """
    # ステップ1: WECyc作成
    df['WECyc'] = int(filename.split('_')[0])
    logging.info("After Step 1: WECyc作成")
    display(df.head())
    
    # ステップ2: DR作成
    df['DR'] = int(filename.split('_')[1].split('.')[0])
    logging.info("After Step 2: DR作成")
    display(df.head())
    
    # ステップ3: WECycからBlockID作成
    df['BlockID'] = np.random.randint(1, 49)
    logging.info("After Step 3: WECycからBlockID作成")
    display(df.head())
    
    # ステップ4: uid作成
    df['uid'] = '_'.join([str(np.random.randint(10000000, 99999999)) for _ in range(4)])
    logging.info("After Step 4: uid作成")
    display(df.head())
    
    # ステップ5: seg合算
    agg_dict = {f'shift{i}': 'first' for i in 'ABCDEFG'}
    agg_dict.update({f'fbc{i}': 'sum' for i in 'ABCDEFG'})
    agg_dict.update({'WECyc': 'first', 'DR': 'first', 'BlockID': 'first', 'uid': 'first'})
    df = df.groupby(['unit', 'shiftIndex']).agg(agg_dict).reset_index()
    logging.info("After Step 5: seg合算")
    display(df.head())
    
    # ステップ6: shiftIndexを削除
    if 'shiftIndex' in df.columns:
        df.drop(columns=['shiftIndex'], inplace=True)
        logging.info("After Step 6: shiftIndexを削除")
        display(df.head())
    
    return df

def process_step7(df: pd.DataFrame) -> pd.DataFrame:
    """
    ステップ7の処理を行う

    Args:
        df (pd.DataFrame): ステップ6まで処理済みのデータフレーム

    Returns:
        pd.DataFrame: ステップ7まで処理済みのデータフレーム
    """
    def select_fbc_closest_to_zero(group):
        shifts = group[[f'shift{i}' for i in 'ABCDEFG']]
        fbc_values = group[[f'fbc{i}' for i in 'ABCDEFG']]
        closest_shift_index = shifts.abs().idxmin(axis=1)
        return fbc_values.lookup(range(len(group)), closest_shift_index)

    df['FBC'] = df.groupby('unit').apply(select_fbc_closest_to_zero).reset_index(level=0, drop=True)
    logging.info("After Step 7: fbcXを選択する")
    display(df.head())

    return df

def process_step8(df: pd.DataFrame) -> pd.DataFrame:
    """
    ステップ8の処理を行う

    Args:
        df (pd.DataFrame): ステップ7まで処理済みのデータフレーム

    Returns:
        pd.DataFrame: ステップ8まで処理済みのデータフレーム
    """
    df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
    logging.info("After Step 8: shiftXを削除する")
    display(df.head())
    
    return df

def process_step9(df: pd.DataFrame) -> pd.DataFrame:
    """
    ステップ9の処理を行う

    Args:
        df (pd.DataFrame): ステップ8まで処理済みのデータフレーム

    Returns:
        pd.DataFrame: ステップ9まで処理済みのデータフレーム
    """
    df['Page'] = np.select(
        [df['seg'] == 0, df['seg'] == 1, df['seg'] == 2],
        ['Lower', 'Middle', 'Upper'],
        default='Top'
    )
    df['FBC'] = np.select(
        [df['Page'] == 'Lower', df['Page'] == 'Middle', df['Page'] == 'Upper'],
        [df['fbcD'], df['fbcA'] + df['fbcC'] + df['fbcF'], df['fbcB'] + df['fbcE'] + df['fbcG']]
    )
    logging.info("After Step 9: pageを作成する")
    display(df.head())

    return df

def create_address_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    アドレス情報の作成

    Args:
        df (pd.DataFrame): ステップ9まで処理済みのデータフレーム

    Returns:
        pd.DataFrame: アドレス情報が追加されたデータフレーム
    """
    # ステップ10: stringを作成する
    df['String'] = df.groupby('unit').cumcount() % 4
    logging.info("After Step 10: stringを作成する")
    display(df.head())
    
    # ステップ11: WLを作成する
    df['WL'] = df['unit'] // 4
    logging.info("After Step 11: WLを作成する")
    display(df.head())
    
    return df

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
                
                # ステップ1から6の処理
                df = process_step1_to_6(df, filename)
                
                # ステップ7の処理
                df = process_step7(df)
                
                # ステップ8の処理
                df = process_step8(df)
                
                # ステップ9の処理
                df = process_step9(df)
                
                # アドレス情報の作成（ステップ10、11）
                df = create_address_info(df)
                
                # 必要なカラムの選択
                df = df[['DR', 'WECyc', 'Page', 'BlockID', 'WL', 'String', 'uid', 'FBC']]
                all_processed_data.append(df)
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
    logging.info(f"Processed data saved to {output_file}")
