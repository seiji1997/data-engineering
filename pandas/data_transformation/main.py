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
        Initializes the DataProcessor class.

        Args:
            df (pd.DataFrame): Input DataFrame.
            filename (str): Name of the file being processed.
        """
        self.df = df
        self.filename = filename

    def create_basic_data(self) -> None:
        """
        Creates basic data including WECyc, DR, BlockID, and uid.
        """
        # Step 1: Create WECyc
        self.df['WECyc'] = int(self.filename.split('_')[0])
        logging.info("After Step 1: Create WECyc")
        display(self.df.head())
        
        # Step 2: Create DR
        self.df['DR'] = int(self.filename.split('_')[1].split('.')[0])
        logging.info("After Step 2: Create DR")
        display(self.df.head())
        
        # Step 3: Create BlockID from WECyc
        self.df['BlockID'] = np.random.randint(1, 49)
        logging.info("After Step 3: Create BlockID from WECyc")
        display(self.df.head())
        
        # Step 4: Create uid
        self.df['uid'] = '_'.join([str(np.random.randint(10000000, 99999999)) for _ in range(4)])
        logging.info("After Step 4: Create uid")
        display(self.df.head())

    def create_page_data(self) -> None:
        """
        Creates page data by aggregating segments and selecting the FBC closest to zero.
        """
        # Step 5: Aggregate segments
        agg_dict = {f'shift{i}': 'first' for i in 'ABCDEFG'}
        agg_dict.update({f'fbc{i}': 'sum' for i in 'ABCDEFG'})
        # Retain necessary columns
        agg_dict.update({'WECyc': 'first', 'DR': 'first', 'BlockID': 'first', 'uid': 'first'})
        self.df = self.df.groupby(['Unit', 'shiftIndex']).agg(agg_dict).reset_index()
        logging.info("After Step 5: Aggregate segments")
        display(self.df.head())
        
        # Step 6: Remove shiftIndex
        if 'shiftIndex' in self.df.columns:
            self.df.drop(columns=['shiftIndex'], inplace=True)
            logging.info("After Step 6: Remove shiftIndex")
            display(self.df.head())

        # Step 7: Select FBC closest to zero
        def select_fbc_closest_to_zero(row):
            shifts = row[[f'shift{i}' for i in 'ABCDEFG']]
            fbc_values = row[[f'fbc{i}' for i in 'ABCDEFG']]
            closest_shift_index = shifts.abs().idxmin()
            return fbc_values[closest_shift_index]

        self.df['FBC'] = self.df.apply(select_fbc_closest_to_zero, axis=1)
        logging.info("After Step 7: Select FBC closest to zero")
        display(self.df.head())
        
        # Step 8: Remove shiftX columns
        self.df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
        logging.info("After Step 8: Remove shiftX columns")
        display(self.df.head())
        
        # Step 9: Create Page column
        self.df['Page'] = np.select(
            [self.df['seg'] == 0, self.df['seg'] == 1, self.df['seg'] == 2],
            ['Lower', 'Middle', 'Upper'],
            default='Top'
        )
        self.df['FBC'] = np.select(
            [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper'],
            [self.df['fbcD'], self.df['fbcA'] + self.df['fbcC'] + self.df['fbcF'], self.df['fbcB'] + self.df['fbcE'] + self.df['fbcG']]
        )
        logging.info("After Step 9: Create Page column")
        display(self.df.head())

    def create_address_info(self) -> None:
        """
        Creates address information including String and WL.
        """
        # Step 10: Create String
        self.df['String'] = self.df.groupby('Unit').cumcount() % 4
        logging.info("After Step 10: Create String")
        display(self.df.head())
        
        # Step 11: Create WL
        self.df['WL'] = self.df['Unit'] // 4
        logging.info("After Step 11: Create WL")
        display(self.df.head())

    def process(self) -> pd.DataFrame:
        """
        Executes data processing steps.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        self.create_basic_data()
        self.create_page_data()
        self.create_address_info()
        return self.df

def process_all_files(pattern: str) -> pd.DataFrame:
    """
    Processes all files matching the given wildcard pattern.

    Args:
        pattern (str): File pattern.

    Returns:
        pd.DataFrame: DataFrame containing all processed data.
    """
    all_processed_data: List[pd.DataFrame] = []
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

