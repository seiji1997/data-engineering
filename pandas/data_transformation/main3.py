import os
import pandas as pd
import numpy as np
import glob
import logging
import yaml

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataProcessor:
    def __init__(self, df: pd.DataFrame, filename: str) -> None:
        """
        Initialize the DataProcessor class

        Args:
            df (pd.DataFrame): Input DataFrame
            filename (str): Name of the file being processed
        """
        self.df = df
        self.filename = filename

    def create_basic_data(self) -> None:
        """
        Create basic data by adding WECyc, DR, BlockID, and uid columns
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
        logging.info("After Step 3: Create BlockID")
        display(self.df.head())
        
        # Step 4: Create uid
        self.df['uid'] = '_'.join([str(np.random.randint(10000000, 99999999)) for _ in range(4)])
        logging.info("After Step 4: Create uid")
        display(self.df.head())

    def create_page_data(self) -> None:
        """
        Create page data by aggregating and processing fbc and shift columns
        """
        # Step 5: Aggregate by seg
        agg_dict = {f'shift{i}': 'first' for i in 'ABCDEFG'}
        agg_dict.update({f'fbc{i}': 'sum' for i in 'ABCDEFG'})
        # Keep necessary columns
        agg_dict.update({'WECyc': 'first', 'DR': 'first', 'BlockID': 'first', 'uid': 'first'})
        self.df = self.df.groupby(['Unit', 'shiftIndex']).agg(agg_dict).reset_index()
        logging.info("After Step 5: Aggregate by seg")
        display(self.df.head())
        
        # Step 6: Remove shiftIndex
        if 'shiftIndex' in self.df.columns:
            self.df.drop(columns=['shiftIndex'], inplace=True)
            logging.info("After Step 6: Remove shiftIndex")
            display(self.df.head())

        # Step 9: Create Page
        self.df['Page'] = np.select(
            [True, True, True],
            ['Lower', 'Middle', 'Upper'],
            default='Top'
        )
        self.df['FBC'] = np.select(
            [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper'],
            [self.df['fbcD'], self.df['fbcA'] + self.df['fbcC'] + self.df['fbcF'], self.df['fbcB'] + self.df['fbcE'] + self.df['fbcG']]
        )
        logging.info("After Step 9: Create Page")
        display(self.df.head())

        # Step 7: Select FBC closest to zero
        def select_fbc_closest_to_zero(df):
            closest_to_zero = df.loc[df[['shiftA', 'shiftB', 'shiftC', 'shiftD', 'shiftE', 'shiftF', 'shiftG']].abs().idxmin(axis=1)]
            return closest_to_zero[['fbcA', 'fbcB', 'fbcC', 'fbcD', 'fbcE', 'fbcF', 'fbcG']].values
        self.df['FBC'] = select_fbc_closest_to_zero(self.df)
        logging.info("After Step 7: Select FBC closest to zero")
        display(self.df.head())
        
        # Step 8: Remove shiftX columns
        self.df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
        logging.info("After Step 8: Remove shiftX columns")
        display(self.df.head())

    def create_address_info(self) -> None:
        """
        Create address information by adding String and WL columns
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
        Execute data processing

        Returns:
            pd.DataFrame: Processed DataFrame
        """
        self.create_basic_data()
        self.create_page_data()
        self.create_address_info()
        return self.df

def process_all_files(pattern: str) -> pd.DataFrame:
    """
    Process all files matching the wildcard pattern

    Args:
        pattern (str): File pattern

    Returns:
        pd.DataFrame: Processed data from all files
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
