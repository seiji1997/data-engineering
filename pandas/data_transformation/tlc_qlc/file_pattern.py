def process_all_files(pattern: str) -> pd.DataFrame:
    """
    Process all files matching the wildcard pattern.

    Args:
        pattern (str): File pattern.

    Returns:
        pd.DataFrame: Data frame containing all processed data.
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
