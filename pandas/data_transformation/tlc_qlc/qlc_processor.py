class QLCProcessor(DataProcessor):
    def create_page_data(self) -> None:
        """
        ページデータの作成
        """
        # ステップ5: seg合算
        agg_dict = {f'shiftS{i}': 'first' for i in range(16)}
        agg_dict.update({f'fbcS{i}': 'sum' for i in range(16)})
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
        def select_fbc_closest_to_zero(df):
            for unit in df['Unit'].unique():
                unit_df = df[df['Unit'] == unit]
                closest_to_zero = unit_df.loc[unit_df[[f'shiftS{i}' for i in range(16)]].abs().idxmin(axis=0)]
                df.loc[df['Unit'] == unit, 'FBC'] = closest_to_zero[[f'fbcS{i}' for i in range(16)]].values
            return df

        self.df = select_fbc_closest_to_zero(self.df)
        logging.info("After Step 7: fbcXを選択する")
        display(self.df.head())

        # ステップ8: shiftXを削除する
        self.df.drop(columns=[f'shiftS{i}' for i in range(16)], inplace=True)
        logging.info("After Step 8: shiftXを削除する")
        display(self.df.head())

        # ステップ9: pageを作成する
        self.df['Page'] = np.select(
            [True, True, True, True],
            ['Lower', 'Middle', 'Upper', 'Top']
        )
        self.df['FBC'] = np.select(
            [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper', self.df['Page'] == 'Top'],
            [self.df['fbcS1'] + self.df['fbcS4'] + self.df['fbcS5'], 
             self.df['fbcS2'] + self.df['fbcS3'] + self.df['fbcS8'] + self.df['fbcS10'] + self.df['fbcS14'] + self.df['fbcS15'], 
             self.df['fbcS6'] + self.df['fbcS9'] + self.df['fbcS11'] + self.df['fbcS12'] + self.df['fbcS13']]
        )
        logging.info("After Step 9: pageを作成する")
        display(self.df.head())
