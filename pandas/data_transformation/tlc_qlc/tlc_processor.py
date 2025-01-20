class TLCProcessor(DataProcessor):
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
        def select_fbc_closest_to_zero(df):
            for unit in df['Unit'].unique():
                unit_df = df[df['Unit'] == unit]
                closest_to_zero = unit_df.loc[unit_df[[f'shift{i}' for i in 'ABCDEFG']].abs().idxmin(axis=0)]
                df.loc[df['Unit'] == unit, 'FBC'] = closest_to_zero[[f'fbc{i}' for i in 'ABCDEFG']].values
            return df

        self.df = select_fbc_closest_to_zero(self.df)
        logging.info("After Step 7: fbcXを選択する")
        display(self.df.head())

        # ステップ8: shiftXを削除する
        self.df.drop(columns=[f'shift{i}' for i in 'ABCDEFG'], inplace=True)
        logging.info("After Step 8: shiftXを削除する")
        display(self.df.head())

        # ステップ9: pageを作成する
        self.df['Page'] = np.select(
            [True, True, True],
            ['Lower', 'Middle', 'Upper']
        )
        self.df['FBC'] = np.select(
            [self.df['Page'] == 'Lower', self.df['Page'] == 'Middle', self.df['Page'] == 'Upper'],
            [self.df['fbcD'], self.df['fbcA'] + self.df['fbcC'] + self.df['fbcF'], self.df['fbcB'] + self.df['fbcE'] + self.df['fbcG']]
        )
        logging.info("After Step 9: pageを作成する")
        display(self.df.head())
