# config.yaml

data_config:

  directories:
    input_data: "data/"
    output_pen: "output_pen/"
    output_book: "output_book/"

  excel_files:
    - name: "quotation"
      path: "data/quotation_test.xlsx"
      sheet_name: "Sheet1"
      # 読み込み時のカラムや型指定があれば記載してもよい
      columns:
        - ID
        - force_refresh
        - stapler_time
        - tape
        - ink_cycle
        - tracking_marker
        - read_crinkle

  csv_files:
    - name: "condition_table"
      path: "data/condition_table_test.csv"
    - name: "ruler_pencilcase_table"
      path: "data/ruler_pencilcase_table_test.csv"

  parquet_files:
    - name: "fbc_sample_fast"
      path: "data/ruler_data/fbc_sample_fast.parquet"
    - name: "fbc_sample_lna"
      path: "data/ruler_data/fbc_sample_lna.parquet"