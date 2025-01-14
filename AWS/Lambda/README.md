
1. 正常系 (Happy Path)

1.1. コマンドライン引数に関する正常系
	1.	--mode pen を指定して実行し、フレームサイズが 36672 になる。
	2.	--mode book を指定して実行し、フレームサイズが 37952 になる。
	3.	--input_data_path に適切なパスを与え、そこに必要なファイル(Excel, CSV, Parquet等)がすべて存在する場合、最終的に出力が得られる。
	4.	--mode 未指定(実装中デフォルトが pen のため、自動的に pen になる) → 36672 で計算が進む。

1.2. Excel / CSV / LUT 読み込みに関する正常系
	1.	quotation_test.xlsx が正しいシート名 (Sheet1) とカラム構成を持ち、想定型 (pl.Int64, pl.Float64 など) で読み込める。
	2.	condition_table_test.csv が想定カラム (condition_temp, condition_time, scotch_temp, scotch_time) を持ち、正しく読み込める。
	3.	ruler_pencilcase_table_test.csv が想定カラム (inkcycle_start, inkcycle_end, readmode, tape, stapler, stapler_temp, filename) を持ち、かつ readmode が "Fast" or "LNA" のみ。
	4.	LUT テーブルに複数レコードがあるものの、対象となる (ink_cycle, tape, stapler, stapler_temp) に対して 重複なく1件だけ該当する。
	5.	各ファイルとも空ではなく、最低1行のデータが存在し、正しく DataFrame 化できる。

1.3. Parquet 読み込み・GLUE 計算に関する正常系
	1.	存在する Parquet ファイルが指定された ruler_data/<filename>.parquet にあり、読み込める。
	2.	Parquet 内の必要カラム (sheet, stripe, ink, ruler など) がすべて存在し、型が適合する。
	3.	(sheet, stripe, ink) -> ruler の平滑化ができる (groupby→mean)
	4.	ink_cycle_val が [min(ink_list), max(ink_list)] の範囲内にある場合にのみ補間を行い、glue が計算できる。
	5.	ある程度のデータ量があっても処理時間内に計算が完了する(性能面含む)。

1.4. 出力処理に関する正常系
	1.	(sheet, stripe) 単位の CSV (sheet_stripe.csv) が正しく出力される。
	2.	(sheet) 単位で集約した CSV (sheet.csv) が正しく出力される。
	3.	出力先のディレクトリ (output_pen/<ID> や output_book/<ID>) が自動的に作成され、複数のIDに対して並列に出力される。
	4.	補間範囲内の場合、(sheet, stripe) 全組み合わせについて glue_fast, glue_lna の数値が算出される(空でない)。
	5.	IDごとにサブフォルダが作られる → ID=0,1,2,… など、それぞれに sheet_stripe.csv & sheet.csv が格納される。

1.5. テスト単位例 (正常系)
	•	test_loader.py:
	•	Excel/CSV の各カラム・型が想定通り読み込まれる
	•	マージ後の DataFrame カラム名が期待通り (e.g. tape, stapler_temp になっている)
	•	test_calculator.py:
	•	Parquet の読み込み → (sheet, stripe, ink) 分割 → 補間 → GLUE 計算が正常に行われる
	•	process_two_parquet_files で filename_fast, filename_lna 両方存在し、glue_fast, glue_lna が計算される
	•	test_schema.py:
	•	pandera で各カラムのスキーマが満たされているか(不要カラムがないか、型が合っているか)

2. 異常系 (Error Path)

2.1. コマンドライン引数に関する異常系
	1.	--mode に "pen" / "book" 以外を指定 (e.g. --mode xyz ) → フレームサイズが0になり、print(f"不明なmode: {args.mode}") で sys.exit(1)。
	2.	--mode に2回指定して矛盾があったり不明 (argparse で許容されない → Argparse エラー)
	3.	--input_data_path を指定したが、存在しないディレクトリ → ファイルが見つからず後続でエラー。

2.2. Excel / CSV / LUT 読み込みに関する異常系
	1.	quotation_test.xlsx が存在しない → ファイル読み込み時に FileNotFoundError (polarsが投げる)
	2.	シート名Sheet1が存在しない → polars.read_excel() がエラー
	3.	想定カラムが1つ以上欠落 (e.g. "ID" 列が無い) → polars の型指定が合わずエラー or KeyError
	4.	カラム型が違う (e.g. "ink_cycle" カラムが文字列で入っている) → 読み込み時に polars がエラー
	5.	condition_table_test.csv が存在しない
	6.	condition_table_test.csv のカラムが不正 (カラムが足りない、名前が違う、型が違う など)
	7.	ruler_pencilcase_table_test.csv が無い または readmode に “Fast”/“LNA” 以外が含まれる → RuntimeError("ruler_pencilcase_tableに未知のreadmodeあり")
	8.	LUT 重複 (同じ (inkcycle_start, inkcycle_end, tape, stapler, stapler_temp, readmode) に該当するレコードが2件以上) → RuntimeError("重複ファイル...")
	9.	LUT が見つからない → None が返り、後続で Parquet が読めずに空データになる

2.3. Parquet 読み込み・GLUE 計算に関する異常系
	1.	ファイル名が None → process_two_parquet_files 内で空データ扱い(df_empty)になる → 最終的に (sheet, stripe) の結果が0行になる。
	2.	存在しないファイル名 (FileNotFoundError) → “Parquetファイルが見つかりません”
	3.	Parquet に必要なカラムが無い (e.g. "ruler" カラムが無い) → groupby 時に KeyError
	4.	ink_cycle_val が [min(ink_list), max(ink_list)] の範囲外 (外挿を許容しない方針) → 補間が行われず continue → 最終的に0行になる
	5.	複数の ID で大量の parquet 読み込みが起こり、メモリが足りない → メモリ不足 (システムエラー)

2.4. 出力処理に関する異常系
	1.	出力先ディレクトリに書き込み権限が無い → PermissionError
	2.	(sheet, stripe) CSV 書き込み時にIOエラー → CSV書き込み失敗
	3.	IDごとに出力ディレクトリ作成が失敗 → OSレベルのIOエラー

2.5. テスト単位例 (異常系)
	•	Loader 側
	•	Excel/CSV ファイルが無い → FileNotFound
	•	カラム名や型が不正 → polars がエラーをスロー
	•	LUT に未知の readmode → RuntimeError
	•	LUT に該当データが無い or 重複 → RuntimeError
	•	Calculator 側
	•	Parquet ファイルが無い → FileNotFoundError
	•	Parquet 必要カラムが無い → groupby 時に KeyError
	•	ink_cycle_val が補間範囲外 → 結果 0 行
	•	Schema テスト (pandera)
	•	スキーマ不一致 (列が足りない、型が違う、余計な列がある etc.) → pandera の ValidationError
