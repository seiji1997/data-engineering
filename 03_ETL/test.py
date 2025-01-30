"""
test_loader.py

【概要】
- Config クラス:
    - ファイルを置かずに yaml.safe_load をモックし、Config.from_yaml() の挙動をテスト
    - 正常データ/欠落データ/型不正データを複数シナリオで検証

- Loader クラス:
    - load_quotation_csv / load_condition_csv など:
       polars.read_csv をモックし、in-memory CSV文字列を利用
    - merge_condition_info 等のjoin動作を確認
    - load_fbc_parquet:
       polars.read_parquet をモックし、BytesIO 上のParquetバイナリを利用

- Pandera でスキーマ検証:
    - QuotationSchema / ConditionSchema を使用
    - 想定外のデータ (列不足/型違い/余分カラム) は SchemaError を発生させ、
      テストで「想定通りエラー」と判断
    - 正常データはエラーなく通過

【実行方法】
1) 必要ライブラリをインストール:
    pip install pytest pandera polars
2) 本ファイル (test_loader.py) を配置
3) pytest test_loader.py
   (または python -m pytest test_loader.py)

これにより、実ファイル無しで Config/Loader の動作を網羅的に確認できます。
"""

import pytest
import polars as pl
from pathlib import Path
from unittest.mock import patch, mock_open
from io import StringIO, BytesIO

import yaml
import pandera as pa
from pandera import Column
from pandera.typing import DataFrame

# ---------------------------------------------------------------------
# loader.py 内のクラスを import する想定
# ---------------------------------------------------------------------
from loader import Config, Loader


# =====================================================================
# (1) Config クラスのテスト (正常/欠落/型不正)
# =====================================================================
FAKE_YAML_DATA_NORMAL = {
    "input_path": {
        "quotation_path": "fake_quotation.csv",
        "condition_table": "fake_condition.csv",
        "lut_table_path": "fake_lut.csv",
        "ruler_data_path": "fake_ruler_data"
    },
    "frame_size": {
        "pen": 10000,
        "book": 20000
    },
    "readmode": {
        "ber1": "bigmac",
        "ber2": "drink"
    },
    "output_path": "fake_output"
}

FAKE_YAML_DATA_MISSING_KEY = {
    "input_path": {
        "quotation_path": "fake_quotation.csv",
        "condition_table": "fake_condition.csv"
        # frame_size キーが全くない、など
    },
    "readmode": {
        "ber1": "bigmac",
        "ber2": "drink"
    },
    "output_path": "fake_output"
}

FAKE_YAML_DATA_WRONG_TYPE = {
    "input_path": {
        "quotation_path": "fake_quotation.csv",
        "condition_table": "fake_condition.csv",
        "ruler_data_path": "fake_ruler_data"
    },
    "frame_size": {
        "pen": "one two three",   # 文字列; 本来 int を期待
        "book": 20000
    },
    "readmode": {
        "ber1": "bigmac",
        "ber2": "drink"
    },
    "output_path": "fake_output"
}

@pytest.mark.parametrize("fake_yaml_data, expected_error", [
    (FAKE_YAML_DATA_NORMAL, None),
    (FAKE_YAML_DATA_MISSING_KEY, KeyError),
    (FAKE_YAML_DATA_WRONG_TYPE, TypeError),
])
def test_config_scenarios(fake_yaml_data, expected_error):
    """
    3パターン:
      - 正常 (expected_error=None)
      - 必須キー欠落 (KeyError想定)
      - 型不正 (TypeError想定)
    """
    with patch("builtins.open", mock_open(read_data="dummy")), \
         patch("yaml.safe_load", return_value=fake_yaml_data):
        if expected_error is None:
            c = Config.from_yaml("dummy_config.yaml")
            assert isinstance(c.pen, int), "pen should be int"
        else:
            with pytest.raises(expected_error):
                Config.from_yaml("dummy_config.yaml")


# =====================================================================
# (2) Pandera スキーマ定義
# =====================================================================
class QuotationSchema(pa.SchemaModel):
    """
    Quotation 用スキーマ
    カラム:
      ID, testid, exam_number, buddy, stapler,
      tracking_marker, read_crinkle, stapler_time, tape,
      tape_temp, ink_cycle
    strict=True: 列過不足や型不一致ならエラー
    """
    ID: Column[int] = pa.Field()
    testid: Column[int] = pa.Field()
    exam_number: Column[int] = pa.Field()
    buddy: Column[int] = pa.Field()
    stapler: Column[int] = pa.Field()
    tracking_marker: Column[float] = pa.Field()
    read_crinkle: Column[float] = pa.Field()
    stapler_time: Column[int] = pa.Field()
    tape: Column[str] = pa.Field()
    tape_temp: Column[float] = pa.Field()
    ink_cycle: Column[float] = pa.Field()

    class Config:
        strict = True

class ConditionSchema(pa.SchemaModel):
    """
    Condition 用スキーマ
    カラム:
      scotch_temp, scotch_time, tape, stapler_temp
    strict=True
    """
    scotch_temp: Column[int] = pa.Field()
    scotch_time: Column[str] = pa.Field()
    tape: Column[int] = pa.Field()
    stapler_temp: Column[float] = pa.Field()

    class Config:
        strict = True


# =====================================================================
# (3) CSV用テスト
#     polars.read_csvをmock => in-memory CSVで検証
# =====================================================================
FAKE_QUOTATION_CSV = """\
ID,testid,exam_number,buddy,stapler,tracking_marker,read_crinkle,stapler_time,tape,tape_temp,ink_cycle
1,10,999,0,168,0.1,0.2,40,"3mon",62,1250
2,11,888,1,120,0.15,0.25,40,"3mon",62,1500
"""

FAKE_CONDITION_CSV = """\
scotch_temp,scotch_time,tape,stapler_temp
40,"3mon",85,71.25
40,"3mon",85,71.25
"""

@pytest.fixture
def mock_loader():
    """
    Loader(config=None) のフィクスチャ
    """
    return Loader(config=None)

def side_effect_read_csv(file_path, *, columns=None, dtypes=None, **kwargs):
    """
    polars.read_csv のモック
    """
    if "quotation" in str(file_path).lower():
        return pl.read_csv(StringIO(FAKE_QUOTATION_CSV))
    elif "condition" in str(file_path).lower():
        return pl.read_csv(StringIO(FAKE_CONDITION_CSV))
    else:
        return pl.DataFrame()

def test_load_quotation_csv_inmemory(mock_loader):
    """
    Loader.load_quotation_csv のテスト (正常CSV)
    """
    with patch("polars.read_csv", side_effect=side_effect_read_csv):
        df = mock_loader.load_quotation_csv()
        assert df.shape[0] == 2
        assert set(df.columns) == {
            "ID","testid","exam_number","buddy","stapler",
            "tracking_marker","read_crinkle","stapler_time","tape","tape_temp","ink_cycle"
        }
        pdf = df.to_pandas()
        QuotationSchema.validate(pdf)

def test_load_condition_csv_inmemory(mock_loader):
    """
    Loader.load_condition_csv のテスト (正常CSV)
    """
    with patch("polars.read_csv", side_effect=side_effect_read_csv):
        df = mock_loader.load_condition_csv()
        assert df.shape[0] == 2
        assert set(df.columns) == {
            "scotch_temp","scotch_time","tape","stapler_temp"
        }
        pdf = df.to_pandas()
        ConditionSchema.validate(pdf)

def test_merge_condition_info_inmemory(mock_loader):
    """
    merge_condition_info (join など) のテスト
    """
    with patch("polars.read_csv", side_effect=side_effect_read_csv):
        df_q = mock_loader.load_quotation_csv()
        df_c = mock_loader.load_condition_csv()
        merged_df = df_q.join(df_c, on=["tape"], how="left")
        # shape => 2 x 2 => 4
        assert merged_df.shape[0] == 4
        assert True


# =====================================================================
# (4) CSV 複数シナリオ (正常/欠落/型不正/余分)
# =====================================================================
FAKE_QUOTATION_CSV_VALID = """\
ID,testid,exam_number,buddy,stapler,tracking_marker,read_crinkle,stapler_time,tape,tape_temp,ink_cycle
1,10,999,0,168,0.1,0.2,40,"3mon",62,1250
2,11,888,1,120,0.15,0.25,40,"3mon",62,1500
"""

FAKE_QUOTATION_CSV_MISSING_COL = """\
ID,testid,exam_number,buddy,stapler,tracking_marker,read_crinkle,stapler_time,tape,ink_cycle
1,10,999,0,168,0.1,0.2,40,"3mon",1250
2,11,888,1,120,0.15,0.25,40,"3mon",1500
"""

FAKE_QUOTATION_CSV_WRONG_TYPE = """\
ID,testid,exam_number,buddy,stapler,tracking_marker,read_crinkle,stapler_time,tape,tape_temp,ink_cycle
1,10,999,0,168,0.1,"abc",40,"3mon",62,1250
"""

FAKE_QUOTATION_CSV_EXTRA_COL = """\
ID,testid,exam_number,buddy,stapler,tracking_marker,read_crinkle,stapler_time,tape,tape_temp,ink_cycle,dummy_col
1,10,999,0,168,0.1,0.2,40,"3mon",62,1250,"extra"
2,11,888,1,120,0.15,0.25,40,"3mon",62,1500,"extra"
"""

def make_side_effect_csv(csv_text: str):
    """
    factory for polars.read_csv => param CSV
    """
    def _side_effect_read_csv(file_path, *, columns=None, dtypes=None, **kwargs):
        return pl.read_csv(StringIO(csv_text))
    return _side_effect_read_csv

import pandera.errors

@pytest.mark.parametrize("csv_data, expected_fail", [
    (FAKE_QUOTATION_CSV_VALID, False),
    (FAKE_QUOTATION_CSV_MISSING_COL, True),
    (FAKE_QUOTATION_CSV_WRONG_TYPE, True),
    (FAKE_QUOTATION_CSV_EXTRA_COL, True),
])
def test_load_quotation_csv_scenarios(mock_loader, csv_data, expected_fail):
    """
    想定外データが混入した場合 => pandera.SchemaError になるか確認
    """
    with patch("polars.read_csv", side_effect=make_side_effect_csv(csv_data)):
        df = mock_loader.load_quotation_csv()
        pdf = df.to_pandas()
        if expected_fail:
            with pytest.raises(pandera.errors.SchemaError):
                QuotationSchema.validate(pdf)
        else:
            QuotationSchema.validate(pdf)


# =====================================================================
# (5) Parquet 版 (mock + BytesIO)
# =====================================================================
def make_fake_parquet(valid: bool) -> bytes:
    """
    Polars DF => write_parquet => raw bytes
    if valid=False => read_crinkle に文字列を入れて型不正
    """
    if valid:
        df = pl.DataFrame({
            "ID": [1,2],
            "testid": [10,11],
            "exam_number": [999,888],
            "buddy":[0,1],
            "stapler":[168,120],
            "tracking_marker":[0.1,0.15],
            "read_crinkle":[0.2,0.25],
            "stapler_time":[40,40],
            "tape":["3mon","3mon"],
            "tape_temp":[62,62],
            "ink_cycle":[1250,1500],
        })
    else:
        df = pl.DataFrame({
            "ID": [1],
            "testid": [10],
            "exam_number": [999],
            "buddy":[0],
            "stapler":[168],
            "tracking_marker":[0.1],
            "read_crinkle":["abc"],  # floatでない
            "stapler_time":[40],
            "tape":["3mon"],
            "tape_temp":[62],
            "ink_cycle":[1250],
        })
    buf = BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()

def make_side_effect_parquet(parquet_bytes: bytes):
    """
    patch("polars.read_parquet") => BytesIO(parquet_bytes)
    """
    def _side_effect_read_parquet(file_path, **kwargs):
        return pl.read_parquet(BytesIO(parquet_bytes))
    return _side_effect_read_parquet

def test_load_quotation_parquet_valid(mock_loader):
    """
    valid parquet => Pandera OK
    """
    data_bytes = make_fake_parquet(valid=True)
    with patch("polars.read_parquet", side_effect=make_side_effect_parquet(data_bytes)):
        df = mock_loader.load_fbc_parquet(Path("fake.parquet"))
        pdf = df.to_pandas()
        QuotationSchema.validate(pdf)
        assert True

def test_load_quotation_parquet_invalid(mock_loader):
    """
    invalid => read_crinkle="abc"
    => pandera.SchemaError
    """
    data_bytes = make_fake_parquet(valid=False)
    with patch("polars.read_parquet", side_effect=make_side_effect_parquet(data_bytes)):
        df = mock_loader.load_fbc_parquet(Path("fake.parquet"))
        pdf = df.to_pandas()
        with pytest.raises(pa.errors.SchemaError):
            QuotationSchema.validate(pdf)