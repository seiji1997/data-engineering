

import pytest
import polars as pl
from pathlib import Path
from unittest.mock import patch, mock_open
from io import StringIO, BytesIO
import yaml

import pandera as pa
from pandera.typing import SchemaModel, Series

# =====================================================================
# (A) Config クラス (loader.py) を想定
# =====================================================================
from loader import Config, Loader

# シナリオ1: 正常 (全キーあり, 型OK)
FAKE_YAML_DATA_NORMAL = {
    "input_path": {
        "quotation_path":  "fake_quotation.csv",
        "condition_table": "fake_condition.csv",
        "lut_table_path":  "fake_lut.csv",
        "ruler_data_path": "fake_ruler_data",
    },
    "frame_size": {
        "pen":  10000,
        "book": 20000
    },
    "readmode": {
        "ber1": "bigmac",
        "ber2": "drink"
    },
    "output_path": "fake_output"
}

# シナリオ2: 欠落キー (例: frame_size が無い)
FAKE_YAML_DATA_MISSING = {
    "input_path": {
        "quotation_path":  "fake_quotation.csv",
        "condition_table": "fake_condition.csv"
    },
    "readmode": {
        "ber1": "bigmac",
        "ber2": "drink"
    },
    "output_path": "fake_output"
}

# シナリオ3: 型不正 (pen が文字列)
FAKE_YAML_DATA_WRONG_TYPE = {
    "input_path": {
        "quotation_path":  "fake_quotation.csv",
        "condition_table": "fake_condition.csv",
        "ruler_data_path": "fake_ruler_data"
    },
    "frame_size": {
        "pen":  "one two three",
        "book": 20000
    },
    "readmode": {
        "ber1": "bigmac",
        "ber2": "drink"
    },
    "output_path": "fake_output"
}


def test_config_normal():
    """
    正常系:
     - すべてのキーがそろい、型も期待通り
     - 生成された config が想定値と一致するか確認
    """
    with patch("pathlib.Path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data="dummy")), \
         patch("yaml.safe_load", return_value=FAKE_YAML_DATA_NORMAL):
        c = Config.from_yaml("dummy_config.yaml")
        # 想定どおりの値をチェック
        assert c.quotation_path  == "fake_quotation.csv"
        assert c.condition_table == "fake_condition.csv"
        assert c.lut_table_path  == "fake_lut.csv"
        assert c.ruler_data_path == "fake_ruler_data"
        assert c.pen  == 10000
        assert c.book == 20000
        assert c.readmode["ber1"] == "bigmac"
        assert c.readmode["ber2"] == "drink"
        assert c.output_path == "fake_output"


def test_config_missing_key():
    """
    異常系(キー欠落):
     - frame_size が無い
     - KeyError を期待
    """
    with patch("pathlib.Path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data="dummy")), \
         patch("yaml.safe_load", return_value=FAKE_YAML_DATA_MISSING), \
         pytest.raises(KeyError):
        Config.from_yaml("dummy_config.yaml")


def test_config_wrong_type():
    """
    異常系(型不正):
     - pen が文字列
     - TypeError を期待
    """
    with patch("pathlib.Path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data="dummy")), \
         patch("yaml.safe_load", return_value=FAKE_YAML_DATA_WRONG_TYPE), \
         pytest.raises(TypeError):
        Config.from_yaml("dummy_config.yaml")


# =====================================================================
# (B) Pandera スキーマ (SchemaModel) => QuotationSchema, ConditionSchema
# =====================================================================
class QuotationSchema(SchemaModel):
    """
    Quotation 用のカラムを定義 (リネーム後のみ)
    """
    ID: Series[int] = pa.Field()
    testid: Series[int] = pa.Field()
    exam_number: Series[int] = pa.Field()
    buddy: Series[int] = pa.Field()
    stapler: Series[int] = pa.Field()
    tracking_marker: Series[float] = pa.Field()
    read_crinkle: Series[float] = pa.Field()
    stapler_time: Series[int] = pa.Field()
    tape: Series[str] = pa.Field()
    tape_temp: Series[float] = pa.Field()
    ink_cycle: Series[float] = pa.Field()

    class Config:
        strict = True


class ConditionSchema(SchemaModel):
    """
    Condition 用
    """
    scotch_temp: Series[int] = pa.Field()
    scotch_time: Series[str] = pa.Field()
    tape: Series[int] = pa.Field()
    stapler_temp: Series[float] = pa.Field()

    class Config:
        strict = True


# =====================================================================
# (C) Loader の CSV テスト
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
    Loader(config=None) を返すフィクスチャ
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

def test_load_quotation_csv_normal(mock_loader):
    """
    Loader.load_quotation_csv => 正常 CSV
    """
    with patch("polars.read_csv", side_effect=side_effect_read_csv):
        df = mock_loader.load_quotation_csv()
        assert df.shape == (2, 11)
        pdf = df.to_pandas()
        QuotationSchema.validate(pdf)


# =====================================================================
# (D) CSV 複数シナリオ => Parametrize
# =====================================================================
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
    factory for polars.read_csv => Param CSV
    """
    def _side_effect_read_csv(file_path, **kwargs):
        return pl.read_csv(StringIO(csv_text))
    return _side_effect_read_csv

import pandera.errors

@pytest.mark.parametrize("csv_text, expected_fail", [
    (FAKE_QUOTATION_CSV, False),           # 正常
    (FAKE_QUOTATION_CSV_MISSING_COL, True),
    (FAKE_QUOTATION_CSV_WRONG_TYPE,   True),
    (FAKE_QUOTATION_CSV_EXTRA_COL,    True),
])
def test_load_quotation_csv_scenarios(mock_loader, csv_text, expected_fail):
    """
    4シナリオ (正常/欠落/型不正/余分カラム)
    => Pandera => QuotationSchema
    """
    with patch("polars.read_csv", side_effect=make_side_effect_csv(csv_text)):
        df = mock_loader.load_quotation_csv()
        pdf = df.to_pandas()
        if expected_fail:
            with pytest.raises(pandera.errors.SchemaError):
                QuotationSchema.validate(pdf)
        else:
            QuotationSchema.validate(pdf)


# =====================================================================
# (E) Parquet 版 mock
# =====================================================================
def make_fake_parquet(valid: bool) -> bytes:
    """
    Polars DF => write_parquet => BytesIO
    if valid=False => read_crinkle="abc"
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
            "read_crinkle":["abc"],
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
    Parquet 正常データ => PanderaでOK
    """
    data_bytes = make_fake_parquet(valid=True)
    with patch("polars.read_parquet", side_effect=make_side_effect_parquet(data_bytes)):
        df = mock_loader.load_fbc_parquet(Path("fake.parquet"))
        pdf = df.to_pandas()
        QuotationSchema.validate(pdf)

def test_load_quotation_parquet_invalid(mock_loader):
    """
    Parquet 不正データ => read_crinkleが"abc" => SchemaError
    """
    data_bytes = make_fake_parquet(valid=False)
    with patch("polars.read_parquet", side_effect=make_side_effect_parquet(data_bytes)):
        df = mock_loader.load_fbc_parquet(Path("fake.parquet"))
        pdf = df.to_pandas()
        with pytest.raises(pa.errors.SchemaError):
            QuotationSchema.validate(pdf)