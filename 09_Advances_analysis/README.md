## constants.py

```python
"""MDM事前バリデーションで使用する定数とSnowflakeオブジェクト定義を管理する。

操作種別、論理Master種別、入力項目名、物理カラム名、チェックIDを
一元的に定義し、検証ロジックから設定値を分離する。"""

from typing import Final

# ============================================================
# Operations
# ============================================================

OP_INSERT: Final[str] = "I"
OP_UPDATE: Final[str] = "U"
OP_DELETE: Final[str] = "D"
SUPPORTED_OPERATIONS: Final[set[str]] = {
    OP_INSERT,
    OP_UPDATE,
    OP_DELETE,
}

# Update時の値の扱い:
# "-" = 変更なし。複合参照チェックではDB上の現行値を使用する。
# ""  = 明示クリア。DB上の現行値では補完しない。
NO_UPDATE_TOKEN: Final[str] = "-"

# ============================================================
# Logical master types
# ============================================================

__SEM_MASTER_A_UPPER__: Final[str] = '__PHY_MASTER_TYPE_A__'
__SEM_MASTER_B_UPPER__: Final[str] = '__PHY_MASTER_TYPE_B__'
SUPPORTED_MASTER_TYPES: Final[set[str]] = {
    __SEM_MASTER_A_UPPER__,
    __SEM_MASTER_B_UPPER__,
}

# ============================================================
# Snowflake master objects
#
# 大文字小文字を区別する識別子を使用する場合は、
# Snowflakeの識別子規則に従ってダブルクォートを含めて定義する。
# ============================================================

MASTER_DATABASE: Final[str] = '__PHY_MASTER_DB__'
MASTER_SCHEMA: Final[str] = '__PHY_MASTER_SCHEMA__'
__SEM_MASTER_A_UPPER___TABLE_NAME: Final[str] = '__PHY_MA_TABLE__'
__SEM_MASTER_B_UPPER___TABLE_NAME: Final[str] = '__PHY_MB_TABLE__'

__SEM_MASTER_A_UPPER___TABLE: Final[str] = (
    f"{MASTER_DATABASE}.{MASTER_SCHEMA}.{__SEM_MASTER_A_UPPER___TABLE_NAME}"
)
__SEM_MASTER_B_UPPER___TABLE: Final[str] = (
    f"{MASTER_DATABASE}.{MASTER_SCHEMA}.{__SEM_MASTER_B_UPPER___TABLE_NAME}"
)

# ============================================================
# Business key columns
# ============================================================

__SEM_MASTER_A_UPPER___ID_COLUMN: Final[str] = '__PHY_MA_ID_COLUMN__'
__SEM_MASTER_B_UPPER___ID_COLUMN: Final[str] = '__PHY_MB_ID_COLUMN__'

# ============================================================
# CSV / Payload input columns
#
# Streamlitから受け取るrecord辞書のキーを定義する。
# DBの物理カラム名とは独立して管理する。
# ============================================================

__SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___INPUT_COLUMN: Final[str] = '__PHY_MB_TO_MA_INPUT_COLUMN__'

__SEM_REF_A_UPPER___INPUT_COLUMN: Final[str] = '__PHY_RA_INPUT_COLUMN__'
__SEM_REF_B_UPPER___INPUT_COLUMN: Final[str] = '__PHY_RB_INPUT_COLUMN__'
__SEM_REF_C_UPPER___INPUT_COLUMN: Final[str] = '__PHY_RC_INPUT_COLUMN__'
__SEM_REF_D_UPPER___INPUT_COLUMN: Final[str] = '__PHY_RD_INPUT_COLUMN__'
__SEM_REF_E_UPPER___INPUT_COLUMN: Final[str] = '__PHY_RE_INPUT_COLUMN__'

# ============================================================
# Current record columns
#
# Update時にDB上の現行値を取得するための物理カラムを定義する。
# ============================================================

__SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___CURRENT_COLUMN: Final[str] = '__PHY_MB_TO_MA_CURRENT_COLUMN__'

__SEM_REF_A_UPPER___CURRENT_COLUMN: Final[str] = '__PHY_RA_CURRENT_COLUMN__'
__SEM_REF_B_UPPER___CURRENT_COLUMN: Final[str] = '__PHY_RB_CURRENT_COLUMN__'
__SEM_REF_C_UPPER___CURRENT_COLUMN: Final[str] = '__PHY_RC_CURRENT_COLUMN__'
__SEM_REF_D_UPPER___CURRENT_COLUMN: Final[str] = '__PHY_RD_CURRENT_COLUMN__'
__SEM_REF_E_UPPER___CURRENT_COLUMN: Final[str] = '__PHY_RE_CURRENT_COLUMN__'

# ============================================================
# Reference target columns
#
# 参照整合性チェックで検索対象とする__SEM_MASTER_A_UPPER__の物理カラムを定義する。
# Current record columnsと同一の物理カラムを指定してもよい。
# ============================================================

__SEM_REF_A_UPPER___TARGET_COLUMN: Final[str] = '__PHY_RA_TARGET_COLUMN__'
__SEM_REF_B_UPPER___TARGET_COLUMN: Final[str] = '__PHY_RB_TARGET_COLUMN__'
__SEM_REF_C_UPPER___TARGET_COLUMN: Final[str] = '__PHY_RC_TARGET_COLUMN__'
__SEM_REF_D_UPPER___TARGET_COLUMN: Final[str] = '__PHY_RD_TARGET_COLUMN__'
__SEM_REF_E_UPPER___TARGET_COLUMN: Final[str] = '__PHY_RE_TARGET_COLUMN__'

# ============================================================
# Check IDs
# ============================================================

ID001: Final[str] = "ID001"
ID002: Final[str] = "ID002"
REF101: Final[str] = "REF101"
REF201: Final[str] = "REF201"
REF202: Final[str] = "REF202"
REF203: Final[str] = "REF203"
REF204: Final[str] = "REF204"
REF205: Final[str] = "REF205"
```

## main.py

```python
"""MDM申請データの事前バリデーションを実行するSnowflake Stored Procedureの
エントリーポイントを定義する。

Operation、ファイル名、Payloadを受け取り、Check Registryで定義された検証を
順番に実行してJSON形式の結果を返す。"""

import json
from typing import Any

from constants import (
    OP_UPDATE,
    SUPPORTED_MASTER_TYPES,
    SUPPORTED_OPERATIONS,
)
from checks import (
    get_checks,
    get_current_record,
)


def _normalize_operation(operation: str) -> str:
    """CRUD操作種別を正規化し、サポート対象かを検証する。
    
    Args:
        operation: 操作種別。I / U / D のいずれかを指定する。
    
    Returns:
        大文字へ正規化済みの操作種別。
    
    Raises:
        ValueError: I / U / D 以外の操作種別が指定された場合。
    """
    op = (operation or "").strip().upper()

    if op not in SUPPORTED_OPERATIONS:
        raise ValueError(
            f"Unsupported operation: {operation}"
        )

    return op


def _parse_payload(
    payload_json: str | dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    """リクエストPayloadを解析し、トップレベル構造を検証する。
    
    Args:
        payload_json:
            master_type と records を含むJSON文字列、
            または既にdictへ変換済みのPayload。
    
    Returns:
        検証済みのmaster_typeとrecords一覧のタプル。
    
    Raises:
        ValueError:
            Payloadが未指定、dict形式でない、未対応master_type、
            またはrecordsが配列でない場合。
        json.JSONDecodeError:
            JSON文字列として解釈できない場合。
    """
    if payload_json is None:
        raise ValueError(
            "P_PAYLOAD_JSON is empty."
        )

    payload = (
        payload_json
        if isinstance(payload_json, dict)
        else json.loads(str(payload_json))
    )

    if not isinstance(payload, dict):
        raise ValueError(
            "Payload must be a JSON object."
        )

    master_type = str(
        payload.get("master_type") or ""
    ).strip().upper()

    if master_type not in SUPPORTED_MASTER_TYPES:
        raise ValueError(
            f"Unsupported master_type: {master_type}"
        )

    records = payload.get("records")

    if not isinstance(records, list):
        raise ValueError(
            "Payload.records must be an array."
        )

    return master_type, records


def validate_request(
    session: Any,
    p_operation: str,
    p_file_name: str,
    p_payload_json: str,
) -> str:
    """1ファイル分の申請レコードに対して事前バリデーションを実行する。
    
    Snowflakeからsessionが自動注入され、呼出元からはOperation、
    ファイル名、Payload JSONのみを渡す。
    
    Updateでは、対象レコードの現行値を1レコードにつき1回だけ取得し、
    同一レコード内の複数チェックで再利用する。
    これにより、複合参照整合性チェックで「-（変更なし）」が含まれる場合に、
    現行値を補完して最終的な組合せを検証できる。
    
    Args:
        session:
            Snowflakeから自動注入されるSnowpark Session。
        p_operation:
            操作種別。I（登録）/ U（更新）/ D（削除）。
        p_file_name:
            アップロード元CSVファイル名。結果追跡用にそのまま返却する。
        p_payload_json:
            master_type と records を含むPayload JSON。
    
    Returns:
        ファイル情報、master_type、operation、各チェック結果を含むJSON文字列。
    
    Raises:
        ValueError:
            Operation、Payload、row_no、record構造などが不正な場合。
        json.JSONDecodeError:
            p_payload_jsonが正しいJSONでない場合。
    """
    operation = _normalize_operation(
        p_operation
    )

    master_type, records = _parse_payload(
        p_payload_json
    )

    checks = get_checks(
        master_type,
        operation,
    )

    results: list[dict[str, Any]] = []

    for item in records:
        if not isinstance(item, dict):
            raise ValueError(
                "Each records[] item must be a JSON object."
            )

        row_no = item.get("row_no")
        record = item.get("record")

        if row_no is None:
            raise ValueError(
                "row_no is required."
            )

        if not isinstance(record, dict):
            raise ValueError(
                "record must be a JSON object."
            )

        current_record = (
            get_current_record(
                session,
                master_type,
                record,
            )
            if operation == OP_UPDATE
            else None
        )

        for check in checks:
            results.append(
                check(
                    session=session,
                    operation=operation,
                    row_no=int(row_no),
                    record=record,
                    current_record=current_record,
                )
            )

    return json.dumps(
        {
            "file_name": p_file_name,
            "master_type": master_type,
            "operation": operation,
            "results": results,
        },
        ensure_ascii=False,
    )
```

## checks.py

```python
"""MDM申請データに対する業務バリデーションを定義する。

__SEM_MASTER_A_UPPER__と__SEM_MASTER_B_UPPER__のID重複・存在チェック、および参照整合性チェックを実行する。

Update時の入力値は次の規則で扱う。

- 「-」:
    変更なし。
    単一参照チェックではSKIPとし、複合参照チェックではDB上の現行値を用いて
    更新後の組合せを検証する。
- 空文字:
    明示クリア。
    参照整合性チェックではSKIPとする。
- その他の値:
    入力値を使用して参照先の存在または組合せの整合性を検証する。

実行対象のチェックと実行順序はCheck Registryで定義する。"""

from typing import Any, Callable

from constants import (
    OP_INSERT,
    OP_UPDATE,
    OP_DELETE,
    NO_UPDATE_TOKEN,
    __SEM_MASTER_A_UPPER__,
    __SEM_MASTER_B_UPPER__,
    __SEM_MASTER_A_UPPER___TABLE,
    __SEM_MASTER_B_UPPER___TABLE,
    __SEM_MASTER_A_UPPER___ID_COLUMN,
    __SEM_MASTER_B_UPPER___ID_COLUMN,
    __SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___INPUT_COLUMN,
    __SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___CURRENT_COLUMN,
    __SEM_REF_A_UPPER___INPUT_COLUMN,
    __SEM_REF_B_UPPER___INPUT_COLUMN,
    __SEM_REF_C_UPPER___INPUT_COLUMN,
    __SEM_REF_D_UPPER___INPUT_COLUMN,
    __SEM_REF_E_UPPER___INPUT_COLUMN,
    __SEM_REF_A_UPPER___CURRENT_COLUMN,
    __SEM_REF_B_UPPER___CURRENT_COLUMN,
    __SEM_REF_C_UPPER___CURRENT_COLUMN,
    __SEM_REF_D_UPPER___CURRENT_COLUMN,
    __SEM_REF_E_UPPER___CURRENT_COLUMN,
    __SEM_REF_A_UPPER___TARGET_COLUMN,
    __SEM_REF_B_UPPER___TARGET_COLUMN,
    __SEM_REF_C_UPPER___TARGET_COLUMN,
    __SEM_REF_D_UPPER___TARGET_COLUMN,
    __SEM_REF_E_UPPER___TARGET_COLUMN,
    ID001,
    ID002,
    REF101,
    REF201,
    REF202,
    REF203,
    REF204,
    REF205,
)
from result_builder import (
    build_ok_result,
    build_ng_result,
    build_skip_result,
)

CheckFunction = Callable[..., dict[str, Any]]


def _split_object_name(name: str) -> tuple[str, str, str]:
    """Snowflakeの3階層オブジェクト名を結果表示用に分解する。
    
    Args:
        name:
            database.schema.object形式の完全修飾オブジェクト名。
    
    Returns:
        database、schema、objectの3要素タプル。
        3階層でない場合はdatabaseとschemaを空文字として返す。
    """
    parts = name.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return "", "", name


def _text(
    data: dict[str, Any] | None,
    column: str,
) -> str:
    """辞書内の値を文字列として安全に取得する。
    
    Noneやキー未存在は空文字へ正規化し、
    文字列化した後に前後空白を除去する。
    
    Args:
        data:
            入力レコードまたはDB上の現行レコードの辞書。Noneも許容する。
        column:
            取得対象の辞書キー。
    
    Returns:
        正規化済み文字列。値が存在しない場合は空文字。
    """
    if not data:
        return ""
    value = data.get(column)
    return "" if value is None else str(value).strip()


def _row_text(row: Any, column: str) -> str:
    """Snowpark Rowの指定項目を文字列として正規化する。
    
    Args:
        row:
            collect()で取得したSnowpark Row。
        column:
            取得対象の物理カラム名。
    
    Returns:
        前後空白を除去した文字列。
        SQL NULLの場合は空文字。
    """
    value = row[column]
    return "" if value is None else str(value).strip()


def _exists(
    session: Any,
    sql_text: str,
    params: list[Any],
) -> bool:
    """指定SQLを実行し、1件以上のレコードが存在するかを判定する。
    
    Args:
        session:
            Snowpark Session。
        sql_text:
            バインド変数を含む存在確認SQL。
        params:
            SQLへ渡す位置パラメータ。
    
    Returns:
        1件以上存在する場合はTrue、存在しない場合はFalse。
    """
    return bool(
        session.sql(sql_text, params=params).collect()
    )


def _effective_value(
    record: dict[str, Any],
    input_column: str,
    current_record: dict[str, str] | None,
    operation: str,
) -> str:
    """複合参照整合性チェックで実際に使用する値を決定する。
    
    Update時に入力値が「-」の場合は「変更なし」を意味するため、
    DB上の現行レコードの値へ置き換える。
    空文字は明示クリアを意味するためDB上の現行値では補完しない。
    
    Args:
        record:
            申請された入力レコード。
        input_column:
            判定対象の入力項目名。
        current_record:
            DB上の現行値を入力項目名へ対応付けした辞書。
        operation:
            現在の操作種別。
    
    Returns:
        複合参照チェックで使用する実効値。
    """
    value = _text(record, input_column)

    if (
        operation == OP_UPDATE
        and value == NO_UPDATE_TOKEN
    ):
        return _text(current_record, input_column)

    return value


def get_current___SEM_MASTER_A_LOWER__(
    session: Any,
    __SEM_MASTER_A_LOWER___id: str,
) -> dict[str, str] | None:
    """Update時の検証に使用する__SEM_MASTER_A_UPPER__の現行レコードを取得する。
    
    DBの物理カラム名でDB上の現行値を取得した後、
    入力項目名をキーとする辞書として返す。
        
    Args:
        session:
            Snowpark Session。
        __SEM_MASTER_A_LOWER___id:
            取得対象__SEM_MASTER_A_UPPER__レコードの業務キー。
    
    Returns:
        入力項目名をキーとしてDB上の現行値を保持する辞書。
        対象IDが存在しない場合はNone。
    """
    rows = session.sql(
        f"""
        SELECT
            {__SEM_MASTER_A_UPPER___ID_COLUMN},
            {__SEM_REF_A_UPPER___CURRENT_COLUMN},
            {__SEM_REF_B_UPPER___CURRENT_COLUMN},
            {__SEM_REF_C_UPPER___CURRENT_COLUMN},
            {__SEM_REF_D_UPPER___CURRENT_COLUMN},
            {__SEM_REF_E_UPPER___CURRENT_COLUMN}
        FROM {__SEM_MASTER_A_UPPER___TABLE}
        WHERE {__SEM_MASTER_A_UPPER___ID_COLUMN} = ?
        LIMIT 1
        """,
        params=[__SEM_MASTER_A_LOWER___id],
    ).collect()

    if not rows:
        return None

    row = rows[0]

    return {
        __SEM_MASTER_A_UPPER___ID_COLUMN:
            _row_text(row, __SEM_MASTER_A_UPPER___ID_COLUMN),
        __SEM_REF_A_UPPER___INPUT_COLUMN:
            _row_text(row, __SEM_REF_A_UPPER___CURRENT_COLUMN),
        __SEM_REF_B_UPPER___INPUT_COLUMN:
            _row_text(row, __SEM_REF_B_UPPER___CURRENT_COLUMN),
        __SEM_REF_C_UPPER___INPUT_COLUMN:
            _row_text(row, __SEM_REF_C_UPPER___CURRENT_COLUMN),
        __SEM_REF_D_UPPER___INPUT_COLUMN:
            _row_text(row, __SEM_REF_D_UPPER___CURRENT_COLUMN),
        __SEM_REF_E_UPPER___INPUT_COLUMN:
            _row_text(row, __SEM_REF_E_UPPER___CURRENT_COLUMN),
    }


def get_current___SEM_MASTER_B_LOWER__(
    session: Any,
    __SEM_MASTER_B_LOWER___id: str,
) -> dict[str, str] | None:
    """Update時の検証に使用する__SEM_MASTER_B_UPPER__の現行レコードを取得する。
    
    __SEM_MASTER_B_UPPER__自身のIDと__SEM_MASTER_A_UPPER__参照項目を取得し、入力項目名をキーとする辞書として返す。
    
    Args:
        session:
            Snowpark Session。
        __SEM_MASTER_B_LOWER___id:
            取得対象__SEM_MASTER_B_UPPER__レコードの業務キー。
    
    Returns:
        入力項目名をキーとしてDB上の現行値を保持する辞書。
        対象IDが存在しない場合はNone。
    """
    rows = session.sql(
        f"""
        SELECT
            {__SEM_MASTER_B_UPPER___ID_COLUMN},
            {__SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___CURRENT_COLUMN}
        FROM {__SEM_MASTER_B_UPPER___TABLE}
        WHERE {__SEM_MASTER_B_UPPER___ID_COLUMN} = ?
        LIMIT 1
        """,
        params=[__SEM_MASTER_B_LOWER___id],
    ).collect()

    if not rows:
        return None

    row = rows[0]

    return {
        __SEM_MASTER_B_UPPER___ID_COLUMN:
            _row_text(row, __SEM_MASTER_B_UPPER___ID_COLUMN),
        __SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___INPUT_COLUMN:
            _row_text(row, __SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___CURRENT_COLUMN),
    }


def _id_check(
    *,
    session: Any,
    row_no: int,
    record: dict[str, Any],
    table: str,
    id_column: str,
    must_exist: bool,
) -> dict[str, Any]:
    """IDの重複または存在・一意性を件数ベースで検証する。

    判定ルール:
        Insert:
            0件     -> OK
            1件以上 -> NG

        Update / Delete:
            0件     -> NG
            1件     -> OK
            2件以上 -> NG

    Args:
        session:
            Snowpark Session。
        row_no:
            入力行番号。
        record:
            申請された入力レコード。
        table:
            検索対象のMaster。
        id_column:
            業務キー項目名。
        must_exist:
            Falseの場合はInsert用の重複チェック、
            Trueの場合はUpdate / Delete用の存在・一意性チェックを実行する。

    Returns:
        ID001またはID002の検証結果。
    """
    value = _text(record, id_column)
    db, schema, obj = _split_object_name(table)

    common = dict(
        row_no=row_no,
        record_id=value,
        check_id=ID002 if must_exist else ID001,
        input_column=id_column,
        input_value=value,
        target_database=db,
        target_schema=schema,
        target_table=obj,
        target_column=id_column,
    )

    if value == "":
        return build_ng_result(
            **common,
            message=f"{id_column} is blank.",
        )

    rows = session.sql(
        f"""
        SELECT COUNT(*) AS MATCH_COUNT
        FROM {table}
        WHERE {id_column} = ?
        """,
        params=[value],
    ).collect()

    match_count = (
        int(rows[0]["MATCH_COUNT"])
        if rows
        else 0
    )

    if must_exist:
        if match_count == 1:
            return build_ok_result(
                **common,
                message="",
            )

        if match_count == 0:
            return build_ng_result(
                **common,
                message=f"{id_column} {value} does not exist.",
            )

        return build_ng_result(
            **common,
            message=(
                f"{id_column} {value} is duplicated in master "
                f"({match_count} records)."
            ),
        )

    if match_count > 0:
        return build_ng_result(
            **common,
            message=f"{id_column} {value} already exists.",
        )

    return build_ok_result(
        **common,
        message="",
    )






def _single_reference(
    *,
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    record_id_column: str,
    input_column: str,
    target_table: str,
    target_column: str,
    check_id: str,
) -> dict[str, Any]:
    """単一項目の参照整合性を指定された参照先Masterに対して検証する。
    
    Update時の「-」は変更なしのためSKIPとする。
    Update時の空文字は明示クリアのため参照整合性としてはSKIPとする。
    Insert時に対象値が空文字の場合はNGとする。
    それ以外は指定した参照先カラムに値が存在するかを確認する。
    
    Args:
        session:
            Snowpark Session。
        operation:
            現在の操作種別。
        row_no:
            入力行番号。
        record:
            申請された入力レコード。
        record_id_column:
            対象レコードの業務キーを保持する入力項目。
        input_column:
            検証対象の参照項目。
        target_table:
            参照先の物理Master。
        target_column:
            検索対象の参照先物理カラム。
        check_id:
            呼出元へ返却する固定チェックID。
    
    Returns:
        OK / NG / SKIPのいずれかの検証結果。
    """
    record_id = _text(record, record_id_column)
    value = _text(record, input_column)

    db, schema, obj = _split_object_name(target_table)

    common = dict(
        row_no=row_no,
        record_id=record_id,
        check_id=check_id,
        input_column=input_column,
        input_value=value,
        target_database=db,
        target_schema=schema,
        target_table=obj,
        target_column=target_column,
    )

    if (
        operation == OP_UPDATE
        and value == NO_UPDATE_TOKEN
    ):
        return build_skip_result(
            **common,
            message=f"{input_column} was not updated.",
        )

    if value == "":
        if operation == OP_INSERT:
            return build_ng_result(
                **common,
                message=f"{input_column} is blank.",
            )

        return build_skip_result(
            **common,
            message=f"{input_column} was explicitly cleared.",
        )

    if _exists(
        session,
        f"""
        SELECT 1
        FROM {target_table}
        WHERE {target_column} = ?
        LIMIT 1
        """,
        [value],
    ):
        return build_ok_result(
            **common,
            message="",
        )

    return build_ng_result(
        **common,
        message=f"{input_column} {value} does not exist.",
    )


def _composite_reference(
    *,
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None,
    record_id_column: str,
    input_columns: list[str],
    target_table: str,
    target_columns: list[str],
    check_id: str,
) -> dict[str, Any]:
    """2項目または3項目の組合せ参照整合性を指定された参照先Masterに対して検証する。
    
    Update時に一部の項目が「-」の場合、
    その項目はDB上の現行値で補完して更新後の実効組合せを作成する。
    対象項目がすべて「-」の場合は変更がないためSKIPとする。
    実効値のいずれかが空文字の場合は明示クリアを含むため、
    参照整合性チェックとしてはSKIPとする。
    
    Args:
        session:
            Snowpark Session。
        operation:
            現在の操作種別。
        row_no:
            入力行番号。
        record:
            申請された入力レコード。
        current_record:
            __SEM_MASTER_A_UPPER__のDB上の現行値を入力項目名をキーとして保持する辞書。
        record_id_column:
            対象レコードの業務キー項目。
        input_columns:
            組合せを構成する2項目または3項目の入力カラム一覧。
        target_table:
            有効な組合せを保持する物理Master。
        target_columns:
            input_columnsに対応する参照先物理カラム一覧。
        check_id:
            呼出元へ返却する固定チェックID。
    
    Returns:
        DB上の現行値補完後の実効組合せに対するOK / NG / SKIP結果。
    
    Raises:
        ValueError:
            input_columnsとtarget_columnsの要素数が一致しない場合。
    """
    if len(input_columns) != len(target_columns):
        raise ValueError(
            "input_columns and target_columns length mismatch."
        )

    record_id = _text(record, record_id_column)
    raw_values = [
        _text(record, column)
        for column in input_columns
    ]

    db, schema, obj = _split_object_name(target_table)

    base = dict(
        row_no=row_no,
        record_id=record_id,
        check_id=check_id,
        input_column=",".join(input_columns),
        target_database=db,
        target_schema=schema,
        target_table=obj,
        target_column=",".join(target_columns),
    )

    if (
        operation == OP_UPDATE
        and all(
            value == NO_UPDATE_TOKEN
            for value in raw_values
        )
    ):
        return build_skip_result(
            **base,
            input_value=",".join(raw_values),
            message="Composite reference columns were not updated.",
        )

    if (
        operation == OP_UPDATE
        and any(
            value == NO_UPDATE_TOKEN
            for value in raw_values
        )
        and current_record is None
    ):
        return build_skip_result(
            **base,
            input_value=",".join(raw_values),
            message=(
                "Current record was not found; "
                "composite reference could not be completed."
            ),
        )

    effective_values = [
        _effective_value(
            record,
            column,
            current_record,
            operation,
        )
        for column in input_columns
    ]

    if (
        operation == OP_INSERT
        and any(
            value == ""
            for value in effective_values
        )
    ):
        return build_ng_result(
            **base,
            input_value=",".join(effective_values),
            message="Composite reference is incomplete.",
        )

    if (
        operation == OP_UPDATE
        and any(
            value == ""
            for value in effective_values
        )
    ):
        return build_skip_result(
            **base,
            input_value=",".join(effective_values),
            message=(
                "Composite reference contains "
                "an explicitly cleared value."
            ),
        )

    where_clause = " AND ".join(
        f"{column} = ?"
        for column in target_columns
    )

    if _exists(
        session,
        f"""
        SELECT 1
        FROM {target_table}
        WHERE {where_clause}
        LIMIT 1
        """,
        effective_values,
    ):
        return build_ok_result(
            **base,
            input_value=",".join(effective_values),
            message="",
        )

    return build_ng_result(
        **base,
        input_value=",".join(effective_values),
        message=(
            f"{' / '.join(effective_values)} "
            "does not exist."
        ),
    )


def __SEM_MASTER_A_LOWER___id_duplicate(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__のInsert時に、入力IDの一致件数が0件であることを確認する。
    
    Returns:
        ID001の検証結果。
    """
    return _id_check(
        session=session,
        row_no=row_no,
        record=record,
        table=__SEM_MASTER_A_UPPER___TABLE,
        id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        must_exist=False,
    )


def __SEM_MASTER_A_LOWER___id_existence(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__のUpdate / Delete時に、対象IDがMaster上で1件だけ存在することを確認する。
    
    Returns:
        ID002の検証結果。
    """
    return _id_check(
        session=session,
        row_no=row_no,
        record=record,
        table=__SEM_MASTER_A_UPPER___TABLE,
        id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        must_exist=True,
    )


def __SEM_MASTER_B_LOWER___id_duplicate(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_B_UPPER__のInsert時に、入力IDの一致件数が0件であることを確認する。
    
    Returns:
        ID001の検証結果。
    """
    return _id_check(
        session=session,
        row_no=row_no,
        record=record,
        table=__SEM_MASTER_B_UPPER___TABLE,
        id_column=__SEM_MASTER_B_UPPER___ID_COLUMN,
        must_exist=False,
    )


def __SEM_MASTER_B_LOWER___id_existence(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_B_UPPER__のUpdate / Delete時に、対象IDがMaster上で1件だけ存在することを確認する。
    
    Returns:
        ID002の検証結果。
    """
    return _id_check(
        session=session,
        row_no=row_no,
        record=record,
        table=__SEM_MASTER_B_UPPER___TABLE,
        id_column=__SEM_MASTER_B_UPPER___ID_COLUMN,
        must_exist=True,
    )


def __SEM_MASTER_B_LOWER___to___SEM_MASTER_A_LOWER___reference(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_B_UPPER__から__SEM_MASTER_A_UPPER__への参照整合性を検証する（REF101）。
    
    __SEM_MASTER_B_UPPER__側の参照項目に設定された値が、
    __SEM_MASTER_A_UPPER__の業務キーとして存在することを確認する。
    
    Returns:
        REF101の検証結果。
    """
    return _single_reference(
        session=session,
        operation=operation,
        row_no=row_no,
        record=record,
        record_id_column=__SEM_MASTER_B_UPPER___ID_COLUMN,
        input_column=__SEM_MASTER_B_UPPER___TO___SEM_MASTER_A_UPPER___INPUT_COLUMN,
        target_table=__SEM_MASTER_A_UPPER___TABLE,
        target_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        check_id=REF101,
    )


def __SEM_REF_A_LOWER___reference(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__の__SEM_REF_A_UPPER__を__SEM_MASTER_A_UPPER__内の参照先カラムに対して検証する（REF201）。
    
    Returns:
        REF201の検証結果。
    """
    return _single_reference(
        session=session,
        operation=operation,
        row_no=row_no,
        record=record,
        record_id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        input_column=__SEM_REF_A_UPPER___INPUT_COLUMN,
        target_table=__SEM_MASTER_A_UPPER___TABLE,
        target_column=__SEM_REF_A_UPPER___TARGET_COLUMN,
        check_id=REF201,
    )


def __SEM_REF_B_LOWER___reference(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__の__SEM_REF_B_UPPER__を__SEM_MASTER_A_UPPER__内の参照先カラムに対して検証する（REF202）。
    
    Returns:
        REF202の検証結果。
    """
    return _single_reference(
        session=session,
        operation=operation,
        row_no=row_no,
        record=record,
        record_id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        input_column=__SEM_REF_B_UPPER___INPUT_COLUMN,
        target_table=__SEM_MASTER_A_UPPER___TABLE,
        target_column=__SEM_REF_B_UPPER___TARGET_COLUMN,
        check_id=REF202,
    )


def __SEM_REF_C_LOWER___reference(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__の__SEM_REF_C_UPPER__を__SEM_MASTER_A_UPPER__内の参照先カラムに対して検証する（REF203）。
    
    Returns:
        REF203の検証結果。
    """
    return _single_reference(
        session=session,
        operation=operation,
        row_no=row_no,
        record=record,
        record_id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        input_column=__SEM_REF_C_UPPER___INPUT_COLUMN,
        target_table=__SEM_MASTER_A_UPPER___TABLE,
        target_column=__SEM_REF_C_UPPER___TARGET_COLUMN,
        check_id=REF203,
    )


def __SEM_REF_C_LOWER_____SEM_REF_D_LOWER___reference(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__の__SEM_REF_C_UPPER__ + __SEM_REF_D_UPPER__の2項目組合せを検証する（REF204）。
    
    Update時に片方が「-」の場合はDB上の現行値を補完し、
    更新後に成立する2項目の組合せが__SEM_MASTER_A_UPPER__内に存在するかを確認する。
    
    Returns:
        REF204の検証結果。
    """
    return _composite_reference(
        session=session,
        operation=operation,
        row_no=row_no,
        record=record,
        current_record=current_record,
        record_id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        input_columns=[
            __SEM_REF_C_UPPER___INPUT_COLUMN,
            __SEM_REF_D_UPPER___INPUT_COLUMN,
        ],
        target_table=__SEM_MASTER_A_UPPER___TABLE,
        target_columns=[
            __SEM_REF_C_UPPER___TARGET_COLUMN,
            __SEM_REF_D_UPPER___TARGET_COLUMN,
        ],
        check_id=REF204,
    )


def __SEM_REF_C_LOWER_____SEM_REF_D_LOWER_____SEM_REF_E_LOWER___reference(
    session: Any,
    operation: str,
    row_no: int,
    record: dict[str, Any],
    current_record: dict[str, str] | None = None,
) -> dict[str, Any]:
    """__SEM_MASTER_A_UPPER__の__SEM_REF_C_UPPER__ + __SEM_REF_D_UPPER__ + __SEM_REF_E_UPPER__の3項目組合せを検証する（REF205）。
    
    Update時に一部項目が「-」の場合はDB上の現行値を補完し、
    更新後に成立する3項目の組合せが__SEM_MASTER_A_UPPER__内に存在するかを確認する。
    
    Returns:
        REF205の検証結果。
    """
    return _composite_reference(
        session=session,
        operation=operation,
        row_no=row_no,
        record=record,
        current_record=current_record,
        record_id_column=__SEM_MASTER_A_UPPER___ID_COLUMN,
        input_columns=[
            __SEM_REF_C_UPPER___INPUT_COLUMN,
            __SEM_REF_D_UPPER___INPUT_COLUMN,
            __SEM_REF_E_UPPER___INPUT_COLUMN,
        ],
        target_table=__SEM_MASTER_A_UPPER___TABLE,
        target_columns=[
            __SEM_REF_C_UPPER___TARGET_COLUMN,
            __SEM_REF_D_UPPER___TARGET_COLUMN,
            __SEM_REF_E_UPPER___TARGET_COLUMN,
        ],
        check_id=REF205,
    )


# ============================================================
# Check Registry
#
# Master種別・操作種別ごとの実行対象チェックと実行順序を定義する。
# ============================================================

__SEM_MASTER_A_UPPER___CHECK_REGISTRY: dict[str, list[CheckFunction]] = {
    OP_INSERT: [
        __SEM_MASTER_A_LOWER___id_duplicate,
        __SEM_REF_A_LOWER___reference,
        __SEM_REF_B_LOWER___reference,
        __SEM_REF_C_LOWER___reference,
        __SEM_REF_C_LOWER_____SEM_REF_D_LOWER___reference,
        __SEM_REF_C_LOWER_____SEM_REF_D_LOWER_____SEM_REF_E_LOWER___reference,
    ],
    OP_UPDATE: [
        __SEM_MASTER_A_LOWER___id_existence,
        __SEM_REF_A_LOWER___reference,
        __SEM_REF_B_LOWER___reference,
        __SEM_REF_C_LOWER___reference,
        __SEM_REF_C_LOWER_____SEM_REF_D_LOWER___reference,
        __SEM_REF_C_LOWER_____SEM_REF_D_LOWER_____SEM_REF_E_LOWER___reference,
    ],
    OP_DELETE: [
        __SEM_MASTER_A_LOWER___id_existence,
    ],
}

__SEM_MASTER_B_UPPER___CHECK_REGISTRY: dict[str, list[CheckFunction]] = {
    OP_INSERT: [
        __SEM_MASTER_B_LOWER___id_duplicate,
        __SEM_MASTER_B_LOWER___to___SEM_MASTER_A_LOWER___reference,
    ],
    OP_UPDATE: [
        __SEM_MASTER_B_LOWER___id_existence,
        __SEM_MASTER_B_LOWER___to___SEM_MASTER_A_LOWER___reference,
    ],
    OP_DELETE: [
        __SEM_MASTER_B_LOWER___id_existence,
    ],
}


def get_checks(
    master_type: str,
    operation: str,
) -> list[CheckFunction]:
    """master_typeとOperationに応じて、実行対象の検証関数一覧を返す。
    
    Check Registryに登録された順序をそのまま実行順として使用する。
    
    Args:
        master_type:
            Payloadで指定された論理Master種別。
        operation:
            I / U / D の操作種別。
    
    Returns:
        実行対象となる検証関数の順序付き一覧。
    
    Raises:
        ValueError:
            未対応のmaster_typeが指定された場合。
    """
    if master_type == __SEM_MASTER_A_UPPER__:
        return __SEM_MASTER_A_UPPER___CHECK_REGISTRY[operation]

    if master_type == __SEM_MASTER_B_UPPER__:
        return __SEM_MASTER_B_UPPER___CHECK_REGISTRY[operation]

    raise ValueError(
        f"Unsupported master_type: {master_type}"
    )


def get_current_record(
    session: Any,
    master_type: str,
    record: dict[str, Any],
) -> dict[str, str] | None:
    """Update対象レコードの現行値をMaster種別に応じて取得する。
    
    __SEM_MASTER_A_UPPER__の場合は__SEM_MASTER_A_UPPER__のDB上の現行値を、
    __SEM_MASTER_B_UPPER__の場合は__SEM_MASTER_B_UPPER__のDB上の現行値を取得する。
    業務キーが空の場合、または対象レコードが存在しない場合はNoneを返す。
    
    Args:
        session:
            Snowpark Session。
        master_type:
            Payloadで指定された論理Master種別。
        record:
            業務キーを含む入力レコード。
    
    Returns:
        入力項目名をキーとしてDB上の現行値を保持する辞書。
        対象が存在しない場合はNone。
    
    Raises:
        ValueError:
            未対応のmaster_typeが指定された場合。
    """
    if master_type == __SEM_MASTER_A_UPPER__:
        record_id = _text(
            record,
            __SEM_MASTER_A_UPPER___ID_COLUMN,
        )
        return (
            get_current___SEM_MASTER_A_LOWER__(
                session,
                record_id,
            )
            if record_id
            else None
        )

    if master_type == __SEM_MASTER_B_UPPER__:
        record_id = _text(
            record,
            __SEM_MASTER_B_UPPER___ID_COLUMN,
        )
        return (
            get_current___SEM_MASTER_B_LOWER__(
                session,
                record_id,
            )
            if record_id
            else None
        )

    raise ValueError(
        f"Unsupported master_type: {master_type}"
    )
```

## result_builder.py

```python
"""バリデーション結果を共通形式へ整形するためのヘルパーを定義する。

各チェック結果を同一の辞書構造で返し、呼出元での表示・集計・CSV出力を
一貫した形式で扱えるようにする。"""

from typing import Any


def build_result(
    *,
    row_no: int,
    record_id: str,
    check_id: str,
    status: str,
    input_column: str = "",
    input_value: str = "",
    target_database: str = "",
    target_schema: str = "",
    target_table: str = "",
    target_column: str = "",
    message: str = "",
) -> dict[str, Any]:
    """1件分のバリデーション結果を共通形式の辞書として生成する。
    
    Args:
        row_no:
            Payload内の1始まりの行番号。
        record_id:
            検証対象レコードの業務キー。
        check_id:
            実行した検証ルールを識別する固定ID。
        status:
            OK / NG / SKIP などの検証結果。
        input_column:
            検証対象となった入力項目。複合参照の場合は複数列を保持する。
        input_value:
            実際に検証した値。複合参照ではDB上の現行値を補完した後の値を保持する。
        target_database:
            参照整合性チェックで検索したDatabase。
        target_schema:
            参照整合性チェックで検索したSchema。
        target_table:
            参照整合性チェックで検索したTable / View。
        target_column:
            参照先の項目。複合参照の場合は複数列を保持する。
        message:
            検証結果の補足メッセージ。
    
    Returns:
        呼出元で共通利用できる形式の検証結果辞書。
    """
    return {
        "row_no": row_no,
        "record_id": record_id,
        "check_id": check_id,
        "status": status,
        "input_column": input_column,
        "input_value": input_value,
        "target_database": target_database,
        "target_schema": target_schema,
        "target_table": target_table,
        "target_column": target_column,
        "message": message,
    }


def build_ok_result(**kwargs: Any) -> dict[str, Any]:
    """OK結果を共通形式で生成する。
    
    Args:
        **kwargs:
            build_resultへ渡す各種検証結果情報。
    
    Returns:
        statusがOKに設定された検証結果辞書。
    """
    return build_result(status="OK", **kwargs)


def build_ng_result(**kwargs: Any) -> dict[str, Any]:
    """NG結果を共通形式で生成する。
    
    Args:
        **kwargs:
            build_resultへ渡す各種検証結果情報。
    
    Returns:
        statusがNGに設定された検証結果辞書。
    """
    return build_result(status="NG", **kwargs)


def build_skip_result(**kwargs: Any) -> dict[str, Any]:
    """SKIP結果を共通形式で生成する。
    
    Args:
        **kwargs:
            build_resultへ渡す各種検証結果情報。
    
    Returns:
        statusがSKIPに設定された検証結果辞書。
    """
    return build_result(status="SKIP", **kwargs)
```
