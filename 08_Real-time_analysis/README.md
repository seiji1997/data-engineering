# data-engineering

# procedure code

```python
from typing import Any, Callable
from constants import (
    OP_INSERT, OP_UPDATE, OP_DELETE,
    MASTER_PARENT, MASTER_CHILD,
    PARENT_MASTER, CHILD_MASTER,
    REFERENCE_MASTER_A, REFERENCE_MASTER_B,
    PARENT_ID_COLUMN, CHILD_ID_COLUMN, CHILD_PARENT_ID_COLUMN,
    REFERENCE_KEY_A_COLUMN, REFERENCE_KEY_B_COLUMN,
    ID001, ID002, REF001, REF002, REF101,
)
from result_builder import build_ok_result, build_ng_result, build_skip_result

CheckFunction = Callable[..., dict[str, Any]]

def _split_object_name(name: str) -> tuple[str, str, str]:
    """Split DATABASE.SCHEMA.OBJECT into three parts."""
    parts = name.split('.')
    return (parts[0], parts[1], parts[2]) if len(parts) == 3 else ('', '', name)

def _text(record: dict[str, Any], column: str) -> str:
    """Return a trimmed string value from one input record."""
    value = record.get(column)
    return '' if value is None else str(value).strip()

def _exists(session: Any, sql_text: str, params: list[Any]) -> bool:
    """Return True when an existence query returns at least one row."""
    return bool(session.sql(sql_text, params=params).collect())

def get_current_parent(session: Any, parent_id: str) -> dict[str, str] | None:
    """Fetch current parent values needed for partial-update validation."""
    rows = session.sql(
        f"""SELECT {PARENT_ID_COLUMN}, {REFERENCE_KEY_A_COLUMN}, {REFERENCE_KEY_B_COLUMN}
            FROM {PARENT_MASTER}
            WHERE {PARENT_ID_COLUMN} = ?
            LIMIT 1""",
        params=[parent_id],
    ).collect()
    if not rows:
        return None
    row = rows[0]
    return {
        PARENT_ID_COLUMN: '' if row[PARENT_ID_COLUMN] is None else str(row[PARENT_ID_COLUMN]).strip(),
        REFERENCE_KEY_A_COLUMN: '' if row[REFERENCE_KEY_A_COLUMN] is None else str(row[REFERENCE_KEY_A_COLUMN]).strip(),
        REFERENCE_KEY_B_COLUMN: '' if row[REFERENCE_KEY_B_COLUMN] is None else str(row[REFERENCE_KEY_B_COLUMN]).strip(),
    }

def parent_id_duplicate(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Insert check: parent ID must not already exist."""
    parent_id = _text(record, PARENT_ID_COLUMN)
    db, schema, table = _split_object_name(PARENT_MASTER)
    common = dict(row_no=row_no, record_id=parent_id, check_id=ID001, input_column=PARENT_ID_COLUMN, input_value=parent_id, target_database=db, target_schema=schema, target_table=table, target_column=PARENT_ID_COLUMN)
    if parent_id == '': return build_ng_result(**common, message=f'{PARENT_ID_COLUMN} is blank.')
    if _exists(session, f'SELECT 1 FROM {PARENT_MASTER} WHERE {PARENT_ID_COLUMN} = ? LIMIT 1', [parent_id]):
        return build_ng_result(**common, message=f'{PARENT_ID_COLUMN} {parent_id} already exists.')
    return build_ok_result(**common, message='')

def parent_id_existence(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Update/Delete check: parent ID must exist."""
    parent_id = _text(record, PARENT_ID_COLUMN)
    db, schema, table = _split_object_name(PARENT_MASTER)
    common = dict(row_no=row_no, record_id=parent_id, check_id=ID002, input_column=PARENT_ID_COLUMN, input_value=parent_id, target_database=db, target_schema=schema, target_table=table, target_column=PARENT_ID_COLUMN)
    if parent_id == '': return build_ng_result(**common, message=f'{PARENT_ID_COLUMN} is blank.')
    if operation == OP_UPDATE:
        return build_ok_result(**common, message='') if current_record is not None else build_ng_result(**common, message=f'{PARENT_ID_COLUMN} {parent_id} does not exist.')
    if _exists(session, f'SELECT 1 FROM {PARENT_MASTER} WHERE {PARENT_ID_COLUMN} = ? LIMIT 1', [parent_id]): return build_ok_result(**common, message='')
    return build_ng_result(**common, message=f'{PARENT_ID_COLUMN} {parent_id} does not exist.')

def parent_reference_a(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Validate a single-key reference when applicable."""
    parent_id = _text(record, PARENT_ID_COLUMN); value = _text(record, REFERENCE_KEY_A_COLUMN)
    db, schema, table = _split_object_name(REFERENCE_MASTER_A)
    common = dict(row_no=row_no, record_id=parent_id, check_id=REF001, input_column=REFERENCE_KEY_A_COLUMN, input_value=value, target_database=db, target_schema=schema, target_table=table, target_column=REFERENCE_KEY_A_COLUMN)
    if operation == OP_UPDATE and value == '': return build_skip_result(**common, message=f'{REFERENCE_KEY_A_COLUMN} was not updated.')
    if value == '': return build_ng_result(**common, message=f'{REFERENCE_KEY_A_COLUMN} is blank.')
    if _exists(session, f'SELECT 1 FROM {REFERENCE_MASTER_A} WHERE {REFERENCE_KEY_A_COLUMN} = ? LIMIT 1', [value]): return build_ok_result(**common, message='')
    return build_ng_result(**common, message=f'{REFERENCE_KEY_A_COLUMN} {value} does not exist.')

def parent_composite_reference(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Validate an effective two-column reference pair."""
    parent_id = _text(record, PARENT_ID_COLUMN); new_a = _text(record, REFERENCE_KEY_A_COLUMN); new_b = _text(record, REFERENCE_KEY_B_COLUMN)
    db, schema, table = _split_object_name(REFERENCE_MASTER_B)
    base = dict(row_no=row_no, record_id=parent_id, check_id=REF002, input_column=f'{REFERENCE_KEY_A_COLUMN},{REFERENCE_KEY_B_COLUMN}', target_database=db, target_schema=schema, target_table=table, target_column=f'{REFERENCE_KEY_A_COLUMN},{REFERENCE_KEY_B_COLUMN}')
    if operation == OP_INSERT:
        effective_a, effective_b = new_a, new_b
        if not effective_a or not effective_b:
            return build_ng_result(**base, input_value=f'{effective_a},{effective_b}', message=f'{REFERENCE_KEY_A_COLUMN} and {REFERENCE_KEY_B_COLUMN} are required.')
    elif operation == OP_UPDATE:
        a_changed, b_changed = bool(new_a), bool(new_b)
        if not a_changed and not b_changed:
            return build_skip_result(**base, input_value=',', message=f'{REFERENCE_KEY_A_COLUMN} and {REFERENCE_KEY_B_COLUMN} were not updated.')
        if a_changed and b_changed:
            effective_a, effective_b = new_a, new_b
        else:
            if current_record is None:
                return build_skip_result(**base, input_value=f'{new_a},{new_b}', message='Current parent record was not found; the composite reference could not be completed.')
            effective_a = new_a if a_changed else current_record.get(REFERENCE_KEY_A_COLUMN, '')
            effective_b = new_b if b_changed else current_record.get(REFERENCE_KEY_B_COLUMN, '')
        if not effective_a or not effective_b:
            return build_ng_result(**base, input_value=f'{effective_a},{effective_b}', message='The effective composite reference is incomplete.')
    else:
        return build_skip_result(**base, input_value=f'{new_a},{new_b}', message='This check is not applicable to the operation.')
    if _exists(session, f'SELECT 1 FROM {REFERENCE_MASTER_B} WHERE {REFERENCE_KEY_A_COLUMN} = ? AND {REFERENCE_KEY_B_COLUMN} = ? LIMIT 1', [effective_a, effective_b]):
        return build_ok_result(**base, input_value=f'{effective_a},{effective_b}', message='')
    return build_ng_result(**base, input_value=f'{effective_a},{effective_b}', message=f'{effective_a} / {effective_b} does not exist in the composite-reference master.')

def get_current_child(session: Any, child_id: str) -> dict[str, str] | None:
    """Fetch the current child row needed for update validation."""
    rows = session.sql(f'SELECT {CHILD_ID_COLUMN}, {CHILD_PARENT_ID_COLUMN} FROM {CHILD_MASTER} WHERE {CHILD_ID_COLUMN} = ? LIMIT 1', params=[child_id]).collect()
    if not rows: return None
    row = rows[0]
    return {CHILD_ID_COLUMN: '' if row[CHILD_ID_COLUMN] is None else str(row[CHILD_ID_COLUMN]).strip(), CHILD_PARENT_ID_COLUMN: '' if row[CHILD_PARENT_ID_COLUMN] is None else str(row[CHILD_PARENT_ID_COLUMN]).strip()}

def child_id_duplicate(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Insert check: child ID must not already exist."""
    child_id = _text(record, CHILD_ID_COLUMN); db, schema, table = _split_object_name(CHILD_MASTER)
    common = dict(row_no=row_no, record_id=child_id, check_id=ID001, input_column=CHILD_ID_COLUMN, input_value=child_id, target_database=db, target_schema=schema, target_table=table, target_column=CHILD_ID_COLUMN)
    if child_id == '': return build_ng_result(**common, message=f'{CHILD_ID_COLUMN} is blank.')
    if _exists(session, f'SELECT 1 FROM {CHILD_MASTER} WHERE {CHILD_ID_COLUMN} = ? LIMIT 1', [child_id]): return build_ng_result(**common, message=f'{CHILD_ID_COLUMN} {child_id} already exists.')
    return build_ok_result(**common, message='')

def child_id_existence(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Update/Delete check: child ID must exist."""
    child_id = _text(record, CHILD_ID_COLUMN); db, schema, table = _split_object_name(CHILD_MASTER)
    common = dict(row_no=row_no, record_id=child_id, check_id=ID002, input_column=CHILD_ID_COLUMN, input_value=child_id, target_database=db, target_schema=schema, target_table=table, target_column=CHILD_ID_COLUMN)
    if child_id == '': return build_ng_result(**common, message=f'{CHILD_ID_COLUMN} is blank.')
    if operation == OP_UPDATE:
        return build_ok_result(**common, message='') if current_record is not None else build_ng_result(**common, message=f'{CHILD_ID_COLUMN} {child_id} does not exist.')
    if _exists(session, f'SELECT 1 FROM {CHILD_MASTER} WHERE {CHILD_ID_COLUMN} = ? LIMIT 1', [child_id]): return build_ok_result(**common, message='')
    return build_ng_result(**common, message=f'{CHILD_ID_COLUMN} {child_id} does not exist.')

def child_parent_reference(session: Any, operation: str, row_no: int, record: dict[str, Any], current_record: dict[str, str] | None = None) -> dict[str, Any]:
    """Validate the child's parent-reference value."""
    child_id = _text(record, CHILD_ID_COLUMN); parent_ref = _text(record, CHILD_PARENT_ID_COLUMN); db, schema, table = _split_object_name(PARENT_MASTER)
    common = dict(row_no=row_no, record_id=child_id, check_id=REF101, input_column=CHILD_PARENT_ID_COLUMN, input_value=parent_ref, target_database=db, target_schema=schema, target_table=table, target_column=PARENT_ID_COLUMN)
    if operation == OP_UPDATE and parent_ref == '': return build_skip_result(**common, message=f'{CHILD_PARENT_ID_COLUMN} was not updated.')
    if parent_ref == '': return build_ng_result(**common, message=f'{CHILD_PARENT_ID_COLUMN} is blank.')
    if _exists(session, f'SELECT 1 FROM {PARENT_MASTER} WHERE {PARENT_ID_COLUMN} = ? LIMIT 1', [parent_ref]): return build_ok_result(**common, message='')
    return build_ng_result(**common, message=f'{CHILD_PARENT_ID_COLUMN} {parent_ref} does not exist in the parent master.')

PARENT_CHECK_REGISTRY: dict[str, list[CheckFunction]] = {
    OP_INSERT: [parent_id_duplicate, parent_reference_a, parent_composite_reference],
    OP_UPDATE: [parent_id_existence, parent_reference_a, parent_composite_reference],
    OP_DELETE: [parent_id_existence],
}
CHILD_CHECK_REGISTRY: dict[str, list[CheckFunction]] = {
    OP_INSERT: [child_id_duplicate, child_parent_reference],
    OP_UPDATE: [child_id_existence, child_parent_reference],
    OP_DELETE: [child_id_existence],
}

def get_checks(master_type: str, operation: str) -> list[CheckFunction]:
    """Return registered checks for one master and operation."""
    if master_type == MASTER_PARENT: return PARENT_CHECK_REGISTRY[operation]
    if master_type == MASTER_CHILD: return CHILD_CHECK_REGISTRY[operation]
    raise ValueError(f'Unsupported master_type: {master_type}')

def get_current_record(session: Any, master_type: str, record: dict[str, Any]) -> dict[str, str] | None:
    """Fetch the current master row once for update processing."""
    if master_type == MASTER_PARENT:
        parent_id = _text(record, PARENT_ID_COLUMN)
        return get_current_parent(session, parent_id) if parent_id else None
    if master_type == MASTER_CHILD:
        child_id = _text(record, CHILD_ID_COLUMN)
        return get_current_child(session, child_id) if child_id else None
    raise ValueError(f'Unsupported master_type: {master_type}')

```

```python
from typing import Final

APP_DATABASE: Final[str] = "{{APP_DATABASE}}"
APP_SCHEMA: Final[str] = "{{APP_SCHEMA}}"
MASTER_DATABASE: Final[str] = "{{MASTER_DATABASE}}"
MASTER_SCHEMA: Final[str] = "{{MASTER_SCHEMA}}"
CODE_STAGE: Final[str] = "{{CODE_STAGE}}"
PROCEDURE_NAME: Final[str] = "{{PROCEDURE_NAME}}"

MASTER_PARENT: Final[str] = "{{PARENT_CODE}}"
MASTER_CHILD: Final[str] = "{{CHILD_CODE}}"
SUPPORTED_MASTER_TYPES: Final[set[str]] = {MASTER_PARENT, MASTER_CHILD}

OP_INSERT: Final[str] = "I"
OP_UPDATE: Final[str] = "U"
OP_DELETE: Final[str] = "D"
SUPPORTED_OPERATIONS: Final[set[str]] = {OP_INSERT, OP_UPDATE, OP_DELETE}

PARENT_MASTER: Final[str] = f"{MASTER_DATABASE}.{MASTER_SCHEMA}.{{PARENT_MASTER_TABLE}}"
CHILD_MASTER: Final[str] = f"{MASTER_DATABASE}.{MASTER_SCHEMA}.{{CHILD_MASTER_TABLE}}"
REFERENCE_MASTER_A: Final[str] = f"{MASTER_DATABASE}.{MASTER_SCHEMA}.{{REFERENCE_MASTER_A_TABLE}}"
REFERENCE_MASTER_B: Final[str] = f"{MASTER_DATABASE}.{MASTER_SCHEMA}.{{REFERENCE_MASTER_B_TABLE}}"

PARENT_ID_COLUMN: Final[str] = "{{PARENT_ID_COLUMN}}"
CHILD_ID_COLUMN: Final[str] = "{{CHILD_ID_COLUMN}}"
CHILD_PARENT_ID_COLUMN: Final[str] = "{{CHILD_PARENT_ID_COLUMN}}"
REFERENCE_KEY_A_COLUMN: Final[str] = "{{REFERENCE_KEY_A_COLUMN}}"
REFERENCE_KEY_B_COLUMN: Final[str] = "{{REFERENCE_KEY_B_COLUMN}}"

ID001: Final[str] = "ID001"
ID002: Final[str] = "ID002"
REF001: Final[str] = "REF001"
REF002: Final[str] = "REF002"
REF101: Final[str] = "REF101"

```

```python
import json
from typing import Any
from constants import OP_UPDATE, SUPPORTED_MASTER_TYPES, SUPPORTED_OPERATIONS
from checks import get_checks, get_current_record

def _normalize_operation(operation: str) -> str:
    """Normalize and validate the procedure operation."""
    op = (operation or '').strip().upper()
    if op not in SUPPORTED_OPERATIONS: raise ValueError(f'Unsupported operation: {operation}')
    return op

def _parse_payload(payload_json: str | dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    """Parse and validate the JSON payload received from the UI."""
    if payload_json is None: raise ValueError('P_PAYLOAD_JSON is empty.')
    payload = payload_json if isinstance(payload_json, dict) else json.loads(str(payload_json))
    if not isinstance(payload, dict): raise ValueError('Payload must be a JSON object.')
    master_type = str(payload.get('master_type') or '').strip().upper()
    if master_type not in SUPPORTED_MASTER_TYPES: raise ValueError(f'Unsupported master_type: {master_type}')
    records = payload.get('records')
    if not isinstance(records, list): raise ValueError('Payload.records must be an array.')
    return master_type, records

def validate_request(session: Any, p_operation: str, p_file_name: str, p_payload_json: str) -> str:
    """Execute validation for one file, master type, and operation."""
    operation = _normalize_operation(p_operation)
    master_type, records = _parse_payload(p_payload_json)
    checks = get_checks(master_type, operation); results: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict): raise ValueError('Each records[] item must be a JSON object.')
        row_no = item.get('row_no'); record = item.get('record')
        if row_no is None: raise ValueError('row_no is required.')
        if not isinstance(record, dict): raise ValueError('record must be a JSON object.')
        current_record = get_current_record(session, master_type, record) if operation == OP_UPDATE else None
        for check in checks:
            results.append(check(session=session, operation=operation, row_no=int(row_no), record=record, current_record=current_record))
    return json.dumps({'file_name': p_file_name, 'master_type': master_type, 'operation': operation, 'results': results}, ensure_ascii=False)

```

```python
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
    """Build one normalized Record × Check validation result.

    Args:
        row_no: 1-based CSV row number within the payload.
        record_id: Business key of the record being validated.
        check_id: Stable validation check identifier.
        status: Business status. Expected values are OK / NG / SKIP.
        input_column: Source CSV column(s) used by the check.
        input_value: Effective value(s) evaluated by the check.
        target_database: Referenced Snowflake database.
        target_schema: Referenced Snowflake schema.
        target_table: Referenced Snowflake table/view.
        target_column: Referenced Snowflake column(s).
        message: Human-readable result message.

    Returns:
        A dictionary with the common validation-result schema.
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


def build_ok_result(**kwargs) -> dict[str, Any]:
    """Build an OK validation result."""
    return build_result(status="OK", **kwargs)


def build_ng_result(**kwargs) -> dict[str, Any]:
    """Build an NG validation result."""
    return build_result(status="NG", **kwargs)


def build_skip_result(**kwargs) -> dict[str, Any]:
    """Build a SKIP validation result."""
    return build_result(status="SKIP", **kwargs)

```
