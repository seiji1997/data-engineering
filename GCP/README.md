# SQL for data engineering

- [BigQuery で学ぶ非エンジニアのための SQL データ分析入門](https://www.udemy.com/course/bigquerysql/)
- [The Complete SQL Bootcamp 2022: Go from Zero to Hero](https://www.udemy.com/course/the-complete-sql-bootcamp/)
- [BigQuery ML - Machine Learning in SQL using Google BigQuery](https://www.udemy.com/course/bigquery-ml-course/)
- [BigQuery for Big data engineers - Master Big Query Internals](https://www.udemy.com/course/bigquery/)

```app
import time
import pandas as pd
import streamlit as st

from upload import (
    read_csv_file,
    validate_file,
    detect_master_type,
    validate_upload_set,
    inspect_uploaded_file,
    build_payload,
    upload_signature,
    FileValidationError,
)
from validation import execute_validation
from result import (
    summarize_all_results,
    render_result_dashboard,
    count_ng_records,
)
from state import (
    init_state,
    save_run,
    get_active_run,
    set_active_result,
    clear_history,
    go_upload,
    go_result,
)


st.set_page_config(
    page_title="MDM 事前バリデーション",
    page_icon="✓",
    layout="wide",
)

init_state()

st.markdown(
    """
<style>
:root {
  --blue:#0A84FF;
  --orange:#FF9900;
  --green:#22C55E;
  --red:#EF4444;
  --border:color-mix(in srgb, var(--text-color) 18%, transparent);
  --surface:color-mix(in srgb, var(--text-color) 4%, transparent);
  --surface-strong:color-mix(in srgb, var(--text-color) 7%, transparent);
  --muted:color-mix(in srgb, var(--text-color) 68%, transparent);
}
.block-container {
  max-width:1180px;
  padding-top:1.55rem;
  padding-bottom:3.2rem;
}
[data-testid="stAppViewContainer"] {
  background:
    radial-gradient(circle at 8% -4%, rgba(10,132,255,.11), transparent 27%),
    radial-gradient(circle at 92% 3%, rgba(255,153,0,.065), transparent 23%);
}
h1 {
  letter-spacing:-.04em;
  font-size:2.28rem !important;
  margin-bottom:.20rem !important;
}
h2,h3 {
  letter-spacing:-.022em;
}
.hero-kicker {
  color:var(--blue);
  font-weight:800;
  font-size:.74rem;
  letter-spacing:.10em;
}
.hero-copy {
  color:var(--muted);
  margin:.2rem 0 1.05rem 0;
}

/* Top navigation */
.nav-caption {
  color:var(--muted);
  font-size:.76rem;
  margin-bottom:4px;
}
div[data-testid="stSegmentedControl"] button {
  min-height:42px;
  border-radius:12px !important;
  font-weight:750;
}

/* Upload heading */
.upload-intro {
  width:100%;
  max-width:720px;
  text-align:center;
  margin:2px auto 9px auto;
}
.upload-cloud {
  width:54px;
  height:54px;
  margin:0 auto 8px auto;
  display:flex;
  align-items:center;
  justify-content:center;
  border-radius:17px;
  background:linear-gradient(145deg, rgba(10,132,255,.18), rgba(37,99,235,.08));
  border:1px solid rgba(10,132,255,.28);
  box-shadow:0 10px 30px rgba(10,132,255,.10);
}
.upload-headline {
  font-size:1.18rem;
  font-weight:850;
  letter-spacing:-.025em;
}
.upload-subline {
  color:var(--muted);
  font-size:.89rem;
  margin-top:4px;
  line-height:1.5;
}

/*
  File uploader:
  The dotted rectangle is the layout container.
  Its native instruction block is hidden and the button + file limit are
  aligned against the exact center axis of that rectangle.
*/
div[data-testid="stFileUploader"] {
  width:100% !important;
  max-width:720px !important;
  margin:0 auto 8px auto !important;
}
section[data-testid="stFileUploaderDropzone"] {
  position:relative !important;
  min-height:112px !important;
  width:100% !important;
  box-sizing:border-box !important;
  display:flex !important;
  flex-direction:column !important;
  align-items:center !important;
  justify-content:center !important;
  gap:7px !important;
  border:1.7px dashed rgba(10,132,255,.64) !important;
  border-radius:20px !important;
  padding:18px 16px !important;
  background:rgba(10,132,255,.04) !important;
}
section[data-testid="stFileUploaderDropzone"]:hover {
  background:rgba(10,132,255,.07) !important;
  border-color:rgba(10,132,255,.92) !important;
}
div[data-testid="stFileUploaderDropzoneInstructions"] {
  display:none !important;
}
section[data-testid="stFileUploaderDropzone"] > button {
  align-self:center !important;
  margin:0 auto !important;
  min-width:142px !important;
  border-radius:12px !important;
}
section[data-testid="stFileUploaderDropzone"]::after {
  content:"最大 200MB / ファイル ・ CSV";
  display:block;
  width:100%;
  text-align:center;
  color:var(--muted);
  font-size:.78rem;
  line-height:1.2;
}

/* Cards / tables */
.soft-card {
  border:1px solid var(--border);
  border-radius:18px;
  padding:16px 18px;
  background:var(--surface);
}
.accent-card {
  border-left:3px solid var(--orange);
}
.rule-line {
  color:var(--muted);
  margin:7px 0;
  font-size:.91rem;
  line-height:1.55;
}
div[data-testid="stMetric"] {
  border:1px solid var(--border);
  border-radius:17px;
  padding:13px 15px;
  background:var(--surface);
}
div.stButton > button[kind="primary"] {
  border-radius:13px;
  min-height:46px;
  font-weight:800;
  box-shadow:0 9px 24px rgba(37,99,235,.20);
}
div.stButton > button:disabled {
  opacity:.58;
}
[data-testid="stDataFrame"] {
  border-radius:15px;
  overflow:hidden;
  border:1px solid var(--border);
}
hr {
  border-color:var(--border) !important;
}

/* Result dashboard */
[data-testid="stAlert"] {
  border-radius:16px;
}
[data-testid="stMetric"] label {
  font-weight:700;
}
div[data-testid="stExpander"] {
  border-radius:15px !important;
  overflow:hidden;
}
[data-testid="stTabs"] button {
  font-weight:750;
}

/* Final result UI */
[data-testid="stMetricValue"] {
  font-weight:800;
}
[data-testid="stProgress"] {
  margin-top:.15rem;
  margin-bottom:.15rem;
}
div[data-testid="stVerticalBlockBorderWrapper"] {
  border-radius:16px;
}

</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    """
<div class="hero-kicker">MDM CSV 事前検証</div>
<h1>MDM 事前バリデーション</h1>
<div class="hero-copy">
__SEM_MASTER_A_LABEL__ / __SEM_MASTER_B_LABEL__ を対象とした事前バリデーションです。1回の検証では同一Master・同一OperationのCSVのみ実行できます。
</div>
""",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------
# Top navigation / history
# ---------------------------------------------------------
history = st.session_state.validation_history
current_nav = "ファイル投入" if st.session_state.page == "upload" else "検証結果"

nav_col, hist_col, clear_col = st.columns([2.3, 2.1, .9], gap="small")

with nav_col:
    st.markdown('<div class="nav-caption">画面</div>', unsafe_allow_html=True)
    selected_nav = st.segmented_control(
        "画面",
        ["ファイル投入", "検証結果"],
        selection_mode="single",
        default=current_nav,
        label_visibility="collapsed",
        key=f"top_nav_{st.session_state.page}",
    )

with hist_col:
    st.markdown('<div class="nav-caption">検証結果履歴</div>', unsafe_allow_html=True)

    if history:
        history_labels = [
            (
                f'{run["result_id"]}｜'
                f'要修正 {count_ng_records(run.get("results", []))}レコード'
            )
            for run in history
        ]
        history_map = {
            label: run["result_id"]
            for label, run in zip(history_labels, history)
        }
        active_id = st.session_state.active_result_id
        active_index = 0

        if active_id:
            for idx, run in enumerate(history):
                if run["result_id"] == active_id:
                    active_index = idx
                    break

        selected_history = st.selectbox(
            "検証結果履歴",
            history_labels,
            index=active_index,
            label_visibility="collapsed",
            key=f"top_history_{len(history)}_{active_id}",
        )
    else:
        history_map = {}
        selected_history = None
        st.selectbox(
            "検証結果履歴",
            ["履歴なし"],
            disabled=True,
            label_visibility="collapsed",
            key="top_history_empty",
        )

with clear_col:
    st.markdown('<div class="nav-caption">セッション</div>', unsafe_allow_html=True)
    if st.button(
        "履歴クリア",
        use_container_width=True,
        disabled=(not history or st.session_state.running),
    ):
        clear_history()
        go_upload(clear_uploader=True)
        st.rerun()

if selected_nav and selected_nav != current_nav and not st.session_state.running:
    if selected_nav == "ファイル投入":
        go_upload(clear_uploader=False)
        st.rerun()
    elif selected_nav == "検証結果":
        if history:
            go_result()
            st.rerun()
        else:
            st.info("まだ検証結果がありません。まずCSVを検証してください。")

if selected_history and history and not st.session_state.running:
    selected_id = history_map[selected_history]
    if selected_id != st.session_state.active_result_id:
        set_active_result(selected_id)
        st.rerun()


# ---------------------------------------------------------
# Upload screen
# ---------------------------------------------------------
if st.session_state.page == "upload":
    main_col, side_col = st.columns([2.45, 1], gap="large")

    with side_col:
        st.markdown(
            """
<div class="soft-card accent-card">
  <b>検証単位</b>
  <div class="rule-line">● 1回の検証では __SEM_MASTER_A_LABEL__ / __SEM_MASTER_B_LABEL__ のどちらか一方のみ実行します</div>
  <div class="rule-line">● 登録 / 更新 / 削除は操作種別ごとに分けて実行します</div>
  <div class="rule-line">● 同一Master・同一Operationであれば複数CSVをまとめて検証できます</div>
  <div class="rule-line">● Updateは「-」を変更なし、空欄を明示クリアとして扱います</div>
  <div class="rule-line">● MasterやOperationが混在する場合は実行前にエラーとして案内します</div>
</div>
""",
            unsafe_allow_html=True,
        )

        st.write("")
        with st.expander("初めての方はこちら｜実行手順"):
            st.markdown(
                """
**1. CSVファイルをアップロード**  
検証したいCSVをドラッグ＆ドロップ、またはアップロードボタンから選択します。

**2. Master・操作・状態を確認**  
投入ファイル一覧で、__SEM_MASTER_A_LABEL__ / __SEM_MASTER_B_LABEL__、登録 / 更新 / 削除、準備状態を確認します。

**3. バリデーションを実行**  
「バリデーション実行」を押します。複数CSVの場合も同一Master・同一Operationであればまとめて実行できます。

**4. 検証結果を確認**  
実行完了後は自動で「検証結果」画面に移動します。全体・ファイル別・レコード別の順に結果を確認できます。

**5. 次の検証を行う**  
「ファイル投入」に戻って次のCSVを投入します。過去の結果はセッション中、上部の「検証結果履歴」から確認できます。
                """
            )

        if history:
            st.caption(f"このセッションでは {len(history)} 件の検証結果を保持しています。")

    with main_col:
        st.markdown(
            """
<div class="upload-intro">
  <div class="upload-cloud">
    <svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="#0A84FF" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
      <path d="M16 16l-4-4-4 4"></path>
      <path d="M12 12v9"></path>
      <path d="M20.39 18.39A5 5 0 0 0 18 9h-1.26A8 8 0 1 0 3 16.3"></path>
    </svg>
  </div>
  <div class="upload-headline">CSVファイルをアップロード</div>
  <div class="upload-subline">
    ドラッグ＆ドロップ、または下のボタンからCSVを選択してください。
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

        uploaded_files = st.file_uploader(
            "CSVファイル",
            type=["csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
            disabled=st.session_state.running,
            key=f"csv_uploader_{st.session_state.upload_widget_version}",
        )

        previews = []
        preview_errors = []
        current_signature = None
        upload_set_error = None
        upload_master_type = None
        upload_operation = None

        if uploaded_files:
            current_signature = upload_signature(uploaded_files)

            for uploaded_file in uploaded_files:
                try:
                    previews.append(inspect_uploaded_file(uploaded_file))
                except FileValidationError as exc:
                    preview_errors.append(
                        {
                            "file_name": uploaded_file.name,
                            "message": f"{exc.code}: {exc.message}",
                        }
                    )

            if previews and not preview_errors:
                try:
                    upload_master_type, upload_operation = validate_upload_set(previews)
                except FileValidationError as exc:
                    upload_set_error = f"{exc.code}: {exc.message}"

        already_completed = bool(
            uploaded_files
            and current_signature
            and st.session_state.completed_signature == current_signature
        )

        # Fixed action position: directly under uploader, before variable file list.
        if uploaded_files:
            if st.session_state.running:
                button_label = "バリデーション実行中…"
                button_disabled = True
            elif already_completed:
                button_label = "バリデーション実行済み"
                button_disabled = True
            else:
                button_label = "バリデーション実行"
                button_disabled = bool(preview_errors or upload_set_error)

            run_clicked = st.button(
                button_label,
                type="primary",
                use_container_width=True,
                disabled=button_disabled,
            )
        else:
            run_clicked = st.button(
                "バリデーション実行",
                type="primary",
                use_container_width=True,
                disabled=True,
            )

        # The uploaded-file list is always below the fixed action button.
        file_table = st.empty()

        def render_file_table(rows):
            if not rows:
                return
            df = pd.DataFrame(rows)[
                ["file_name", "master_label", "operation_label", "record_count", "size_kb", "status"]
            ].copy()
            df.columns = [
                "ファイル名",
                "Master",
                "操作",
                "レコード数",
                "サイズ(KB)",
                "状態",
            ]
            file_table.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
            )

        if previews:
            st.markdown("#### 投入ファイル")
            render_file_table(previews)

        for error in preview_errors:
            st.warning(f'{error["file_name"]} — {error["message"]}')

        if upload_set_error:
            st.error(upload_set_error)

        if already_completed:
            st.caption(
                "このファイル構成は直前に検証済みです。"
                "新しい検証を行う場合はファイルを入れ替えてください。"
            )

        if run_clicked:
            st.session_state.running = True
            st.session_state.run_requested = True
            st.rerun()

        # Reuses the SAME uploaded-file table for progress; no duplicate status table.
        if (
            uploaded_files
            and st.session_state.running
            and st.session_state.run_requested
        ):
            started_at = time.time()
            all_results = []
            total_files = len(uploaded_files)

            state_rows = []
            preview_by_name = {row["file_name"]: row for row in previews}

            for uploaded_file in uploaded_files:
                base = preview_by_name.get(
                    uploaded_file.name,
                    {
                        "file_name": uploaded_file.name,
                        "master_type": "-",
                        "master_label": "-",
                        "operation_label": "-",
                        "record_count": 0,
                        "size_kb": round(len(uploaded_file.getvalue()) / 1024, 1),
                    },
                )
                state_rows.append(
                    {
                        **base,
                        "status": "待機中",
                    }
                )

            progress = st.progress(0)

            for idx, uploaded_file in enumerate(uploaded_files, start=1):
                file_name = uploaded_file.name

                state_rows[idx - 1]["status"] = "実行中"
                render_file_table(state_rows)

                with st.spinner(f"{file_name} を検証中…"):
                    try:
                        rows = read_csv_file(uploaded_file)
                        master_type = detect_master_type(rows)
                        operation = validate_file(rows, master_type)
                        payload = build_payload(rows, master_type)

                        result = execute_validation(
                            session=st.connection("snowflake").session(),
                            operation=operation,
                            file_name=file_name,
                            payload=payload,
                        )

                        all_results.append(
                            {
                                "type": "BUSINESS_RESULT",
                                "file_name": file_name,
                                "master_type": master_type,
                                "operation": operation,
                                "record_count": len(rows),
                                "result": result,
                            }
                        )
                        state_rows[idx - 1]["status"] = "完了"

                    except FileValidationError as exc:
                        all_results.append(
                            {
                                "type": "FILE_ERROR",
                                "file_name": file_name,
                                "operation": "-",
                                "record_count": 0,
                                "error_code": exc.code,
                                "message": exc.message,
                            }
                        )
                        state_rows[idx - 1]["status"] = "入力エラー"

                    except Exception as exc:
                        all_results.append(
                            {
                                "type": "SYSTEM_ERROR",
                                "file_name": file_name,
                                "operation": "-",
                                "record_count": 0,
                                "error_code": "SYS-999",
                                "message": str(exc),
                            }
                        )
                        state_rows[idx - 1]["status"] = "システムエラー"

                render_file_table(state_rows)
                progress.progress(idx / total_files)

            finished_at = time.time()
            summary = summarize_all_results(all_results)

            save_run(
                results=all_results,
                summary=summary,
                started_at=started_at,
                finished_at=finished_at,
                upload_signature=current_signature,
            )

            st.session_state.running = False
            st.session_state.run_requested = False
            st.session_state.upload_widget_version += 1

            # Always show the result that has just completed.
            # save_run() already sets active_result_id to the newest result.
            st.session_state.page = "result"
            st.rerun()

        if not uploaded_files:
            if history:
                st.info(
                    "新しいCSVを投入できます。過去の結果は上部の「検証結果履歴」からいつでも確認できます。"
                )
            else:
                st.info("CSVファイルを投入すると、ここにファイル概要が表示されます。")


# ---------------------------------------------------------
# Result screen
# ---------------------------------------------------------
else:
    active_run = get_active_run()

    if not active_run:
        st.info("まだ検証結果がありません。")
    else:
        st.subheader("検証結果")
        render_result_dashboard(active_run)

```


```python
from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


CHECK_LABELS = {
    "ID001": "ID重複チェック",
    "ID002": "ID存在チェック",
    "REF101": "__SEM_MASTER_A_LABEL__参照整合性チェック",
    "REF201": "__SEM_REF_A_LABEL__参照整合性チェック",
    "REF202": "__SEM_REF_B_LABEL__参照整合性チェック",
    "REF203": "__SEM_REF_C_LABEL__参照整合性チェック",
    "REF204": "__SEM_REF_C_LABEL__ + __SEM_REF_D_LABEL__参照整合性チェック",
    "REF205": "__SEM_REF_C_LABEL__ + __SEM_REF_D_LABEL__ + __SEM_REF_E_LABEL__参照整合性チェック",
}

OPERATION_LABELS = {
    "I": "登録",
    "U": "更新",
    "D": "削除",
}

DISPLAY_COLUMNS = [
    "operation_label",
    "file_name",
    "row_no",
    "record_id",
    "check_id",
    "check_name",
    "status",
    "input_column",
    "input_value",
    "message",
]

RECORD_SUMMARY_COLUMNS = [
    "record_key",
    "file_name",
    "operation_label",
    "row_no",
    "record_id",
    "record_status",
    "check_count",
    "ok_count",
    "ng_count",
    "skip_count",
    "ng_summary",
]


def get_check_label(check_id: str, input_column: str) -> str:
    """Validation結果を利用者向けの表示名へ変換する。"""
    if check_id == "ID001":
        if input_column == "__PHY_MA_ID_COLUMN__":
            return "__SEM_MASTER_A_LABEL__ ID 重複チェック"
        if input_column == "__PHY_MB_ID_COLUMN__":
            return "__SEM_MASTER_B_LABEL__ ID 重複チェック"
        return "ID 重複チェック"

    if check_id == "ID002":
        if input_column == "__PHY_MA_ID_COLUMN__":
            return "__SEM_MASTER_A_LABEL__ ID 存在チェック"
        if input_column == "__PHY_MB_ID_COLUMN__":
            return "__SEM_MASTER_B_LABEL__ ID 存在チェック"
        return "ID 存在チェック"

    return CHECK_LABELS.get(check_id, check_id)


def summarize_all_results(results: list[dict[str, Any]]) -> dict[str, int]:
    """1回のValidation実行結果をチェック項目単位で集計する。"""
    summary = {
        "files": len(results),
        "records": 0,
        "checks": 0,
        "ok": 0,
        "ng": 0,
        "skip": 0,
        "file_error": 0,
        "system_error": 0,
    }

    for item in results:
        result_type = item.get("type")

        if result_type == "FILE_ERROR":
            summary["file_error"] += 1
            continue

        if result_type == "SYSTEM_ERROR":
            summary["system_error"] += 1
            continue

        summary["records"] += int(item.get("record_count", 0) or 0)
        rows = item.get("result", {}).get("results", [])
        summary["checks"] += len(rows)

        for row in rows:
            status = str(row.get("status") or "").upper()
            if status == "OK":
                summary["ok"] += 1
            elif status == "NG":
                summary["ng"] += 1
            elif status == "SKIP":
                summary["skip"] += 1

    return summary


def overall_run_status(summary: dict[str, int]) -> str:
    if summary.get("system_error", 0) > 0:
        return "SYSTEM_ERROR"
    if summary.get("file_error", 0) > 0:
        return "FILE_ERROR"
    if summary.get("ng", 0) > 0:
        return "NG"
    return "OK"


def build_result_df(results: list[dict[str, Any]]) -> pd.DataFrame:
    """Stored Procedureの結果を画面表示用DataFrameへ正規化する。"""
    rows: list[dict[str, Any]] = []

    for item in results:
        if item.get("type") != "BUSINESS_RESULT":
            continue

        file_name = str(item.get("file_name") or "")
        operation = str(item.get("operation") or "")

        for row in item.get("result", {}).get("results", []):
            normalized = dict(row)
            check_id = str(normalized.get("check_id") or "")
            input_column = str(normalized.get("input_column") or "")

            normalized["file_name"] = file_name
            normalized["operation_label"] = OPERATION_LABELS.get(operation, operation)
            normalized["check_id"] = check_id
            normalized["check_name"] = get_check_label(check_id, input_column)
            normalized["status"] = str(normalized.get("status") or "").upper()
            rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    df = pd.DataFrame(rows)

    for col in DISPLAY_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df["file_name"] = df["file_name"].fillna("").astype(str)
    df["record_id"] = df["record_id"].fillna("").astype(str)
    df["row_no"] = df["row_no"].fillna("")
    df["input_column"] = df["input_column"].fillna("").astype(str)
    df["input_value"] = df["input_value"].fillna("").astype(str)
    df["message"] = df["message"].fillna("").astype(str)

    return df[DISPLAY_COLUMNS]


def _first_non_blank(values: pd.Series) -> str:
    for value in values:
        text = "" if pd.isna(value) else str(value).strip()
        if text:
            return text
    return ""


def _record_key(file_name: str, row_no: Any) -> str:
    return f"{file_name}|||{row_no}"


def build_record_summary(df: pd.DataFrame) -> pd.DataFrame:
    """チェック結果をCSVレコード単位へ集約する。"""
    if df.empty:
        return pd.DataFrame(columns=RECORD_SUMMARY_COLUMNS)

    rows: list[dict[str, Any]] = []

    for (file_name, row_no), group in df.groupby(
        ["file_name", "row_no"],
        dropna=False,
        sort=False,
    ):
        statuses = group["status"].fillna("").astype(str).str.upper()
        ok_count = int((statuses == "OK").sum())
        ng_count = int((statuses == "NG").sum())
        skip_count = int((statuses == "SKIP").sum())

        ng_names = (
            group.loc[statuses == "NG", "check_name"]
            .fillna("")
            .astype(str)
            .tolist()
        )
        ng_names = list(dict.fromkeys(name for name in ng_names if name))

        file_text = "" if pd.isna(file_name) else str(file_name)
        rows.append(
            {
                "record_key": _record_key(file_text, row_no),
                "file_name": file_text,
                "operation_label": _first_non_blank(group["operation_label"]),
                "row_no": row_no,
                "record_id": _first_non_blank(group["record_id"]),
                "record_status": "要修正" if ng_count > 0 else "正常",
                "check_count": int(len(group)),
                "ok_count": ok_count,
                "ng_count": ng_count,
                "skip_count": skip_count,
                "ng_summary": " / ".join(ng_names),
            }
        )

    return pd.DataFrame(rows, columns=RECORD_SUMMARY_COLUMNS)


def build_file_summary(
    results: list[dict[str, Any]],
    record_summary: pd.DataFrame,
) -> pd.DataFrame:
    """複数CSVの結果をファイル単位へ集約する。"""
    rows: list[dict[str, Any]] = []

    for item in results:
        file_name = str(item.get("file_name") or "")
        result_type = item.get("type")
        operation = str(item.get("operation") or "")

        if result_type == "BUSINESS_RESULT":
            records = record_summary[record_summary["file_name"] == file_name]
            total_records = int(item.get("record_count", 0) or 0)
            normal_records = int((records["record_status"] == "正常").sum())
            ng_records = int((records["record_status"] == "要修正").sum())
            check_rows = item.get("result", {}).get("results", [])

            ok_checks = sum(
                1
                for row in check_rows
                if str(row.get("status") or "").upper() == "OK"
            )
            ng_checks = sum(
                1
                for row in check_rows
                if str(row.get("status") or "").upper() == "NG"
            )
            skip_checks = sum(
                1
                for row in check_rows
                if str(row.get("status") or "").upper() == "SKIP"
            )
            normal_rate = (
                round(normal_records / total_records * 100, 1)
                if total_records
                else 0.0
            )
            status = "✅ 正常" if ng_records == 0 else "❌ 要修正"

            rows.append(
                {
                    "状態": status,
                    "ファイル": file_name,
                    "操作": OPERATION_LABELS.get(operation, operation),
                    "対象レコード": total_records,
                    "正常レコード": f"{normal_records} / {total_records}",
                    "要修正レコード": f"{ng_records} / {total_records}",
                    "チェック項目": len(check_rows),
                    "OK": ok_checks,
                    "NG": ng_checks,
                    "SKIP": skip_checks,
                    "正常率": normal_rate,
                }
            )
            continue

        if result_type == "FILE_ERROR":
            rows.append(
                {
                    "状態": "⚠️ 入力エラー",
                    "ファイル": file_name,
                    "操作": "-",
                    "対象レコード": 0,
                    "正常レコード": "-",
                    "要修正レコード": "-",
                    "チェック項目": 0,
                    "OK": 0,
                    "NG": 0,
                    "SKIP": 0,
                    "正常率": 0.0,
                }
            )
            continue

        if result_type == "SYSTEM_ERROR":
            rows.append(
                {
                    "状態": "⛔ システムエラー",
                    "ファイル": file_name,
                    "操作": "-",
                    "対象レコード": 0,
                    "正常レコード": "-",
                    "要修正レコード": "-",
                    "チェック項目": 0,
                    "OK": 0,
                    "NG": 0,
                    "SKIP": 0,
                    "正常率": 0.0,
                }
            )

    return pd.DataFrame(rows)



def _safe_table_height(row_count: int, minimum: int = 150, maximum: int = 340) -> int:
    return max(minimum, min(maximum, 38 + row_count * 35))


def _normal_rate(total_records: int, normal_records: int) -> float:
    if total_records <= 0:
        return 0.0
    return normal_records / total_records


def render_result_header(run: dict[str, Any], record_summary: pd.DataFrame) -> None:
    """全体・ファイル・レコード・チェック項目の結果を最初のブロックに集約する。"""
    summary = run["summary"]
    status = overall_run_status(summary)
    results = run.get("results", [])
    file_summary = build_file_summary(results, record_summary)

    total_files = int(summary.get("files", 0) or 0)
    normal_files = int((file_summary["状態"] == "✅ 正常").sum()) if not file_summary.empty else 0
    ng_files = int((file_summary["状態"] == "❌ 要修正").sum()) if not file_summary.empty else 0
    error_files = total_files - normal_files - ng_files

    total_records = int(summary.get("records", 0) or 0)
    normal_records = int((record_summary["record_status"] == "正常").sum())
    ng_records = int((record_summary["record_status"] == "要修正").sum())
    rate = _normal_rate(total_records, normal_records)

    if status == "OK":
        st.success(
            f"✅ 検証完了：{total_files}ファイル・{total_records}レコードすべて正常です。"
        )
    elif status == "NG":
        st.error(
            f"❌ 修正が必要なデータがあります。{total_files}ファイル中 {ng_files}ファイル、"
            f"{total_records}レコード中 {ng_records}レコードが要修正です。"
        )
    elif status == "FILE_ERROR":
        st.warning("⚠️ 一部ファイルに入力エラーがあります。ファイル別結果を確認してください。")
    else:
        st.error("⛔ システムエラーにより、一部の検証を完了できませんでした。")

    st.caption(
        f'検証ID: {run["result_id"]} ・ '
        f'実行時間: {run["elapsed_seconds"]:.2f}秒'
    )

    with st.container(border=True):
        st.markdown("### 検証サマリー")
        st.caption(
            "ファイル → レコード → チェック項目の3階層で、今回の検証結果をまとめています。"
        )

        st.markdown("**📁 ファイル単位**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("対象ファイル", f"{total_files}件")
        c2.metric("正常ファイル", f"{normal_files}件")
        c3.metric("要修正ファイル", f"{ng_files}件")
        c4.metric("処理エラー", f"{max(error_files, 0)}件")

        st.markdown("**🧾 レコード単位**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("対象レコード", f"{total_records}件")
        c2.metric("正常レコード", f"{normal_records}件")
        c3.metric("要修正レコード", f"{ng_records}件")
        c4.metric("正常率", f"{rate * 100:.1f}%")

        st.progress(rate)
        st.caption(
            "レコード内にNGが1項目以上あれば「要修正」と判定します。SKIPはエラーではありません。"
        )

        st.markdown("**🔎 チェック項目単位**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("チェック項目", f'{summary.get("checks", 0)}件')
        c2.metric("✅ OK", f'{summary.get("ok", 0)}件')
        c3.metric("❌ NG", f'{summary.get("ng", 0)}件')
        c4.metric("⏭️ SKIP", f'{summary.get("skip", 0)}件')

        st.markdown("**CSVファイル別**")
        if file_summary.empty:
            st.info("表示できるファイル別結果がありません。")
        else:
            st.dataframe(
                file_summary,
                use_container_width=True,
                hide_index=True,
                height=_safe_table_height(len(file_summary), minimum=145, maximum=280),
                column_config={
                    "正常率": st.column_config.ProgressColumn(
                        "正常率",
                        min_value=0.0,
                        max_value=100.0,
                        format="%.1f%%",
                    ),
                },
            )



def render_file_errors(results: list[dict[str, Any]]) -> None:
    """CSV入力エラーとシステムエラーを簡潔に表示する。"""
    errors = [
        item
        for item in results
        if item.get("type") in {"FILE_ERROR", "SYSTEM_ERROR"}
    ]

    if not errors:
        return

    st.subheader("ファイル / システムエラー")

    for item in errors:
        raw_message = str(item.get("message", ""))
        if item.get("type") == "SYSTEM_ERROR":
            lines = [line.strip() for line in raw_message.splitlines() if line.strip()]
            concise = lines[-1] if lines else raw_message
            if len(concise) > 900:
                concise = concise[-900:]
            st.error(
                f'{item.get("file_name")} — '
                f'{item.get("error_code")}: {concise}'
            )
        else:
            st.warning(
                f'{item.get("file_name")} — '
                f'{item.get("error_code")}: {raw_message}'
            )


def render_file_summary(
    results: list[dict[str, Any]],
    record_summary: pd.DataFrame,
) -> None:
    """互換用。ファイル別サマリーは現在render_result_header内で表示する。"""
    return



def _record_display_df(record_summary: pd.DataFrame) -> pd.DataFrame:
    if record_summary.empty:
        return pd.DataFrame(
            columns=[
                "結果",
                "ファイル",
                "行",
                "レコードID",
                "チェック数",
                "OK",
                "NG",
                "SKIP",
                "NG内訳",
            ]
        )

    display_df = record_summary[
        [
            "record_status",
            "file_name",
            "row_no",
            "record_id",
            "check_count",
            "ok_count",
            "ng_count",
            "skip_count",
            "ng_summary",
        ]
    ].copy()
    display_df["record_status"] = display_df["record_status"].map(
        {"正常": "✅ 正常", "要修正": "❌ 要修正"}
    ).fillna(display_df["record_status"])
    display_df.columns = [
        "結果",
        "ファイル",
        "行",
        "レコードID",
        "チェック数",
        "OK",
        "NG",
        "SKIP",
        "NG内訳",
    ]
    return display_df



def _filter_records(
    records: pd.DataFrame,
    file_filter: str,
    query: str,
) -> pd.DataFrame:
    filtered = records.copy()

    if file_filter != "すべて":
        filtered = filtered[filtered["file_name"] == file_filter]

    query = query.strip().lower()
    if query:
        row_text = filtered["row_no"].astype(str).str.lower()
        id_text = filtered["record_id"].fillna("").astype(str).str.lower()
        filtered = filtered[
            row_text.str.contains(query, regex=False)
            | id_text.str.contains(query, regex=False)
        ]

    return filtered


def _humanize_input_value(value: Any) -> str:
    text = "" if value is None else str(value)
    if text == "":
        return "（空欄）"
    if text == "-":
        return "-（変更なし）"
    return text


def _check_guidance(check: pd.Series) -> tuple[str, str]:
    """チェック結果から、利用者向けの判定理由とNext Actionを返す。"""
    check_id = str(check.get("check_id") or "")
    status = str(check.get("status") or "").upper()
    input_column = str(check.get("input_column") or "")
    input_value = _humanize_input_value(check.get("input_value"))
    message = str(check.get("message") or "").strip()

    if status == "SKIP":
        reason = message or "今回の操作では、このチェックは実施対象外と判定されました。"
        action = "対応は不要です。意図した非更新・クリア指定になっているかだけ確認してください。"
        return reason, action

    if check_id == "ID001":
        if status == "OK":
            return (
                "入力したIDは既存Masterと重複していないため、新規登録可能と判定されました。",
                "対応は不要です。このまま申請を進められます。",
            )
        return (
            message or "入力したIDが既存Masterと重複しているため、新規登録できません。",
            "IDを新しい値へ変更してください。既存レコードの変更が目的の場合は、更新申請になっているか確認してください。",
        )

    if check_id == "ID002":
        if status == "OK":
            return (
                "対象IDが既存Masterで確認できたため、更新・削除対象として有効と判定されました。",
                "対応は不要です。このまま申請を進められます。",
            )
        return (
            message or "対象IDを既存Masterで正常に確認できませんでした。",
            "入力したIDが正しいか確認し、既存Masterに登録されている対象IDを指定してください。Master側の重複が疑われる場合は管理担当者へ確認してください。",
        )

    if check_id == "REF101":
        if status == "OK":
            return (
                "指定した__SEM_MASTER_A_LABEL__が__SEM_MASTER_A_LABEL__ Masterに存在するため、__SEM_MASTER_B_LABEL__から参照可能と判定されました。",
                "対応は不要です。このまま申請を進められます。",
            )
        return (
            message or "指定した__SEM_MASTER_A_LABEL__を__SEM_MASTER_A_LABEL__ Masterで確認できないため、参照できません。",
            "__SEM_MASTER_A_LABEL__ IDを確認し、既存の有効な__SEM_MASTER_A_LABEL__を指定してください。新規__SEM_MASTER_A_LABEL__を使用する場合は、先に__SEM_MASTER_A_LABEL__登録が必要か業務ルールを確認してください。",
        )

    ref_labels = {
        "REF201": "__SEM_REF_A_LABEL__",
        "REF202": "__SEM_REF_B_LABEL__",
        "REF203": "__SEM_REF_C_LABEL__",
    }
    if check_id in ref_labels:
        label = ref_labels[check_id]
        if status == "OK":
            return (
                f"指定した{label}の値が参照先Masterに存在するため、有効と判定されました。",
                "対応は不要です。このまま申請を進められます。",
            )
        return (
            message or f"指定した{label}の値を参照先Masterで確認できませんでした。",
            f"{label}の入力値を確認し、Masterに存在する有効な値へ修正してください。新規値の場合は事前登録が必要か業務担当者へ確認してください。",
        )

    if check_id == "REF204":
        if status == "OK":
            return (
                "__SEM_REF_C_LABEL__と__SEM_REF_D_LABEL__の組み合わせがMaster上に存在するため、有効な組み合わせと判定されました。",
                "対応は不要です。このまま申請を進められます。",
            )
        return (
            message or "__SEM_REF_C_LABEL__と__SEM_REF_D_LABEL__の組み合わせをMaster上で確認できませんでした。",
            "__SEM_REF_C_LABEL__と__SEM_REF_D_LABEL__をそれぞれ確認し、Masterに存在する有効な組み合わせへ修正してください。",
        )

    if check_id == "REF205":
        if status == "OK":
            return (
                "__SEM_REF_C_LABEL__・__SEM_REF_D_LABEL__・__SEM_REF_E_LABEL__の組み合わせがMaster上に存在するため、有効な組み合わせと判定されました。",
                "対応は不要です。このまま申請を進められます。",
            )
        return (
            message or "__SEM_REF_C_LABEL__・__SEM_REF_D_LABEL__・__SEM_REF_E_LABEL__の組み合わせをMaster上で確認できませんでした。",
            "3項目の組み合わせを確認し、Masterに存在する有効な組み合わせへ修正してください。",
        )

    if status == "OK":
        return (
            message or f"入力値 {input_value} はチェック条件を満たしているため、OKと判定されました。",
            "対応は不要です。このまま申請を進められます。",
        )

    return (
        message or f"入力項目 {input_column} の値 {input_value} がチェック条件を満たしていません。",
        "入力内容を確認し、チェック条件を満たす値へ修正してください。",
    )


def _short_ng_summary(summary: Any, max_items: int = 2) -> str:
    text = "" if summary is None else str(summary).strip()
    if not text:
        return "-"
    parts = [part.strip() for part in text.split(" / ") if part.strip()]
    if len(parts) <= max_items:
        return " / ".join(parts)
    return " / ".join(parts[:max_items]) + f" / 他{len(parts) - max_items}件"


def _render_record_detail(
    df: pd.DataFrame,
    record_summary: pd.DataFrame,
    record_key: str,
) -> None:
    """選択した1レコードの全チェック結果とNext Actionを一度に表示する。"""
    selected = record_summary[record_summary["record_key"] == record_key]
    if selected.empty:
        return

    record = selected.iloc[0]
    file_name = str(record["file_name"])
    row_no = record["row_no"]
    record_id = str(record["record_id"] or "-")

    detail_df = df[
        (df["file_name"] == file_name)
        & (df["row_no"].astype(str) == str(row_no))
    ].copy()

    st.markdown("#### レコード詳細")
    with st.container(border=True):
        if int(record["ng_count"]) > 0:
            st.error(
                f"❌ 要修正 ｜ {file_name} ｜ 行 {row_no} ｜ レコードID: {record_id}"
            )
        else:
            st.success(
                f"✅ 正常 ｜ {file_name} ｜ 行 {row_no} ｜ レコードID: {record_id}"
            )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("チェック項目", f'{int(record["check_count"])}件')
        c2.metric("✅ OK", f'{int(record["ok_count"])}件')
        c3.metric("❌ NG", f'{int(record["ng_count"])}件')
        c4.metric("⏭️ SKIP", f'{int(record["skip_count"])}件')

        ng_detail = detail_df[detail_df["status"] == "NG"].copy()
        if not ng_detail.empty:
            st.markdown("##### 🎯 このレコードのNext Action")
            for _, issue in ng_detail.iterrows():
                _, action = _check_guidance(issue)
                issue_name = str(issue.get("check_name") or issue.get("check_id") or "-")
                st.markdown(f"- **{issue_name}**：{action}")
        else:
            st.success("🎉 このレコードで修正が必要な項目はありません。")

        st.markdown("##### チェック結果詳細")
        st.caption("このレコードに対して実施した全チェックを、判定理由とNext Actionまで表示しています。")

        for _, check in detail_df.iterrows():
            check_status = str(check.get("status") or "").upper()
            if check_status == "OK":
                icon = "✅"
                status_label = "OK"
            elif check_status == "NG":
                icon = "❌"
                status_label = "NG"
            elif check_status == "SKIP":
                icon = "⏭️"
                status_label = "SKIP"
            else:
                icon = "•"
                status_label = check_status or "-"

            check_name = str(check.get("check_name") or check.get("check_id") or "-")
            input_column = str(check.get("input_column") or "-")
            input_value = _humanize_input_value(check.get("input_value"))
            reason, action = _check_guidance(check)

            with st.container(border=True):
                title_col, result_col = st.columns([5, 1])
                title_col.markdown(f"**{icon} {check_name}**")
                result_col.markdown(f"**{status_label}**")

                input_col, value_col = st.columns(2)
                input_col.markdown("**入力項目**")
                input_col.markdown(f"`{input_column}`")
                value_col.markdown("**入力値**")
                value_col.markdown(f"`{input_value}`")

                if check_status == "NG":
                    st.error(f"**判定理由**：{reason}")
                    st.warning(f"**Next Action**：{action}")
                elif check_status == "OK":
                    st.success(f"**OKの理由**：{reason}")
                    st.info(f"**Next Action**：{action}")
                elif check_status == "SKIP":
                    st.info(f"**SKIPの理由**：{reason}")
                    st.caption(f"**Next Action**：{action}")
                else:
                    st.info(f"**判定理由**：{reason}")
                    st.caption(f"**Next Action**：{action}")


def render_record_browser(
    df: pd.DataFrame,
    record_summary: pd.DataFrame,
    key_prefix: str,
) -> None:
    """大量レコードを固定高さの一覧で確認し、選択レコードだけ詳細表示する。"""
    if record_summary.empty:
        return

    st.subheader("レコード別検証結果")
    st.caption(
        "一覧は固定サイズでスクロールできます。NGを優先して確認し、詳細は必要な1レコードだけ表示します。"
    )

    ng_records = record_summary[record_summary["record_status"] == "要修正"].copy()
    normal_records = record_summary[record_summary["record_status"] == "正常"].copy()

    ng_records = ng_records.sort_values(
        ["ng_count", "file_name", "row_no"],
        ascending=[False, True, True],
        kind="stable",
    )
    normal_records = normal_records.sort_values(
        ["file_name", "row_no"],
        ascending=[True, True],
        kind="stable",
    )
    all_records = pd.concat([ng_records, normal_records], ignore_index=True)

    tab_ng, tab_normal, tab_all = st.tabs(
        [
            f"❌ 要修正（{len(ng_records)}件）",
            f"✅ 正常（{len(normal_records)}件）",
            f"📋 全レコード（{len(record_summary)}件）",
        ]
    )

    def render_table_tab(records: pd.DataFrame, tab_key: str) -> None:
        if records.empty:
            st.info("該当するレコードはありません。")
            return

        file_options = ["すべて"] + list(dict.fromkeys(records["file_name"].tolist()))
        filter_col, search_col = st.columns([1, 1.5])
        file_filter = filter_col.selectbox(
            "ファイル",
            file_options,
            key=f"{key_prefix}_{tab_key}_file_filter",
        )
        query = search_col.text_input(
            "レコードID / 行を検索",
            placeholder="例: TH12345 / 18",
            key=f"{key_prefix}_{tab_key}_search",
        )
        filtered = _filter_records(records, file_filter, query)
        st.caption(f"表示 {len(filtered)}件 / {len(records)}件")
        st.dataframe(
            _record_display_df(filtered),
            use_container_width=True,
            hide_index=True,
            height=340,
        )

    with tab_ng:
        render_table_tab(ng_records, "ng")

    with tab_normal:
        render_table_tab(normal_records, "normal")

    with tab_all:
        render_table_tab(all_records, "all")

    st.markdown("#### 詳細を確認するレコード")
    st.caption("NG内訳を見ながらレコードを選択できます。「詳細を表示」を押すと、判定理由とNext Actionを含む全チェック結果を一度に表示します。")

    option_map: dict[str, str] = {}
    for _, row in all_records.iterrows():
        icon = "❌" if row["record_status"] == "要修正" else "✅"
        record_id = str(row["record_id"] or "-")
        ng_breakdown = _short_ng_summary(row.get("ng_summary", ""))
        breakdown_text = (
            f" ｜ NG内訳: {ng_breakdown}"
            if int(row["ng_count"]) > 0
            else ""
        )
        label = (
            f'{icon} {row["file_name"]} ｜ 行 {row["row_no"]} ｜ '
            f'{record_id} ｜ OK {int(row["ok_count"])} / NG {int(row["ng_count"])} / '
            f'SKIP {int(row["skip_count"])}{breakdown_text}'
        )
        option_map[label] = str(row["record_key"])

    select_col, button_col = st.columns([4.5, 1.1])
    selected_label = select_col.selectbox(
        "レコード",
        list(option_map.keys()),
        key=f"{key_prefix}_record_selector",
        label_visibility="collapsed",
    )
    if button_col.button(
        "詳細を表示",
        type="primary",
        use_container_width=True,
        key=f"{key_prefix}_show_record_detail",
    ):
        st.session_state[f"{key_prefix}_detail_record_key"] = option_map[selected_label]

    selected_key = st.session_state.get(f"{key_prefix}_detail_record_key")
    if selected_key:
        _render_record_detail(df, record_summary, selected_key)



def render_result_matrix(results: list[dict[str, Any]], key_prefix: str) -> None:
    """全チェック結果を必要時のみ展開して確認・ダウンロードできるようにする。"""
    df = build_result_df(results)

    if df.empty:
        return

    with st.expander("全チェック結果・CSVダウンロード", expanded=False):
        st.caption(
            "監査・詳細確認向けの全チェック結果です。通常は上のレコード別結果から確認してください。"
        )

        c1, c2, c3 = st.columns([1.2, 1.2, 1.6])

        file_options = ["すべて"] + list(dict.fromkeys(df["file_name"].tolist()))
        file_filter = c1.selectbox(
            "ファイル",
            file_options,
            key=f"{key_prefix}_matrix_file",
        )

        status_filter = c2.selectbox(
            "結果",
            ["すべて", "NG", "OK", "SKIP"],
            key=f"{key_prefix}_matrix_status",
        )

        query = c3.text_input(
            "レコードID / チェック内容を検索",
            key=f"{key_prefix}_matrix_search",
        )

        filtered = df.copy()

        if file_filter != "すべて":
            filtered = filtered[filtered["file_name"] == file_filter]

        if status_filter != "すべて":
            filtered = filtered[filtered["status"] == status_filter]

        query = query.strip().lower()
        if query:
            filtered = filtered[
                filtered["record_id"].astype(str).str.lower().str.contains(query, regex=False)
                | filtered["check_name"].astype(str).str.lower().str.contains(query, regex=False)
            ]

        display_df = filtered[
            [
                "operation_label",
                "file_name",
                "row_no",
                "record_id",
                "check_name",
                "status",
                "input_column",
                "input_value",
                "message",
            ]
        ].copy()
        display_df.columns = [
            "操作",
            "ファイル",
            "行",
            "レコードID",
            "チェック内容",
            "結果",
            "入力項目",
            "入力値",
            "メッセージ",
        ]

        st.caption(f"表示 {len(display_df)}件 / 全 {len(df)}件")
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=420,
        )

        st.download_button(
            "全チェック結果CSVをダウンロード",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{key_prefix}_validation_result.csv",
            mime="text/csv",
            key=f"{key_prefix}_matrix_download",
        )


def count_ng_records(results: list[dict[str, Any]]) -> int:
    """要修正レコード数を返す。"""
    record_summary = build_record_summary(build_result_df(results))
    if record_summary.empty:
        return 0
    return int((record_summary["record_status"] == "要修正").sum())


def render_result_dashboard(run: dict[str, Any]) -> None:
    """検証結果画面全体を、総括→レコード一覧→レコード詳細の順に表示する。"""
    results = run.get("results", [])
    df = build_result_df(results)
    record_summary = build_record_summary(df)

    render_result_header(run, record_summary)
    render_file_errors(results)

    st.divider()
    render_record_browser(
        df,
        record_summary,
        key_prefix=run["result_id"],
    )

    st.write("")
    render_result_matrix(
        results,
        key_prefix=run["result_id"],
    )



def render_top_issues(df: pd.DataFrame) -> None:
    """旧呼出互換用。新UIではrender_result_dashboardを使用する。"""
    if df.empty:
        return
    ng_df = df[df["status"] == "NG"]
    if ng_df.empty:
        st.success("修正が必要な項目はありません。")
        return
    st.info("新UIではレコード別検証結果からNGレコードを確認してください。")


def render_history(history: list[dict[str, Any]], active_id: str | None) -> None:
    """セッション内の検証履歴を一覧表示する。"""
    if not history:
        st.info("このセッションには過去の検証結果がありません。")
        return

    rows = []
    for run in history:
        result_df = build_result_df(run.get("results", []))
        records = build_record_summary(result_df)
        ng_records = int((records["record_status"] == "要修正").sum())
        summary = run["summary"]
        rows.append(
            {
                "検証ID": run["result_id"],
                "ファイル": summary.get("files", 0),
                "レコード": summary.get("records", 0),
                "要修正レコード": ng_records,
                "NG項目": summary.get("ng", 0),
                "実行時間(秒)": run.get("elapsed_seconds", 0),
                "表示中": "●" if run["result_id"] == active_id else "",
            }
        )

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
        height=_safe_table_height(len(rows), minimum=145, maximum=300),
    )

```
