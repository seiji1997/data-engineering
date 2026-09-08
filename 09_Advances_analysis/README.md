# data-engineering

# streamlit code

```python
import pandas as pd
import streamlit as st


CHECK_LABELS = {
    "ID001": "ID 重複チェック",
    "ID002": "ID 存在チェック",
    "REF001": "{{REFERENCE_A_LABEL}} 参照整合性チェック",
    "REF002": "{{REFERENCE_B_LABEL}} 参照整合性チェック",
    "REF101": "{{CHILD_LABEL}} → {{PARENT_LABEL}} 参照整合性チェック",
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
    "check_name",
    "status",
    "input_column",
    "input_value",
    "message",
]


def summarize_all_results(results):
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

        summary["records"] += item.get("record_count", 0)
        rows = item.get("result", {}).get("results", [])
        summary["checks"] += len(rows)

        for row in rows:
            status = row.get("status")
            if status == "OK":
                summary["ok"] += 1
            elif status == "NG":
                summary["ng"] += 1
            elif status == "SKIP":
                summary["skip"] += 1

    return summary


def overall_run_status(summary):
    if summary.get("system_error", 0) > 0:
        return "SYSTEM_ERROR"
    if summary.get("file_error", 0) > 0:
        return "FILE_ERROR"
    if summary.get("ng", 0) > 0:
        return "NG"
    return "OK"


def build_result_df(results):
    rows = []

    for item in results:
        if item.get("type") != "BUSINESS_RESULT":
            continue

        file_name = item.get("file_name", "")
        operation = item.get("operation", "")

        for row in item.get("result", {}).get("results", []):
            normalized = dict(row)
            normalized["file_name"] = file_name
            normalized["operation_label"] = OPERATION_LABELS.get(operation, operation)
            normalized["check_name"] = CHECK_LABELS.get(
                normalized.get("check_id", ""),
                normalized.get("check_id", ""),
            )
            rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    df = pd.DataFrame(rows)

    for col in DISPLAY_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df[DISPLAY_COLUMNS]


def render_result_header(run):
    summary = run["summary"]
    status = overall_run_status(summary)

    if status == "OK":
        st.success("バリデーションが正常に完了しました。")
    elif status == "NG":
        st.error("修正が必要なデータがあります。")
    elif status == "FILE_ERROR":
        st.warning("一部ファイルに入力エラーがあります。")
    else:
        st.error("システムエラーにより、一部の検証を完了できませんでした。")

    st.caption(
        f'検証ID: {run["result_id"]} ・ 実行時間: {run["elapsed_seconds"]:.2f}秒'
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("対象ファイル", summary.get("files", 0))
    c2.metric("対象レコード", summary.get("records", 0))
    c3.metric("OK", summary.get("ok", 0))
    c4.metric("NG", summary.get("ng", 0))
    c5.metric("SKIP", summary.get("skip", 0))


def render_file_errors(results):
    errors = [
        item for item in results
        if item.get("type") in {"FILE_ERROR", "SYSTEM_ERROR"}
    ]

    if not errors:
        return

    st.subheader("ファイル / システムエラー")
    for item in errors:
        raw_message = str(item.get("message", ""))
        if item.get("type") == "SYSTEM_ERROR":
            # Snowflake exceptions can contain a full Python traceback.
            # Surface the useful tail in the business UI instead of a wall of text.
            lines = [line.strip() for line in raw_message.splitlines() if line.strip()]
            concise = lines[-1] if lines else raw_message
            if len(concise) > 900:
                concise = concise[-900:]
            message = (
                f'{item.get("file_name")} — '
                f'{item.get("error_code")}: {concise}'
            )
            st.error(message)
        else:
            message = (
                f'{item.get("file_name")} — '
                f'{item.get("error_code")}: {raw_message}'
            )
            st.warning(message)


def render_top_issues(df):
    """Render business NG items without masking file/system errors."""
    # Empty business results can mean a file/system error; do not
    # render a misleading green success message in that case.
    if df.empty:
        return

    ng_df = df[df["status"] == "NG"].copy()

    if ng_df.empty:
        st.success("修正が必要な項目はありません。")
        return

    st.subheader("優先して確認するエラー")

    top = ng_df[
        [
            "file_name",
            "row_no",
            "record_id",
            "check_name",
            "input_value",
            "message",
        ]
    ].head(10)

    top.columns = [
        "ファイル",
        "行",
        "レコードID",
        "チェック内容",
        "入力値",
        "メッセージ",
    ]

    st.dataframe(top, use_container_width=True, hide_index=True)

    if len(ng_df) > 10:
        st.caption(f"NG {len(ng_df)}件のうち、先頭10件を表示しています.")


def render_result_matrix(results, key_prefix):
    df = build_result_df(results)

    if df.empty:
        st.info("表示できるバリデーション結果がありません。")
        return

    st.subheader("検証結果一覧")

    c1, c2 = st.columns(2)

    operation_filter = c1.selectbox(
        "操作種別",
        ["すべて", "登録", "更新", "削除"],
        key=f"{key_prefix}_operation",
    )

    status_filter = c2.selectbox(
        "結果",
        ["すべて", "NG", "OK", "SKIP"],
        key=f"{key_prefix}_status",
    )

    filtered = df.copy()

    if operation_filter != "すべて":
        filtered = filtered[filtered["operation_label"] == operation_filter]

    if status_filter != "すべて":
        filtered = filtered[filtered["status"] == status_filter]

    display_df = filtered.copy()
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

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.download_button(
        "結果CSVをダウンロード",
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"{key_prefix}_validation_result.csv",
        mime="text/csv",
        key=f"{key_prefix}_download",
    )


def render_history(history, active_id):
    if not history:
        st.info("このセッションには過去の検証結果がありません。")
        return

    rows = []
    for run in history:
        summary = run["summary"]
        rows.append(
            {
                "検証ID": run["result_id"],
                "ファイル": summary.get("files", 0),
                "レコード": summary.get("records", 0),
                "NG": summary.get("ng", 0),
                "実行時間(秒)": run.get("elapsed_seconds", 0),
                "表示中": "●" if run["result_id"] == active_id else "",
            }
        )

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
    )

```

```python
import uuid
import streamlit as st


def init_state():
    defaults = {
        "page": "upload",
        "validation_history": [],
        "active_result_id": None,
        "running": False,
        "completed_signature": None,
        "run_requested": False,
        "upload_widget_version": 0,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def new_result_id():
    return f"VAL-{uuid.uuid4().hex[:8].upper()}"


def save_run(results, summary, started_at, finished_at, upload_signature):
    result_id = new_result_id()
    run = {
        "result_id": result_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round(finished_at - started_at, 2),
        "results": results,
        "summary": summary,
        "upload_signature": upload_signature,
    }
    st.session_state.validation_history.insert(0, run)
    st.session_state.active_result_id = result_id
    st.session_state.completed_signature = upload_signature
    return result_id


def get_active_run():
    if not st.session_state.validation_history:
        return None

    active_id = st.session_state.active_result_id
    for run in st.session_state.validation_history:
        if run["result_id"] == active_id:
            return run

    return st.session_state.validation_history[0]


def set_active_result(result_id):
    st.session_state.active_result_id = result_id
    st.session_state.page = "result"


def clear_history():
    st.session_state.validation_history = []
    st.session_state.active_result_id = None
    st.session_state.completed_signature = None


def go_upload(clear_uploader=False):
    st.session_state.page = "upload"
    if clear_uploader:
        # Gives the user a fresh upload field without touching session history.
        st.session_state.upload_widget_version += 1
        st.session_state.completed_signature = None


def go_result():
    if st.session_state.validation_history:
        st.session_state.page = "result"

```

```python
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
    render_result_header,
    render_top_issues,
    render_result_matrix,
    render_file_errors,
    build_result_df,
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
    page_title="{{APP_TITLE}}",
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
  --border:rgba(148,163,184,.18);
  --surface:rgba(255,255,255,.035);
  --muted:rgba(226,232,240,.66);
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
  color:rgba(226,232,240,.58);
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
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    """
<div class="hero-kicker">{{APP_KICKER}}</div>
<h1>{{APP_TITLE}}</h1>
<div class="hero-copy">
{{PARENT_LABEL}} / {{CHILD_LABEL}} を対象とした事前バリデーションです。1回の検証では同一Master・同一OperationのCSVのみ実行できます。
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
            f'{run["result_id"]}｜NG {run["summary"].get("ng", 0)}件'
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
  <div class="rule-line">● 1回の検証では {{PARENT_LABEL}} / {{CHILD_LABEL}} のどちらか一方のみ実行します</div>
  <div class="rule-line">● 登録 / 更新 / 削除は操作種別ごとに分けて実行します</div>
  <div class="rule-line">● 同一Master・同一Operationであれば複数CSVをまとめて検証できます</div>
  <div class="rule-line">● Updateの空欄は「変更なし」として扱います</div>
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
投入ファイル一覧で、Master種別、登録 / 更新 / 削除、準備状態を確認します。

**3. バリデーションを実行**  
「バリデーション実行」を押します。複数CSVの場合も同一Master・同一Operationであればまとめて実行できます。

**4. 検証結果を確認**  
実行完了後は自動で「検証結果」画面に移動します。OK / NG / SKIP とエラー内容を確認してください。

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
        render_result_header(active_run)
        render_file_errors(active_run["results"])

        result_df = build_result_df(active_run["results"])
        render_top_issues(result_df)

        st.write("")
        render_result_matrix(
            active_run["results"],
            active_run["result_id"],
        )

```

```python
import csv, hashlib, io
from typing import Any, Literal
MasterType = Literal["{{PARENT_CODE}}", "{{CHILD_CODE}}"]
Operation = Literal['I','U','D']
PARENT_REQUIRED_COLUMNS = {'{{PARENT_ID_COLUMN}}','{{PARENT_DESCRIPTION_COLUMN}}','{{RELATED_ID_COLUMN}}','{{RELATED_DESCRIPTION_COLUMN}}','{{GROUP_COLUMN}}','{{REFERENCE_KEY_A_COLUMN}}','{{REFERENCE_KEY_B_COLUMN}}','UPD_FLAG'}
CHILD_REQUIRED_COLUMNS = {'{{CHILD_ID_COLUMN}}','{{CHILD_PARENT_ID_COLUMN}}','UPD_FLAG'}
PARENT_SIGNATURE_COLUMNS = {'{{RELATED_ID_COLUMN}}','{{REFERENCE_KEY_A_COLUMN}}','{{REFERENCE_KEY_B_COLUMN}}'}
VALID_OPERATIONS = {'I','U','D'}
OPERATION_LABELS = {'I':'登録','U':'更新','D':'削除'}
MASTER_LABELS = {'{{PARENT_CODE}}':'{{PARENT_LABEL}}','{{CHILD_CODE}}':'{{CHILD_LABEL}}'}
class FileValidationError(Exception):
    """Input-file error raised before procedure execution."""
    def __init__(self, code: str, message: str): self.code=code; self.message=message; super().__init__(message)
def upload_signature(uploaded_files: list[Any] | None) -> str:
    digest=hashlib.sha256()
    for f in uploaded_files or []: digest.update(f.name.encode('utf-8')); digest.update(f.getvalue())
    return digest.hexdigest()
def _decode_csv(raw_data: bytes) -> str:
    try: return raw_data.decode('utf-8-sig')
    except UnicodeDecodeError as exc: raise FileValidationError('FILE-001','CSVはUTF-8形式でアップロードしてください。') from exc
def read_csv_file(uploaded_file: Any) -> list[dict[str,str]]:
    reader=csv.DictReader(io.StringIO(_decode_csv(uploaded_file.getvalue())))
    if not reader.fieldnames: raise FileValidationError('FILE-002','CSVヘッダーを読み取れませんでした。')
    headers=[h.strip() if h is not None else '' for h in reader.fieldnames]
    if len(headers)!=len(set(headers)): raise FileValidationError('FILE-002','CSVヘッダーに重複した列名があります。')
    rows=[]
    for row in reader:
        n={}
        for k,v in row.items():
            if k is not None: n[k.strip()]='' if v is None else v.strip()
        if any(v!='' for v in n.values()): rows.append(n)
    if not rows: raise FileValidationError('FILE-001','CSVにデータ行がありません。')
    return rows
def detect_master_type(rows: list[dict[str,str]]) -> MasterType:
    headers=set(rows[0].keys()); p=PARENT_REQUIRED_COLUMNS.issubset(headers); c=CHILD_REQUIRED_COLUMNS.issubset(headers)
    if p and not c: return '{{PARENT_CODE}}'
    if c and not p: return '{{CHILD_CODE}}'
    if PARENT_SIGNATURE_COLUMNS.issubset(headers): return '{{PARENT_CODE}}'
    if '{{CHILD_ID_COLUMN}}' in headers: return '{{CHILD_CODE}}'
    raise FileValidationError('FILE-007','対象Masterを判定できません。CSV項目を確認してください。')
def validate_file(rows: list[dict[str,str]], master_type: MasterType) -> Operation:
    headers=set(rows[0].keys()); required=PARENT_REQUIRED_COLUMNS if master_type=='{{PARENT_CODE}}' else CHILD_REQUIRED_COLUMNS; missing=required-headers
    if missing: raise FileValidationError('FILE-003','必須列が不足しています: '+', '.join(sorted(missing)))
    ops={r.get('UPD_FLAG','').strip().upper() for r in rows}
    if {x for x in ops if x not in VALID_OPERATIONS}: raise FileValidationError('FILE-004','UPD_FLAGは I / U / D のいずれかを指定してください。')
    if len(ops)!=1: raise FileValidationError('FILE-005','1つのCSV内に複数の操作種別が含まれています。操作種別ごとにファイルを分けてください。')
    return next(iter(ops))  # type: ignore[return-value]
def inspect_uploaded_file(uploaded_file: Any) -> dict[str,Any]:
    rows=read_csv_file(uploaded_file); mt=detect_master_type(rows); op=validate_file(rows,mt)
    return {'file_name':uploaded_file.name,'master_type':mt,'master_label':MASTER_LABELS[mt],'operation':op,'operation_label':OPERATION_LABELS[op],'record_count':len(rows),'size_kb':round(len(uploaded_file.getvalue())/1024,1),'status':'準備完了'}
def validate_upload_set(previews: list[dict[str,Any]]) -> tuple[MasterType,Operation]:
    if not previews: raise FileValidationError('FILE-008','検証対象ファイルがありません。')
    masters={x['master_type'] for x in previews}; ops={x['operation'] for x in previews}
    if len(masters)!=1: raise FileValidationError('FILE-008','異なるMasterのCSVが混在しています。1回の検証では同一MasterのCSVのみ投入してください。')
    if len(ops)!=1: raise FileValidationError('FILE-009','登録・更新・削除のCSVが混在しています。1回の検証では同一操作のCSVのみ投入してください。')
    return next(iter(masters)), next(iter(ops))  # type: ignore[return-value]
def build_payload(rows: list[dict[str,str]], master_type: MasterType) -> dict[str,Any]:
    return {'master_type':master_type,'records':[{'row_no':i,'record':r} for i,r in enumerate(rows,start=1)]}

```

```python
import json
from typing import Any
PROCEDURE_NAME = "{{APP_DATABASE}}.{{APP_SCHEMA}}.{{PROCEDURE_NAME}}"
def execute_validation(session: Any, operation: str, file_name: str, payload: dict[str,Any]) -> dict[str,Any]:
    """Call the configured validation Stored Procedure."""
    result=session.call(PROCEDURE_NAME,operation,file_name,json.dumps(payload,ensure_ascii=False))
    return result if isinstance(result,dict) else json.loads(result)

```
