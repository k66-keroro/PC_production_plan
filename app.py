import streamlit as st
import pandas as pd
from src.processors.data_merger import get_merged_data
from src.importers.data_importer import import_data_from_files
from src.utils.config import UI_CONFIG
from src.database.connection import get_db_connection
import os

# --- ページ設定 ---
st.set_page_config(layout="wide", page_title=UI_CONFIG['title'])

# --- データ取得 ---
@st.cache_data(ttl=600) # 10分間キャッシュ
def get_completion_history():
    """完了履歴テーブルをDBから読み込む"""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM completion_history", conn)
        conn.close()
        return df
    except Exception as e:
        # テーブルがない場合も空のDataFrameを返す
        print(f"履歴テーブルの読み込みに失敗: {e}")
        return pd.DataFrame(columns=['子指図番号', '完了日', '基準計画終了日'])

# --- UIコンポーネント ---
def display_compliance_dashboard():
    """遵守率ダッシュボードを表示する"""
    st.header("📈 遵守率ダッシュボード")

    history_df = get_completion_history()

    if history_df.empty:
        st.warning("完了履歴データがまだありません。")
        return

    history_df['完了日'] = pd.to_datetime(history_df['完了日'])
    history_df['基準計画終了日'] = pd.to_datetime(history_df['基準計画終了日'])

    # 遵守状況を計算
    history_df['遵守'] = history_df['完了日'] <= history_df['基準計画終了日']

    today = pd.Timestamp.now()

    # 週次遵守率
    start_of_week = today - pd.to_timedelta(today.dayofweek, unit='d')
    weekly_completed = history_df[history_df['完了日'] >= start_of_week]

    if not weekly_completed.empty:
        weekly_rate = weekly_completed['遵守'].mean() * 100
        st.metric(
            label="今週の遵守率",
            value=f"{weekly_rate:.1f}%",
            delta=f"{weekly_completed['遵守'].sum()}件 / {len(weekly_completed)}件"
        )
    else:
        st.metric(label="今週の遵守率", value="N/A", delta="今週の完成実績なし")

    # 月次遵守率
    start_of_month = today.replace(day=1)
    monthly_completed = history_df[history_df['完了日'] >= start_of_month]

    if not monthly_completed.empty:
        monthly_rate = monthly_completed['遵守'].mean() * 100
        st.metric(
            label="今月の遵守率",
            value=f"{monthly_rate:.1f}%",
            delta=f"{monthly_completed['遵守'].sum()}件 / {len(monthly_completed)}件"
        )
    else:
        st.metric(label="今月の遵守率", value="N/A", delta="今月の完成実績なし")

@st.cache_data
def to_excel(df):
    import io
    output = io.BytesIO()
    # xlsxwriter をエンジンとして指定
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='生産計画データ')
        # 列幅を自動調整
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            writer.sheets['生産計画データ'].set_column(col_idx, col_idx, column_length)
    processed_data = output.getvalue()
    return processed_data

# --- メインロジック ---

# --- タイトル ---
st.title(UI_CONFIG['title'])
st.caption("製造指図データ (ZP02) と部品構成データ (ZP51N) を統合し、生産計画の進捗を可視化します。")

# --- セッションステートの初期化 ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df = pd.DataFrame()
    st.session_state.last_update_time = None

# --- サイドバー ---
with st.sidebar:
    st.header("⚙️ 操作パネル")
    
    if st.button('🔄 データ更新', type="primary"):
        with st.spinner('ステップ1/2: データファイルをインポートしています...'):
            import_success = import_data_from_files()
        
        if import_success:
            st.toast("✅ ファイルのインポートが完了しました。")
            with st.spinner('ステップ2/2: データを統合・整形しています...'):
                df = get_merged_data()
                if not df.empty:
                    st.session_state.df = df
                    st.session_state.data_loaded = True
                    st.session_state.last_update_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                    # キャッシュをクリアしてダッシュボードを強制的に更新
                    st.cache_data.clear()
                    st.success('データの更新が完了しました。')
                else:
                    st.error('データの統合に失敗しました。詳細はコンソールログを確認してください。')
        else:
            st.error('データファイルのインポートに失敗しました。詳細はコンソールログを確認してください。')

    if st.session_state.data_loaded:
        st.info(f"最終更新: {st.session_state.last_update_time}")
        display_compliance_dashboard()
        
        excel_data = to_excel(st.session_state.df)
        st.download_button(
            label="📥 Excel形式で全件ダウンロード",
            data=excel_data,
            file_name=f"production_plan_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# --- メインコンテンツ ---
if st.session_state.data_loaded:
    st.header("📊 統合データ一覧")
    
    df_display = st.session_state.df.copy()

    # --- フィルター機能 ---
    with st.expander("絞り込みフィルター", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # 所要日フィルター
            df_display['所要日_dt'] = pd.to_datetime(df_display['所要日'], errors='coerce')
            min_date = df_display['所要日_dt'].min()
            max_date = df_display['所要日_dt'].max()
            if pd.notna(min_date) and pd.notna(max_date):
                date_range = st.date_input("所要日の範囲", value=(min_date, max_date), min_value=min_date, max_value=max_date, format="YYYY/MM/DD")
                if len(date_range) == 2:
                    df_display = df_display[df_display['所要日_dt'].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))]
            else:
                st.info("所要日データがありません。")

        with col2:
            # 子指図計画終了日フィルター
            df_display['計画終了日_dt'] = pd.to_datetime(df_display['子指図計画終了日'], errors='coerce')
            min_p_date = df_display['計画終了日_dt'].min()
            max_p_date = df_display['計画終了日_dt'].max()
            if pd.notna(min_p_date) and pd.notna(max_p_date):
                p_date_range = st.date_input("子指図計画終了日の範囲", value=(min_p_date, max_p_date), min_value=min_p_date, max_value=max_p_date, format="YYYY/MM/DD")
                if len(p_date_range) == 2:
                    df_display = df_display[df_display['計画終了日_dt'].between(pd.to_datetime(p_date_range[0]), pd.to_datetime(p_date_range[1]))]
            else:
                st.info("計画終了日データがありません。")

        with col3:
            # 生産タイプフィルター
            prod_types = sorted(df_display['生産タイプ'].dropna().unique())
            selected_types = st.multiselect("生産タイプ", options=prod_types, default=prod_types)
            df_display = df_display[df_display['生産タイプ'].isin(selected_types)]

        with col4:
            # 遵守状況フィルター
            compliance_status = sorted(df_display['遵守状況'].dropna().unique())
            selected_compliance = st.multiselect("遵守状況", options=compliance_status, default=compliance_status)
            df_display = df_display[df_display['遵守状況'].isin(selected_compliance)]

    # 表示するカラムの順番を定義
    display_columns = [
        'No', '親指図番号', '親品目コード', '親品目テキスト', '子指図番号', '子品目コード',
        '子品目テキスト', '所要日', '子指図計画開始日', '子指図計画終了日', '計画数量',
        '進捗', '完了日', '基準計画終了日', '子MRP管理者', '生産タイプ', '遵守状況'
    ]
    # 存在しないカラムを除外
    display_columns = [col for col in display_columns if col in df_display.columns]

    st.dataframe(
        df_display[display_columns],
        height=600,
        use_container_width=True,
        hide_index=True
    )
    st.info(f"全 {len(st.session_state.df)} 件中 {len(df_display)} 件を表示しています。")
else:
    st.info('サイドバーの「データ更新」ボタンをクリックして、最新の生産計画データを表示してください。')
