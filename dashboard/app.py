from pathlib import Path
import duckdb
import streamlit as st
import plotly.express as px

BASE = Path("/app/output")

st.set_page_config(page_title="Промышленный мониторинг IoT", layout="wide")
st.title("🏭 Промышленный мониторинг IoT-телеметрии")

@st.cache_data(ttl=300, show_spinner=False)
def get_source_file_kpi():
    target_dir = BASE / "source_file_kpi"
    if not target_dir.exists():
        return None
    with duckdb.connect() as con:
        # Загружаем только готовые агрегаты из витрины
        return con.execute(f"SELECT * FROM read_parquet('{target_dir}/*.parquet') ORDER BY source_file").df()

@st.cache_data(ttl=300, show_spinner=False)
def get_line_kpi():
    target_dir = BASE / "line_kpi"
    if not target_dir.exists():
        return None
    with duckdb.connect() as con:
        return con.execute(f"SELECT * FROM read_parquet('{target_dir}/*.parquet')").df()

@st.cache_data(ttl=300, show_spinner=False)
def get_hourly_summary():
    target_dir = BASE / "hourly_metrics"
    if not target_dir.exists():
        return None
    with duckdb.connect() as con:
        # Агрегируем данные прямо в DuckDB, отдаем в Python НЕ БОЛЕЕ 1000 строк!
        return con.execute(f"""
            SELECT sensor_type, SUM(events) as total_events, SUM(alarms) as total_alarms
            FROM read_parquet('{target_dir}/*.parquet')
            GROUP BY sensor_type
            LIMIT 500
        """).df()

source_files = get_source_file_kpi()
line_kpi = get_line_kpi()
hourly_metrics = get_hourly_summary()

if source_files is None or line_kpi is None:
    st.error("⚠️ Витрины данных не найдены в `/app/output/`!")
    st.stop()

tab_load, tab_lines, tab_hourly = st.tabs(["Контроль загрузки", "KPI Линий и Цехов", "Почасовая динамика"])

# -------------------------------------------------------------
# Вкладка 1: Контроль загрузки
# -------------------------------------------------------------
with tab_load:
    st.header("Статистика импорта CSV-файлов")
    
    c1, c2 = st.columns(2)
    c1.metric("Обработано файлов", len(source_files))
    c2.metric("Всего событий", f"{int(source_files['events'].sum()):,}")

    fig = px.bar(source_files, x="source_file", y="events", title="Записи по исходным CSV-файлам")
    st.plotly_chart(fig, use_container_width=True)

    # Ограничиваем вывод таблицы максимум 100 строками
    st.dataframe(source_files.head(100), use_container_width=True)

# -------------------------------------------------------------
# Вкладка 2: KPI Линий и Цехов
# -------------------------------------------------------------
with tab_lines:
    st.header("Анализ производственных линий")

    workshops = ["Все"] + list(line_kpi["workshop"].dropna().unique()) if "workshop" in line_kpi.columns else ["Все"]
    selected_ws = st.selectbox("Фильтр по цеху", workshops)

    filtered = line_kpi if selected_ws == "Все" else line_kpi[line_kpi["workshop"] == selected_ws]

    m1, m2 = st.columns(2)
    m1.metric("Всего линий", int(filtered["line_id"].nunique()) if "line_id" in filtered.columns else len(filtered))
    m2.metric("Тревог (Alarms)", int(filtered["alarms"].sum()) if "alarms" in filtered.columns else 0)

    if "alarms" in filtered.columns and "line_name" in filtered.columns:
        fig_lines = px.bar(filtered, x="line_name", y="alarms", title="Тревоги по линиям")
        st.plotly_chart(fig_lines, use_container_width=True)

    st.dataframe(filtered.head(100), use_container_width=True)

# -------------------------------------------------------------
# Вкладка 3: Почасовая динамика
# -------------------------------------------------------------
with tab_hourly:
    st.header("Сводные метрики по датчикам")
    if hourly_metrics is not None and not hourly_metrics.empty:
        fig_sensors = px.bar(hourly_metrics, x="sensor_type", y="total_events", color="total_alarms", title="События по типам датчиков")
        st.plotly_chart(fig_sensors, use_container_width=True)
        st.dataframe(hourly_metrics, use_container_width=True)