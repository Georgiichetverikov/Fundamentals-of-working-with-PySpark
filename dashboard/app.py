from pathlib import Path
import duckdb
import plotly.express as px
import streamlit as st

BASE = Path("/app/output")

st.set_page_config(page_title="Промышленный мониторинг IoT", layout="wide")
st.title("🏭 Промышленный мониторинг IoT-телеметрии")


@st.cache_data(ttl=300, show_spinner=False)
def load_parquet_data(dir_or_file_name: str):
    target_path = BASE / dir_or_file_name
    if not target_path.exists():
        return None
    try:
        with duckdb.connect() as con:
            if target_path.is_dir():
                query = (
                    f"SELECT * FROM read_parquet('{target_path}/**/*.parquet')"
                )
            else:
                query = f"SELECT * FROM read_parquet('{target_path}')"
            return con.execute(query).df()
    except Exception as e:
        st.error(f"Ошибка чтения витрины `{dir_or_file_name}`: {e}")
        return None


# Загрузка витрин
source_files = load_parquet_data("source_file_kpi")
line_kpi_df = load_parquet_data("line_kpi")
hourly_metrics_df = load_parquet_data("hourly_metrics")
v5_data = load_parquet_data("variant_5_kpi.parquet")
v10_data = load_parquet_data("variant_10_kpi.parquet")

if source_files is None or line_kpi_df is None:
    st.error("⚠️ Базовые витрины данных не найдены в `/app/output/`!")
    st.stop()

tab_load, tab_lines, tab_hourly, tab5, tab10 = st.tabs([
    "📦 Контроль загрузки",
    "🏭 KPI Линий и Цехов",
    "⏱️ Почасовая динамика",
    "📊 Вариант 5 (Качество)",
    "⚙️ Вариант 10 (RPM)",
])

# Вкладка 1: Контроль загрузки
with tab_load:
    st.header("Статистика импорта CSV-файлов")
    c1, c2 = st.columns(2)
    c1.metric("Обработано файлов", len(source_files))
    if "events" in source_files.columns:
        c2.metric("Всего событий", f"{int(source_files['events'].sum()):,}")

    fig = px.bar(
        source_files,
        x="source_file",
        y="events",
        title="Количество записей по исходным CSV-файлам",
        labels={"source_file": "Исходный файл", "events": "Число событий"},
        text_auto=True,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(source_files, use_container_width=True)

# Вкладка 2: KPI Линий
with tab_lines:
    st.header("Анализ производственных линий")
    workshops = (
        ["Все"] + list(line_kpi_df["workshop"].dropna().unique())
        if "workshop" in line_kpi_df.columns
        else ["Все"]
    )
    selected_ws = st.selectbox("Фильтр по цеху", workshops)

    filtered = (
        line_kpi_df
        if selected_ws == "Все" or "workshop" not in line_kpi_df.columns
        else line_kpi_df[line_kpi_df["workshop"] == selected_ws]
    )

    m1, m2 = st.columns(2)
    if "line_id" in filtered.columns:
        m1.metric("Всего линий", int(filtered["line_id"].nunique()))
    if "alarms" in filtered.columns:
        m2.metric("Тревог (Alarms)", f"{int(filtered['alarms'].sum()):,}")

    fig_lines = px.bar(
        filtered,
        x="line_name" if "line_name" in filtered.columns else "line_id",
        y="alarms" if "alarms" in filtered.columns else filtered.columns[0],
        title="Количество тревог по линиям",
        text_auto=True,
    )
    st.plotly_chart(fig_lines, use_container_width=True)
    st.dataframe(filtered, use_container_width=True)

# Вкладка 3: Почасовая динамика
with tab_hourly:
    st.header("Сводные метрики по датчикам")
    if hourly_metrics_df is not None and not hourly_metrics_df.empty:
        fig_sensors = px.bar(
            hourly_metrics_df,
            x="sensor_type",
            y="events",
            color="workshop" if "workshop" in hourly_metrics_df.columns else None,
            title="Распределение событий по типам датчиков",
            barmode="group",
        )
        st.plotly_chart(fig_sensors, use_container_width=True)
        st.dataframe(hourly_metrics_df.head(100), use_container_width=True)
    else:
        st.info("Информация о почасовой динамике отсутствует.")

# Вкладка 4: Вариант 5 (Качество измерений)
with tab5:
    st.header("Вариант 5: Линии с наибольшей долей bad/suspect quality")
    if v5_data is not None and not v5_data.empty:
        top_bad_line = v5_data.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric(
            "Худшая линия по качеству",
            str(top_bad_line.get("line_name", top_bad_line.get("line_id"))),
        )
        c2.metric(
            "Макс. доля проблемных записей",
            f"{top_bad_line.get('problem_ratio_pct', 0)}%",
        )
        bad_sum = top_bad_line.get("bad_events", 0) + top_bad_line.get(
            "suspect_events", 0
        )
        c3.metric("Всего некачественных событий", f"{int(bad_sum):,}")

        fig5 = px.bar(
            v5_data,
            x="line_name" if "line_name" in v5_data.columns else "line_id",
            y="problem_ratio_pct",
            color="workshop" if "workshop" in v5_data.columns else None,
            title="Процент проблемных измерений (bad + suspect) по линиям",
            text_auto=True,
            labels={
                "line_name": "Линия",
                "problem_ratio_pct": "Проблема (%)",
            },
        )
        st.plotly_chart(fig5, use_container_width=True)
        st.dataframe(v5_data, use_container_width=True)
    else:
        st.warning("Витрина `variant_5_kpi.parquet` не найдена!")

# Вкладка 5: Вариант 10 (Профиль RPM)
with tab10:
    st.header("Вариант 10: Профиль RPM по типам оборудования")
    if v10_data is not None and not v10_data.empty:
        fig10 = px.bar(
            v10_data,
            x="equipment_type",
            y="avg_rpm",
            color="sensor_type",
            title="Средняя скорость вращения (RPM) по типам оборудования",
            barmode="group",
            text_auto=True,
            labels={
                "equipment_type": "Тип оборудования",
                "avg_rpm": "Средний RPM",
            },
        )
        st.plotly_chart(fig10, use_container_width=True)
        st.dataframe(v10_data, use_container_width=True)
    else:
        st.warning("Витрина `variant_10_kpi.parquet` не найдена!")