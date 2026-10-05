from pathlib import Path
import time
import duckdb
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# Задание 7.1 - 7.3
spark = (
    SparkSession.builder.appName("IndustrialIoTLab")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

telemetry_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("equipment_id", StringType(), False),
        StructField("line_id", StringType(), False),
        StructField("value", DoubleType(), True),
        StructField("quality", StringType(), True),
        StructField("alarm_code", StringType(), True),
    ]
)

telemetry = (
    spark.read.option("header", True)
    .schema(telemetry_schema)
    .csv("/app/scripts/telemetry_*.csv")
    .withColumn(
        "source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
    )
)

print("Схема DataFrame")
telemetry.printSchema()
telemetry.show(25, truncate=False)

print("\nПодсчет общей суммы строк")
rows = telemetry.count()
print("total rows:", rows)
print("partitions:", telemetry.rdd.getNumPartitions())

source_stats = (
    telemetry.groupBy("source_file").count().orderBy("source_file")
)
source_stats.show(truncate=False)
print("source files:", source_stats.count())

# Задание 7.4
one_file = (
    spark.read.option("header", True)
    .schema(telemetry_schema)
    .csv("/app/scripts/telemetry_01.csv")
)

all_files = (
    spark.read.option("header", True)
    .schema(telemetry_schema)
    .csv("/app/scripts/telemetry_*.csv")
)

print("Количество строк в telemetry_01.csv:", one_file.count())
print("Количество строк во всех 5 файлах:", all_files.count())

print("Партиций для 1 файла:", one_file.rdd.getNumPartitions())
print("Партиций для 5 файлов:", all_files.rdd.getNumPartitions())

# Задание 8.1
telemetry.select(
    "source_file", "timestamp", "sensor_id", "equipment_id", "value"
).show(20, truncate=False)

telemetry.filter(F.col("quality") == "good").show(20)

# Задание 8.2
telemetry2 = (
    telemetry.withColumn("event_date", F.to_date("timestamp"))
    .withColumn("hour", F.hour("timestamp"))
    .withColumn(
        "has_alarm", F.when(F.col("alarm_code").isNotNull(), 1).otherwise(0)
    )
)

# Задание 8.3
by_source = (
    telemetry2.groupBy("source_file", "line_id")
    .agg(
        F.count("*").alias("events"),
        F.sum("has_alarm").alias("alarms"),
    )
    .orderBy("source_file", F.desc("alarms"))
)

by_source.show(50, truncate=False)

# Задание 9
telemetry2.createOrReplaceTempView("telemetry")

spark.sql("""
SELECT
    source_file,
    line_id,
    COUNT(*) AS events,
    SUM(has_alarm) AS alarms
FROM telemetry
GROUP BY source_file, line_id
ORDER BY source_file, alarms DESC
""").show(100, truncate=False)

spark.sql("""
    SELECT
        sensor_id,
        value,
        CASE
            WHEN value IS NULL THEN 'missing'
            WHEN alarm_code IS NOT NULL THEN 'alarm'
            ELSE 'normal'
        END AS event_class
    FROM telemetry
    LIMIT 20
""").show()

# Задание 10
# Чтение справочных таблиц 10.1
lines = spark.read.option("header", True).option("inferSchema", True).csv(
    "/app/scripts/production_lines.csv"
)
equipment = spark.read.option("header", True).option("inferSchema", True).csv(
    "/app/scripts/equipment.csv"
)
sensors = spark.read.option("header", True).option("inferSchema", True).csv(
    "/app/scripts/sensors.csv"
)
maintenance = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("/app/scripts/maintenance.csv")
    .withColumn("start_time", F.to_timestamp("start_time"))
)

# Задание 10.2
full_events = (
    telemetry2.join(sensors, on=["sensor_id", "equipment_id"], how="left")
    .join(equipment, on=["equipment_id", "line_id"], how="left")
    .join(lines, on="line_id", how="left")
)

full_events.select(
    "source_file",
    "timestamp",
    "workshop",
    "line_name",
    "equipment_type",
    "sensor_type",
    "value",
    "unit",
).show(20, truncate=False)

# Задание 10.3
print(
    "Null sensor_type count:",
    full_events.filter(F.col("sensor_type").isNull()).count(),
)
print(
    "Null equipment_type count:",
    full_events.filter(F.col("equipment_type").isNull()).count(),
)
print(
    "Null line_name count:",
    full_events.filter(F.col("line_name").isNull()).count(),
)

# Задание 10.4
full_events.createOrReplaceTempView("iot")

sensor_line_stats = spark.sql("""
    SELECT
        workshop,
        line_name,
        sensor_type,
        COUNT(*) AS events,
        ROUND(AVG(value), 3) AS avg_value,
        ROUND(STDDEV(value), 3) AS std_value,
        SUM(CASE WHEN alarm_code IS NOT NULL THEN 1 ELSE 0 END) AS alarms
    FROM iot
    WHERE quality = 'good'
    GROUP BY workshop, line_name, sensor_type
    ORDER BY alarms DESC
""")

sensor_line_stats.show(20, truncate=False)

# Задание 11.1
w = Window.partitionBy("sensor_id").orderBy("timestamp")

with_prev = full_events.withColumn(
    "prev_value", F.lag("value").over(w)
).withColumn("delta", F.col("value") - F.col("prev_value"))

# Задание 11.2
w10 = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(-9, 0)

rolling = with_prev.withColumn("moving_avg_10", F.avg("value").over(w10))

# Задание 11.3
# Фильтруем корректные измерения
clean = full_events.filter(
    (F.col("quality") == "good") & F.col("value").isNotNull()
)

# 1. Быстрый расчет статистики по типам
stat_by_type = clean.groupBy("equipment_type", "sensor_type").agg(
    F.avg("value").alias("mean_by_type"),
    F.stddev_samp("value").alias("std_by_type"),
)

# 2. Присоединение через Broadcast Join и расчёт z-score
# 2. Присоединение через Broadcast Join и расчёт z-score
scored = (
    with_prev
    .join(
        F.broadcast(stat_by_type),
        ["equipment_type", "sensor_type"],
        "left"
    )
    .withColumn(
        "z_score",
        F.when(
            F.col("std_by_type") > 0,
            (F.col("value") - F.col("mean_by_type")) / F.col("std_by_type"),
        )
    )
    .withColumn(
        "is_stat_anomaly",
        F.when(F.abs(F.col("z_score")) >= 3.0, 1).otherwise(0),
    )
)

# Отдельный датафрейм только с найденными аномалиями
anomalies = scored.filter(F.col("is_stat_anomaly") == 1)
print("Найдено статистических аномалий:", anomalies.count())

# Задание 12.1-12.2
query = (
    full_events.filter(F.col("sensor_type") == "temperature")
    .groupBy("line_id")
    .agg(F.avg("value").alias("avg_temperature"))
)

query.explain("formatted")

# Задание 12.3
full_events.cache()
full_events.count()

full_events.groupBy("sensor_type").count().show()
full_events.groupBy("line_id").count().show()

full_events.unpersist()

# Задание 12.5
# Замер времени чтения одного CSV
t0 = time.time()
spark.read.option("header", True).schema(telemetry_schema).csv(
    "/app/scripts/telemetry_01.csv"
).groupBy("quality").count().collect()
print(f"Время обработки 1 CSV: {time.time() - t0:.2f} сек")

# Замер времени чтения всех 5 CSV
t0 = time.time()
spark.read.option("header", True).schema(telemetry_schema).csv(
    "/app/scripts/telemetry_*.csv"
).groupBy("quality").count().collect()
print(f"Время обработки 5 CSV: {time.time() - t0:.2f} сек")

# Задание 13.1 — ДОБАВЛЕНО ПОЛЕ "quality"
(
    scored.select(
        "source_file",
        "event_date",
        "timestamp",
        "workshop",
        "line_id",
        "line_name",
        "equipment_id",
        "equipment_type",
        "sensor_id",
        "sensor_type",
        "value",
        "quality",
        "unit",
        "alarm_code",
        "has_alarm",
        "z_score",
        "is_stat_anomaly",
    )
    .write.mode("overwrite")
    .partitionBy("event_date")
    .parquet("/app/output/clean_telemetry")
)

# Задание 13.2
source_file_kpi = (
    telemetry2.groupBy("source_file")
    .agg(
        F.count("*").alias("events"),
        F.min("timestamp").alias("first_timestamp"),
        F.max("timestamp").alias("last_timestamp"),
        F.sum("has_alarm").alias("alarms"),
        F.sum(F.when(F.col("quality") == "good", 1).otherwise(0)).alias(
            "good_events"
        ),
        F.sum(F.when(F.col("quality") == "suspect", 1).otherwise(0)).alias(
            "suspect_events"
        ),
        F.sum(F.when(F.col("quality") == "bad", 1).otherwise(0)).alias(
            "bad_events"
        ),
    )
    .orderBy("source_file")
)

source_file_kpi.write.mode("overwrite").parquet("/app/output/source_file_kpi")

# Задание 13.3
line_kpi = scored.groupBy("workshop", "line_id", "line_name").agg(
    F.count("*").alias("events"),
    F.countDistinct("equipment_id").alias("equipment_count"),
    F.sum("has_alarm").alias("alarms"),
    F.sum("is_stat_anomaly").alias("anomalies"),
)
line_kpi.write.mode("overwrite").parquet("/app/output/line_kpi")

# Задание 13.4
hourly_metrics = scored.groupBy(
    "workshop", "event_date", "hour", "sensor_type", "unit"
).agg(
    F.count("*").alias("events"),
    F.sum("value").alias("sum_value"),
    F.min("value").alias("min_value"),
    F.max("value").alias("max_value"),
    F.sum("has_alarm").alias("alarms"),
    F.sum("is_stat_anomaly").alias("anomalies"),
)
hourly_metrics.write.mode("overwrite").parquet("/app/output/hourly_metrics")

# Задание 13.5
(
    anomalies.select(
        "timestamp",
        "workshop",
        "line_id",
        "line_name",
        "equipment_id",
        "equipment_type",
        "sensor_id",
        "sensor_type",
        "value",
        "unit",
        "alarm_code",
        "z_score",
        "delta",
    )
    .write.mode("overwrite")
    .parquet("/app/output/sensor_anomalies")
)

# Задание 13.6
equipment_kpi = scored.groupBy(
    "workshop",
    "line_id",
    "line_name",
    "equipment_id",
    "equipment_type",
    "criticality",
    "sensor_type",
    "unit",
).agg(
    F.count("*").alias("events"),
    F.sum("has_alarm").alias("alarms"),
    F.sum("is_stat_anomaly").alias("anomalies"),
)
equipment_kpi.write.mode("overwrite").parquet("/app/output/equipment_kpi")

# Задание 13.7
maintenance_window = (
    maintenance.join(
        equipment.select("equipment_id", "line_id"), "equipment_id", "left"
    )
    .join(lines.select("line_id", "line_name", "workshop"), "line_id", "left")
    .withColumn("window_start", F.expr("start_time - INTERVAL 24 HOURS"))
    .withColumn("window_end", F.expr("start_time + INTERVAL 24 HOURS"))
)

maintenance_events = (
    scored.alias("e")
    .join(
        F.broadcast(maintenance_window).alias("m"),
        (F.col("e.equipment_id") == F.col("m.equipment_id"))
        & (F.col("e.timestamp") >= F.col("m.window_start"))
        & (F.col("e.timestamp") < F.col("m.window_end")),
        "inner",
    )
    .select(
        F.col("m.workshop").alias("workshop"),
        F.col("m.line_id").alias("line_id"),
        F.col("m.line_name").alias("line_name"),
        F.col("m.maintenance_type").alias("maintenance_type"),
        F.col("e.timestamp").alias("timestamp"),
        F.col("e.has_alarm").alias("has_alarm"),
        F.col("e.is_stat_anomaly").alias("is_stat_anomaly"),
        F.when(F.col("e.timestamp") < F.col("m.start_time"), "before")
        .otherwise("after")
        .alias("period"),
    )
)

maintenance_effect = (
    maintenance_events.groupBy(
        "workshop", "line_id", "line_name", "maintenance_type", "period"
    )
    .agg(
        F.count("*").alias("events"),
        F.sum("has_alarm").alias("alarms"),
        F.sum("is_stat_anomaly").alias("anomalies"),
    )
    .withColumn("alarm_rate", F.col("alarms") / F.col("events"))
    .withColumn("anomaly_rate", F.col("anomalies") / F.col("events"))
)
maintenance_effect.write.mode("overwrite").parquet(
    "/app/output/maintenance_effect"
)

OUTPUT_DIR = Path("/app/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

print("Расчет Варианта 5: Доля bad/suspect quality по линиям...")
con.execute(f"""
    COPY (
        SELECT 
            line_id,
            line_name,
            workshop,
            COUNT(*) as total_events,
            COUNT(CASE WHEN quality = 'bad' THEN 1 END) as bad_events,
            COUNT(CASE WHEN quality = 'suspect' THEN 1 END) as suspect_events,
            COUNT(CASE WHEN quality = 'good' THEN 1 END) as good_events,
            ROUND(COUNT(CASE WHEN quality IN ('bad', 'suspect') THEN 1 END) * 100.0 / COUNT(*), 2) as problem_ratio_pct
        FROM read_parquet('/app/output/clean_telemetry/*/*.parquet')
        GROUP BY line_id, line_name, workshop
        ORDER BY problem_ratio_pct DESC
    ) TO '{OUTPUT_DIR}/variant_5_kpi.parquet' (FORMAT PARQUET);
""")

print("Расчет Варианта 10: Профиль RPM по типам оборудования...")
con.execute(f"""
    COPY (
        SELECT 
            equipment_type,
            sensor_type,
            COUNT(*) as sample_count,
            ROUND(AVG(value), 2) as avg_rpm,
            ROUND(MIN(value), 2) as min_rpm,
            ROUND(MAX(value), 2) as max_rpm,
            ROUND(STDDEV(value), 2) as std_rpm
        FROM read_parquet('/app/output/clean_telemetry/*/*.parquet')
        WHERE LOWER(sensor_type) LIKE '%rpm%'
        GROUP BY equipment_type, sensor_type
        ORDER BY avg_rpm DESC
    ) TO '{OUTPUT_DIR}/variant_10_kpi.parquet' (FORMAT PARQUET);
""")

print("Все витрины успешно сгенерированы!")