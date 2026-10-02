from pathlib import Path
import argparse

import numpy as np
import pandas as pd


# unit, normal_min, normal_max, mean, std, physical_min
SENSOR_CFG = {
    "temperature": ("C", 35.0, 85.0, 60.0, 8.0, 0.0),
    "vibration": ("mm/s", 0.5, 7.0, 2.8, 1.0, 0.0),
    "pressure": ("bar", 2.0, 10.0, 6.0, 1.2, 0.0),
    "power": ("kW", 5.0, 150.0, 65.0, 20.0, 0.0),
    "rpm": ("rpm", 500.0, 3200.0, 1750.0, 420.0, 0.0),
    "flow": ("m3/h", 10.0, 220.0, 95.0, 30.0, 0.0),
}


EQUIPMENT_TYPES = [
    "motor",
    "pump",
    "compressor",
    "cnc",
    "conveyor",
    "gearbox",
]


def build_reference_data(
    out_dir: Path,
    rng: np.random.Generator,
    n_lines=10,
    n_equipment=320,
    n_sensors=1400,
):
    """
    Генерация справочных файлов:

    production_lines.csv
    equipment.csv
    sensors.csv
    maintenance.csv
    """

    if n_sensors < n_equipment:
        raise ValueError("n_sensors must be >= n_equipment")

    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # Производственные линии
    # ---------------------------------------------------------

    lines = pd.DataFrame({
        "line_id": [
            f"L{i:03d}"
            for i in range(1, n_lines + 1)
        ],

        "line_name": [
            f"Production line {i}"
            for i in range(1, n_lines + 1)
        ],

        "workshop": rng.choice(
            ["A", "B", "C"],
            n_lines,
        ),

        "product_family": rng.choice(
            ["P1", "P2", "P3", "P4"],
            n_lines,
        ),

        "nominal_capacity": rng.integers(
            50,
            220,
            n_lines,
        ),
    })

    lines.to_csv(
        out_dir / "production_lines.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Оборудование
    # ---------------------------------------------------------

    equipment = pd.DataFrame({
        "equipment_id": [
            f"EQ{i:05d}"
            for i in range(1, n_equipment + 1)
        ],

        "line_id": rng.choice(
            lines["line_id"].to_numpy(),
            n_equipment,
        ),

        "equipment_type": rng.choice(
            EQUIPMENT_TYPES,
            n_equipment,
        ),

        "model": [
            f"M-{x:04d}"
            for x in rng.integers(
                100,
                9999,
                n_equipment,
            )
        ],

        "install_year": rng.integers(
            2012,
            2027,
            n_equipment,
        ),

        "criticality": rng.choice(
            ["low", "medium", "high"],
            n_equipment,
            p=[0.25, 0.50, 0.25],
        ),
    })

    equipment.to_csv(
        out_dir / "equipment.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Датчики
    #
    # Каждому оборудованию гарантирован минимум один датчик.
    # ---------------------------------------------------------

    base_assignment = np.arange(n_equipment)

    extra_assignment = rng.integers(
        0,
        n_equipment,
        n_sensors - n_equipment,
    )

    eq_idx = np.concatenate([
        base_assignment,
        extra_assignment,
    ])

    rng.shuffle(eq_idx)

    sensor_ids = [
        f"S{i:06d}"
        for i in range(1, n_sensors + 1)
    ]

    sensor_types = rng.choice(
        list(SENSOR_CFG),
        n_sensors,
    )

    sensors = pd.DataFrame({
        "sensor_id": sensor_ids,

        "equipment_id":
            equipment.iloc[eq_idx]["equipment_id"].to_numpy(),

        "sensor_type": sensor_types,
    })

    sensors["unit"] = (
        sensors["sensor_type"]
        .map(lambda x: SENSOR_CFG[x][0])
    )

    sensors["normal_min"] = (
        sensors["sensor_type"]
        .map(lambda x: SENSOR_CFG[x][1])
    )

    sensors["normal_max"] = (
        sensors["sensor_type"]
        .map(lambda x: SENSOR_CFG[x][2])
    )

    sensors.to_csv(
        out_dir / "sensors.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # История обслуживания
    # ---------------------------------------------------------

    n_maint = max(
        1200,
        n_equipment * 4,
    )

    maintenance = pd.DataFrame({

        "maintenance_id": [
            f"MNT{i:07d}"
            for i in range(1, n_maint + 1)
        ],

        "equipment_id": rng.choice(
            equipment["equipment_id"].to_numpy(),
            n_maint,
        ),

        "start_time":
            pd.to_datetime("2026-01-02")
            + pd.to_timedelta(
                rng.integers(
                    0,
                    238 * 24 * 60,
                    n_maint,
                ),
                unit="m",
            ),

        "duration_min": rng.integers(
            20,
            600,
            n_maint,
        ),

        "maintenance_type": rng.choice(
            [
                "inspection",
                "preventive",
                "repair",
                "calibration",
            ],
            n_maint,
        ),

        "result": rng.choice(
            [
                "ok",
                "adjusted",
                "parts_replaced",
            ],
            n_maint,
            p=[0.58, 0.27, 0.15],
        ),
    })

    maintenance = (
        maintenance
        .sort_values("start_time")
        .reset_index(drop=True)
    )

    maintenance.to_csv(
        out_dir / "maintenance.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Таблица для быстрого сопоставления датчика и оборудования
    # ---------------------------------------------------------

    sensor_lookup = sensors.merge(
        equipment[
            [
                "equipment_id",
                "line_id",
            ]
        ],
        on="equipment_id",
        how="left",
    )

    return (
        lines,
        equipment,
        sensors,
        maintenance,
        sensor_lookup,
    )


def prepare_maintenance_lookup(
    maintenance: pd.DataFrame,
):
    """
    Подготовка таблицы обслуживания для merge_asof().
    """

    return (
        maintenance[
            [
                "equipment_id",
                "start_time",
            ]
        ]
        .sort_values("start_time")
        .reset_index(drop=True)
    )


def build_telemetry_chunk(
    global_start: int,
    n: int,
    total_rows: int,
    sensor_lookup: pd.DataFrame,
    maint_lookup: pd.DataFrame,
    rng: np.random.Generator,
):
    """
    Генерирует один блок телеметрических данных.
    """

    sensor_id = sensor_lookup[
        "sensor_id"
    ].to_numpy()

    equipment_id = sensor_lookup[
        "equipment_id"
    ].to_numpy()

    line_id = sensor_lookup[
        "line_id"
    ].to_numpy()

    sensor_type = sensor_lookup[
        "sensor_type"
    ].to_numpy()

    n_sensors = len(sensor_lookup)

    base_ts = pd.Timestamp(
        "2026-01-01 00:00:00"
    )

    horizon_seconds = (
        240 * 24 * 3600
    )

    # ---------------------------------------------------------
    # Глобальные номера событий
    # ---------------------------------------------------------

    row_numbers = np.arange(
        global_start,
        global_start + n,
        dtype=np.int64,
    )

    # ---------------------------------------------------------
    # Датчики выбираются циклически.
    #
    # Благодаря этому каждый датчик получает приблизительно
    # одинаковое количество событий.
    # ---------------------------------------------------------

    idx = (
        row_numbers
        % n_sensors
    )

    # ---------------------------------------------------------
    # Равномерное распределение событий по 240 дням
    # ---------------------------------------------------------

    seconds = (
        row_numbers
        * horizon_seconds
        // total_rows
    ).astype(np.int64)

    timestamps = (
        base_ts
        + pd.to_timedelta(
            seconds,
            unit="s",
        )
    )

    stype = sensor_type[idx]

    # ---------------------------------------------------------
    # Массивы измерений
    # ---------------------------------------------------------

    values = np.empty(
        n,
        dtype=float,
    )

    normal_min = np.empty(
        n,
        dtype=float,
    )

    normal_max = np.empty(
        n,
        dtype=float,
    )

    physical_min = np.empty(
        n,
        dtype=float,
    )

    # ---------------------------------------------------------
    # Генерация нормальных значений
    # ---------------------------------------------------------

    for current_type, cfg in SENSOR_CFG.items():

        mask = (
            stype
            == current_type
        )

        count = int(
            mask.sum()
        )

        if count == 0:
            continue

        values[mask] = rng.normal(
            cfg[3],
            cfg[4],
            count,
        )

        normal_min[mask] = cfg[1]
        normal_max[mask] = cfg[2]
        physical_min[mask] = cfg[5]

    # ---------------------------------------------------------
    # Нормальные значения остаются в рабочем диапазоне
    # ---------------------------------------------------------

    values = np.clip(
        values,
        normal_min,
        normal_max,
    )

    # ---------------------------------------------------------
    # Проверяем наличие недавнего обслуживания
    # ---------------------------------------------------------

    event_keys = pd.DataFrame({
        "timestamp": timestamps,

        "equipment_id":
            equipment_id[idx],
    })

    event_keys = (
        event_keys
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    recent_maint = pd.merge_asof(
        event_keys,
        maint_lookup,

        left_on="timestamp",
        right_on="start_time",

        by="equipment_id",

        direction="backward",
    )

    hours_since_maint = (
        (
            recent_maint["timestamp"]
            - recent_maint["start_time"]
        )
        .dt.total_seconds()
        / 3600.0
    )

    after_maintenance = (
        hours_since_maint
        .between(
            0,
            48,
            inclusive="both",
        )
        .fillna(False)
        .to_numpy()
    )

    # ---------------------------------------------------------
    # Вероятность аномалии
    #
    # После обслуживания она уменьшается.
    # ---------------------------------------------------------

    anomaly_probability = np.where(
        after_maintenance,
        0.0025,
        0.0070,
    )

    anomaly_mask = (
        rng.random(n)
        < anomaly_probability
    )

    anomaly_high = (
        rng.random(n)
        < 0.65
    )

    span = (
        normal_max
        - normal_min
    )

    deviation = (
        rng.uniform(
            0.10,
            0.45,
            n,
        )
        * span
    )

    high_mask = (
        anomaly_mask
        & anomaly_high
    )

    low_mask = (
        anomaly_mask
        & ~anomaly_high
    )

    # Аномальное превышение диапазона
    values[high_mask] = (
        normal_max[high_mask]
        + deviation[high_mask]
    )

    # Аномальное снижение
    values[low_mask] = np.maximum(
        physical_min[low_mask],

        normal_min[low_mask]
        - deviation[low_mask],
    )

    # ---------------------------------------------------------
    # Качество данных
    # ---------------------------------------------------------

    quality_r = rng.random(n)

    quality = np.where(
        quality_r < 0.985,
        "good",

        np.where(
            quality_r < 0.997,
            "suspect",
            "bad",
        ),
    )

    # ---------------------------------------------------------
    # Код тревоги
    # ---------------------------------------------------------

    alarm = np.where(
        anomaly_mask,
        "OUT_OF_RANGE",
        None,
    )

    # ---------------------------------------------------------
    # Итоговый DataFrame
    # ---------------------------------------------------------

    chunk = pd.DataFrame({

        "event_id": [
            f"EV{x:012d}"
            for x in range(
                global_start + 1,
                global_start + n + 1,
            )
        ],

        "timestamp": timestamps,

        "sensor_id":
            sensor_id[idx],

        "equipment_id":
            equipment_id[idx],

        "line_id":
            line_id[idx],

        "value":
            np.round(
                values,
                4,
            ),

        "quality":
            quality,

        "alarm_code":
            alarm,
    })

    return chunk


def build_telemetry_files(
    out_dir: Path,
    sensor_lookup: pd.DataFrame,
    maintenance: pd.DataFrame,
    rng: np.random.Generator,
    total_rows: int,
    chunk_size: int,
    file_count: int = 5,
):
    """
    Генерирует несколько отдельных файлов telemetry_XX.csv.

    total_rows — общее число строк во всех файлах.
    file_count — число создаваемых telemetry-файлов.
    """

    if file_count <= 0:
        raise ValueError(
            "file_count must be > 0"
        )

    if total_rows < file_count:
        raise ValueError(
            "total_rows must be >= file_count"
        )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    maint_lookup = (
        prepare_maintenance_lookup(
            maintenance
        )
    )

    # ---------------------------------------------------------
    # Удаляем старые telemetry-файлы
    # ---------------------------------------------------------

    for old_file in out_dir.glob(
        "telemetry_*.csv"
    ):
        old_file.unlink()

    # ---------------------------------------------------------
    # Распределяем строки между файлами
    #
    # Например:
    #
    # 5 000 000 строк / 5 файлов
    # =
    # 1 000 000 строк на файл
    # ---------------------------------------------------------

    rows_per_file = (
        total_rows
        // file_count
    )

    remainder = (
        total_rows
        % file_count
    )

    global_start = 0

    for file_index in range(
        file_count
    ):

        # Остаток распределяется по первым файлам
        current_file_rows = (
            rows_per_file
            + (
                1
                if file_index < remainder
                else 0
            )
        )

        target = (
            out_dir
            / f"telemetry_{file_index + 1:02d}.csv"
        )

        print()
        print(
            f"Creating {target.name}"
        )

        print(
            f"Rows: {current_file_rows:,}"
        )

        written = 0

        while written < current_file_rows:

            n = min(
                chunk_size,
                current_file_rows - written,
            )

            chunk = build_telemetry_chunk(
                global_start=global_start,
                n=n,
                total_rows=total_rows,
                sensor_lookup=sensor_lookup,
                maint_lookup=maint_lookup,
                rng=rng,
            )

            chunk.to_csv(
                target,
                mode=(
                    "w"
                    if written == 0
                    else "a"
                ),
                header=(
                    written == 0
                ),
                index=False,
            )

            written += n
            global_start += n

            print(
                f"  {written:,}"
                f" / "
                f"{current_file_rows:,}"
            )

        print(
            f"Finished: {target.name}"
        )


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Industrial IoT synthetic "
            "dataset generator"
        )
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=5_000_000,
        help=(
            "Total telemetry rows "
            "across all files"
        ),
    )

    parser.add_argument(
        "--files",
        type=int,
        default=5,
        help=(
            "Number of telemetry CSV files"
        ),
    )

    parser.add_argument(
        "--chunk",
        type=int,
        default=200_000,
        help=(
            "Rows generated in one chunk"
        ),
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    args = parser.parse_args()

    # ---------------------------------------------------------
    # Каталог, в котором находится сам generator.py
    # ---------------------------------------------------------

    out_dir = (
        Path(__file__)
        .resolve()
        .parent
    )

    print(
        "Output directory:"
    )

    print(
        out_dir
    )

    print()

    rng = np.random.default_rng(
        args.seed
    )

    (
        _,
        _,
        _,
        maintenance,
        sensor_lookup,
    ) = build_reference_data(
        out_dir,
        rng,
    )

    build_telemetry_files(
        out_dir=out_dir,
        sensor_lookup=sensor_lookup,
        maintenance=maintenance,
        rng=rng,
        total_rows=args.rows,
        chunk_size=args.chunk,
        file_count=args.files,
    )

    print()
    print(
        "Dataset generation completed."
    )


if __name__ == "__main__":
    main()
