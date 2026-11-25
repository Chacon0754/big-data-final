import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
from bson.decimal128 import Decimal128
from pymongo import MongoClient
from pymongo.errors import PyMongoError

Document = dict[str, Any]
AMBER = "#B56800"
BLUE = "#167D98"
DAY_NAMES = ("Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom")


@st.cache_data(ttl=60, show_spinner=False)
def load_report(mongo_uri: str) -> Document | None:
    client: MongoClient[Document] = MongoClient(
        mongo_uri, serverSelectionTimeoutMS=5_000
    )
    with client:
        return client["nyc_taxi"]["mobility_reports"].find_one(
            sort=[("period_end_exclusive", -1), ("_id", -1)]
        )


def show_demand(tables: dict[str, list[Document]]) -> None:
    hourly: pd.DataFrame = pd.DataFrame(tables["demand_by_hour"])
    trend: alt.Chart = alt.Chart(hourly).mark_line(point=True, color=AMBER).encode(
        x=alt.X("pickup_hour:Q", title="Hora de recogida", axis=alt.Axis(tickMinStep=1)),
        y=alt.Y("trip_count:Q", title="Viajes"),
        tooltip=[
            alt.Tooltip("pickup_hour:Q", title="Hora"),
            alt.Tooltip("trip_count:Q", title="Viajes", format=",d"),
        ],
    )
    st.subheader("Viajes por hora")
    st.altair_chart(trend, width="stretch")

    weekday: pd.DataFrame = pd.DataFrame(tables["amounts_by_weekday"])[
        ["iso_weekday", "trip_count"]
    ].copy()
    weekday["Día"] = weekday["iso_weekday"].map(dict(enumerate(DAY_NAMES, start=1)))

    profile: pd.DataFrame = pd.DataFrame(tables["demand_by_day_type_hour"]).rename(
        columns={"pickup_hour": "Hora", "avg_trips_per_day": "Viajes por día"}
    )
    profile["Tipo de día"] = profile["day_type"].map({
        "weekday": "Lunes a viernes",
        "weekend": "Fin de semana",
    })

    left, right = st.columns(2)
    with left:
        st.subheader("Viajes por día de la semana")
        st.altair_chart(
            alt.Chart(weekday).mark_bar(color=BLUE).encode(
                x=alt.X("Día:N", sort=list(DAY_NAMES)),
                y=alt.Y("trip_count:Q", title="Viajes"),
                tooltip=[
                    alt.Tooltip("Día:N"),
                    alt.Tooltip("trip_count:Q", title="Viajes", format=",d"),
                ],
            ),
            width="stretch",
        )
    with right:
        st.subheader("Entre semana y fin de semana")
        st.line_chart(profile, x="Hora", y="Viajes por día", color="Tipo de día")
        st.caption("Promedio por día de cada grupo para cada hora.")


def show_zones(tables: dict[str, list[Document]]) -> None:
    st.subheader("Mapa de calor de recogidas")
    zones: pd.DataFrame = pd.DataFrame(tables["demand_by_zone_hour"])
    boroughs: list[str] = sorted(zones["pickup_borough"].dropna().unique().tolist())
    borough: str = st.selectbox("Distrito de origen", ["Todos", *boroughs])
    if borough != "Todos":
        zones = zones.loc[zones["pickup_borough"] == borough].copy()

    zones["Zona"] = zones["pickup_zone"] + " · " + zones["pickup_borough"]
    ranking: list[str] = (
        zones.groupby("Zona")["trip_count"].sum().nlargest(10).index.tolist()
    )
    heatmap: pd.DataFrame = (
        zones.pivot_table(
            index="Zona", columns="pickup_hour", values="trip_count",
            aggfunc="sum", fill_value=0,
        )
        .reindex(index=ranking, columns=range(24), fill_value=0)
        .reset_index()
        .melt(id_vars="Zona", var_name="Hora", value_name="Viajes")
    )
    heatmap["Hora"] = heatmap["Hora"].astype(int)

    chart: alt.Chart = alt.Chart(heatmap).mark_rect().encode(
        x=alt.X(
            "Hora:O", title="Hora de recogida", sort=list(range(24)),
            axis=alt.Axis(labelAngle=0),
        ),
        y=alt.Y("Zona:N", title=None, sort=ranking, axis=alt.Axis(labelLimit=280)),
        color=alt.Color("Viajes:Q", scale=alt.Scale(scheme="yelloworangered")),
        tooltip=[
            alt.Tooltip("Zona:N"),
            alt.Tooltip("Hora:O"),
            alt.Tooltip("Viajes:Q", format=",d"),
        ],
    ).properties(height=max(160, 27 * len(ranking)))

    st.altair_chart(chart, width="stretch")
    st.caption("Hasta diez zonas con más recogidas en el distrito seleccionado.")

    pickup, dropoff = st.columns(2)
    for column, title, key in (
        (pickup, "Top 10 de orígenes", "top_pickup_zones"),
        (dropoff, "Top 10 de destinos", "top_dropoff_zones"),
    ):
        with column:
            st.subheader(title)
            ranking_table: pd.DataFrame = pd.DataFrame(tables[key])[
                ["zone", "borough", "trip_count"]
            ].rename(columns={"zone": "Zona", "borough": "Distrito", "trip_count": "Viajes"})
            st.dataframe(ranking_table, hide_index=True, width="stretch")


def decimal_to_float(value: Decimal128 | None) -> float | None:
    return float(value.to_decimal()) if value is not None else None


def show_amounts(tables: dict[str, list[Document]]) -> None:
    labels: dict[str, str] = {
        "Credit card": "Tarjeta",
        "Cash": "Efectivo",
        "Flex Fare": "Flex Fare",
        "Dispute": "En disputa",
        "No charge": "Sin cargo",
        "Unknown": "Desconocido",
        "Voided trip": "Anulado",
        "Missing code": "Sin código",
        "Unmapped code": "Código sin clasificar",
    }
    payments: pd.DataFrame = pd.DataFrame(tables["amounts_by_payment"])
    payments["Forma de pago"] = payments["payment_method"].replace(labels)
    payments["Cobro total (USD)"] = payments["reported_total_usd"].map(decimal_to_float)
    payments["Promedio por viaje (USD)"] = payments["avg_reported_total_usd"].map(
        decimal_to_float
    )
    payments = payments.rename(columns={"trip_count": "Viajes"})

    st.subheader("Cobros por forma de pago")
    st.bar_chart(
        payments[["Forma de pago", "Cobro total (USD)"]],
        x="Forma de pago", y="Cobro total (USD)", color=AMBER,
    )
    st.dataframe(
        payments[["Forma de pago", "Viajes", "Cobro total (USD)", "Promedio por viaje (USD)"]],
        hide_index=True,
        width="stretch",
    )

    zones: pd.DataFrame = pd.DataFrame(tables["amounts_by_pickup_zone"]).head(10).copy()
    zones["Cobro total (USD)"] = zones["reported_total_usd"].map(decimal_to_float)
    zones = zones.rename(columns={"pickup_zone": "Zona", "pickup_borough": "Distrito"})
    st.subheader("Zonas con mayor cobro total")
    st.dataframe(zones[["Zona", "Distrito", "Cobro total (USD)"]], hide_index=True, width="stretch")

    weekday: pd.DataFrame = pd.DataFrame(tables["amounts_by_weekday"])
    weekday["Día"] = weekday["iso_weekday"].map(dict(enumerate(DAY_NAMES, start=1)))
    weekday["Cobro total (USD)"] = weekday["reported_total_usd"].map(decimal_to_float)
    st.subheader("Cobro total por día de la semana")
    st.dataframe(weekday[["Día", "Cobro total (USD)"]], hide_index=True, width="stretch")


def show_trip_metrics(tables: dict[str, list[Document]]) -> None:
    labels: dict[str, str] = {
        "pickup_hour": "Hora",
        "avg_duration_min": "Duración media (min)",
        "median_duration_min": "Duración mediana (min)",
        "avg_distance_km": "Distancia media (km)",
        "median_distance_km": "Distancia mediana (km)",
        "pickup_zone": "Aeropuerto",
        "trip_count": "Viajes",
    }
    metrics: pd.DataFrame = pd.DataFrame(tables["trip_metrics_by_hour"]).rename(columns=labels)

    left, right = st.columns(2)
    with left:
        st.subheader("Duración por hora")
        st.line_chart(metrics, x="Hora", y=["Duración media (min)", "Duración mediana (min)"])
    with right:
        st.subheader("Distancia por hora")
        st.line_chart(metrics, x="Hora", y=["Distancia media (km)", "Distancia mediana (km)"])

    st.subheader("Salidas de aeropuertos")
    airports: pd.DataFrame = pd.DataFrame(tables["airport_pickup_metrics"]).rename(columns=labels)
    st.dataframe(
        airports[[
            "Aeropuerto", "Viajes", "Duración media (min)", "Duración mediana (min)",
            "Distancia media (km)", "Distancia mediana (km)",
        ]],
        hide_index=True,
        width="stretch",
    )


def main() -> None:
    st.set_page_config(page_title="Movilidad en Nueva York", layout="wide")
    st.title("Movilidad urbana en Nueva York")

    mongo_uri: str = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27087")
    try:
        report = load_report(mongo_uri)
    except PyMongoError:
        st.error("No se pudo conectar a MongoDB. Comprueba que el servicio esté activo.")
        return

    if report is None:
        st.info("Publica el reporte de Spark en MongoDB para ver los indicadores.")
        return

    tables: dict[str, list[Document]] = report["indicators"]
    start: date = date.fromisoformat(report["period_start"])
    end: date = date.fromisoformat(report["period_end_exclusive"]) - timedelta(days=1)
    st.caption(f"NYC TLC · Yellow Taxi · {start:%d/%m/%Y} al {end:%d/%m/%Y}")

    peak: Document = max(tables["demand_by_hour"], key=lambda row: row["trip_count"])
    total_usd: Decimal = sum(
        (
            row["reported_total_usd"].to_decimal()
            for row in tables["amounts_by_payment"]
            if row["reported_total_usd"] is not None
        ),
        Decimal("0"),
    )

    trips, hour, charges = st.columns(3)
    trips.metric("Viajes válidos", f"{report['valid_trip_count']:,}")
    hour.metric("Hora pico de recogidas", f"{peak['pickup_hour']:02d}:00")
    charges.metric("Cobro total registrado", f"USD {total_usd:,.2f}")

    demand_tab, zones_tab, amounts_tab, metrics_tab = st.tabs(
        ["Demanda", "Zonas", "Cobros", "Duración y distancia"]
    )
    with demand_tab:
        show_demand(tables)
    with zones_tab:
        show_zones(tables)
    with amounts_tab:
        show_amounts(tables)
    with metrics_tab:
        show_trip_metrics(tables)


if __name__ == "__main__":
    main()