"""Port congestion dashboard: reads the Gold and ops Delta tables directly, without Spark.

Run: streamlit run dashboard/app.py -- --env prod
"""

from __future__ import annotations

import argparse
import sys

import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from ais_pipeline.config import load_settings
from ais_pipeline.paths import table_path

PORT_NAMES = {"DKEBJ": "Esbjerg", "DKAAR": "Aarhus", "DKCPH": "Copenhagen"}


def _env() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="prod", choices=["dev", "prod", "ci"])
    return parser.parse_args(sys.argv[1:]).env


@st.cache_data(ttl=300)
def load(env: str, layer: str, name: str) -> pd.DataFrame:
    """A whole (small) Delta table as pandas; empty if the table does not exist yet."""
    path = table_path(load_settings(env), layer, name)
    try:
        return DeltaTable(str(path)).to_pandas()
    except Exception:  # noqa: BLE001 - a missing table is a normal state before the first run
        return pd.DataFrame()


def congestion_page(env: str) -> None:
    df = load(env, "gold", "port_congestion_hourly")
    if df.empty:
        st.info("No congestion data yet: run the gold stage.")
        return
    port = st.selectbox("Port", sorted(df["port_id"].unique()), format_func=PORT_NAMES.get)
    groups = sorted(df["ship_type_group"].unique())
    chosen = st.multiselect("Ship type groups", groups, default=groups)
    d = df[(df["port_id"] == port) & (df["ship_type_group"].isin(chosen))]
    hourly = (
        d.groupby("hour_ts")[["vessels_in_port", "vessels_at_anchor", "arrivals", "departures"]]
        .sum()
        .reset_index()
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Peak vessels in port", int(hourly["vessels_in_port"].max()))
    c2.metric("Peak waiting at anchor", int(hourly["vessels_at_anchor"].max()))
    c3.metric("Arrivals", int(hourly["arrivals"].sum()))

    long = hourly.melt(
        "hour_ts", ["vessels_in_port", "vessels_at_anchor"], var_name="measure", value_name="n"
    )
    chart = (
        alt.Chart(long)
        .mark_line()
        .encode(
            x=alt.X("hour_ts:T", title="Hour (UTC)"),
            y=alt.Y("n:Q", title="Vessels"),
            color=alt.Color("measure:N", title=None),
            tooltip=["hour_ts:T", "measure:N", "n:Q"],
        )
        .properties(height=320)
    )
    st.altair_chart(chart, width="stretch")

    st.subheader("Busiest hours")
    st.dataframe(hourly.sort_values("vessels_in_port", ascending=False).head(10), hide_index=True)


def port_calls_page(env: str) -> None:
    df = load(env, "gold", "port_calls")
    if df.empty:
        st.info("No port calls yet: run the gold stage.")
        return
    port = st.selectbox(
        "Port", sorted(df["port_id"].unique()), format_func=PORT_NAMES.get, key="calls_port"
    )
    d = df[df["port_id"] == port]
    st.metric("Port calls", len(d))
    box = (
        alt.Chart(d)
        .mark_boxplot()
        .encode(
            x=alt.X("ship_type_group:N", title="Ship type group"),
            y=alt.Y("dwell_h:Q", title="Dwell (hours)", scale=alt.Scale(type="symlog")),
        )
        .properties(height=320)
    )
    st.altair_chart(box, width="stretch")
    st.subheader("Longest calls")
    cols = ["mmsi", "ship_type_group", "arrival_ts", "departure_ts", "dwell_h", "n_positions"]
    st.dataframe(d.nlargest(10, "dwell_h")[cols], hide_index=True)


def health_page(env: str) -> None:
    runs = load(env, "ops", "pipeline_runs")
    if runs.empty:
        st.info("No pipeline runs logged yet.")
        return
    stale = runs[runs["status"] == "stale"]
    if not stale.empty:
        days = sorted(stale["source_date"].astype(str).unique())
        st.warning(f"Stale dates (failed a quality gate): {', '.join(days)}")
    st.subheader("Last runs")
    cols = [
        "ended_at",
        "stage",
        "source_date",
        "status",
        "rows_in",
        "rows_out",
        "rows_rejected",
        "message",
    ]
    st.dataframe(runs.sort_values("ended_at", ascending=False)[cols].head(50), hide_index=True)

    dq = load(env, "ops", "dq_metrics")
    if dq.empty:
        return
    st.subheader("Data quality before and after Silver")
    day = st.selectbox("Date", sorted(dq["source_date"].unique(), reverse=True))
    d = dq[dq["source_date"] == day]
    d = d[d["run_id"] == d.sort_values("logged_at")["run_id"].iloc[-1]]
    table = d.pivot_table(index="metric", columns="checkpoint", values="value", aggfunc="last")
    st.dataframe(table, width="stretch")


def main() -> None:
    env = _env()
    st.set_page_config(page_title="AIS port congestion", layout="wide")
    st.title("Port congestion: Esbjerg, Aarhus, Copenhagen")
    st.caption(f"Gold tables, environment `{env}`. Times are UTC.")
    tab1, tab2, tab3 = st.tabs(["Congestion", "Port calls", "Pipeline health"])
    with tab1:
        congestion_page(env)
    with tab2:
        port_calls_page(env)
    with tab3:
        health_page(env)


main()
