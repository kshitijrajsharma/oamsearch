import json

import duckdb
import geopandas as gpd
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

pd.options.plotting.backend = "plotly"


def fetch_iso3_bboxes():
    try:
        response = requests.get(
            "https://raw.githubusercontent.com/kshitijrajsharma/global-boundaries-bbox/refs/heads/main/bbox.json"
        )
        response.raise_for_status()
        bbox_data = response.json()
        return bbox_data
    except Exception as e:
        st.error(f"Error fetching country data: {str(e)}")
        return {}


def calculate_bbox(geojson):
    features = geojson["features"]
    bounds = []
    for feature in features:
        geometry = feature["geometry"]
        if geometry["type"] == "Polygon":
            bounds.extend(geometry["coordinates"][0])
        elif geometry["type"] == "MultiPolygon":
            for polygon in geometry["coordinates"]:
                bounds.extend(polygon[0])

    min_x = min(coord[0] for coord in bounds)
    min_y = min(coord[1] for coord in bounds)
    max_x = max(coord[0] for coord in bounds)
    max_y = max(coord[1] for coord in bounds)

    return [min_x, min_y, max_x, max_y]


def fetch_openaerialmap_data(bbox=None, from_date=None, to_date=None):
    base_url = "https://api.openaerialmap.org/meta"
    params = {"limit": 100}

    if bbox:
        params["bbox"] = ",".join(map(str, bbox))

    if from_date:
        params["acquisition_from"] = from_date.strftime("%Y-%m-%d")

    if to_date:
        params["acquisition_to"] = to_date.strftime("%Y-%m-%d")

    all_results = []
    page = 1
    while True:
        params["page"] = page
        response = requests.get(base_url, params=params)
        data = response.json()

        all_results.extend(data["results"])

        total_results = data["meta"]["found"]
        limit = data["meta"]["limit"]
        if len(all_results) >= total_results:
            break
        page += 1

    return all_results


def create_geodataframe(data):
    features = []
    for result in data:
        for key, value in result.items():
            if key not in [
                "properties",
                "bbox",
                "footprint",
                "user",
                "projection",
                "meta_uri",
                "__v",
                "geojson",
            ]:
                result["properties"][key] = value
        properties = result["properties"]
        geometry = result["geojson"]
        features.append({"geometry": geometry, "properties": properties})

    gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
    gdf_proj = gdf.to_crs(epsg=3857)
    gdf_proj["area_sqm"] = gdf_proj.geometry.area
    gdf_proj_back = gdf_proj.to_crs(epsg=4326)
    gdf_proj_back["geometry"] = gdf_proj_back.geometry.envelope

    return gdf_proj_back


def create_chart(df, x_col, y_col, chart_type, time_interval="year"):
    try:
        df[x_col] = pd.to_datetime(df[x_col], errors="coerce")

        if time_interval == "year":
            df["time_group"] = df[x_col].dt.year
        elif time_interval == "month":
            df["time_group"] = df[x_col].dt.to_period("M").astype(str)
        else:
            raise ValueError("Unsupported time_interval. Use 'year' or 'month'.")

        grouped_df = df.groupby(["time_group", y_col]).size().reset_index(name="count")
        grouped_df = grouped_df.sort_values(by="time_group")

        if chart_type == "line":
            fig = px.line(
                grouped_df,
                x="time_group",
                y="count",
                color=y_col,
                title=f"{y_col} Uploads by {time_interval.capitalize()}",
                markers=True,
            )
        elif chart_type == "bar":
            fig = px.bar(
                grouped_df,
                x="time_group",
                y="count",
                color=y_col,
                title=f"{y_col} Uploads by {time_interval.capitalize()}",
            )
        else:
            raise ValueError("Unsupported chart type. Use 'line' or 'bar'.")

        return fig

    except Exception as e:
        st.error(f"Error creating chart: {str(e)}")
        return None


def execute_duckdb_query(df, query):
    try:
        con = duckdb.connect(database=":memory:")
        con.register("data", df)
        result = con.execute(query).fetchdf()
        return result
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None


st.set_page_config(page_title="Search OpenAerialMap Metadata", layout="wide")
st.title("Search OpenAerialMap Metadata")

default_x_col = "uploaded_at"
default_y_col = "platform"
default_chart_type = "line"
default_time_interval = "year"


area_selection_method = st.radio(
    "Choose area selection method:",
    ["Select Country (ISO3)", "Upload GeoJSON", "Paste GeoJSON"],
)


bbox = None
geojson = None

if area_selection_method == "Select Country (ISO3)":
    iso3_data = fetch_iso3_bboxes()
    if iso3_data:
        country_codes = sorted(iso3_data.keys())
        selected_country = st.selectbox("Select Country (ISO3)", country_codes)
        if selected_country:
            bbox = iso3_data[selected_country]
            st.write(f"Selected country bbox: {bbox}")

elif area_selection_method == "Upload GeoJSON":
    uploaded_file = st.file_uploader("Upload a GeoJSON file", type=["geojson"])
    if uploaded_file is not None:
        geojson = json.load(uploaded_file)
        bbox = calculate_bbox(geojson)
        st.write(f"Calculated Bounding Box: {bbox}")

else:
    geojson_text = st.text_area("Paste GeoJSON here")
    if geojson_text:
        try:
            geojson = json.loads(geojson_text)
            bbox = calculate_bbox(geojson)
            st.success("GeoJSON successfully parsed")
            st.write(f"Calculated Bounding Box: {bbox}")
        except json.JSONDecodeError:
            st.error("Invalid GeoJSON")

from_date = st.date_input("From Date (optional)", value=None, key="from_date")
to_date = st.date_input("To Date (optional)", value=None, key="to_date")

x_col = st.text_input("Chart X-axis column", value=default_x_col)
y_col = st.text_input("Chart Y-axis column", value=default_y_col)
chart_type = st.selectbox(
    "Chart type", ["line", "bar"], index=0 if default_chart_type == "line" else 1
)
time_interval = st.selectbox(
    "Chart Time interval",
    ["year", "month"],
    index=0 if default_time_interval == "year" else 1,
)
default_query = """SELECT platform, COUNT(*) as count , sum(area_sqm) as area_covered
FROM data 
GROUP BY platform 
ORDER BY count DESC"""
query = st.text_area("Custom SQL Stats", value=default_query)

if st.button("Fetch Data"):
    if not bbox and not from_date and not to_date:
        st.warning("No filters selected. This might return a large amount of data")

    with st.spinner("Fetching data..."):
        data = fetch_openaerialmap_data(bbox, from_date, to_date)
        if data:
            gdf = create_geodataframe(data)
            st.session_state["gdf"] = gdf

            st.subheader("Result")
            st.text(f"Total Features: {len(gdf)}")
            df = pd.DataFrame(gdf)
            df.drop("geometry", axis=1, inplace=True)
            st.write(df.head(100))

            st.subheader("Charts")
            fig = create_chart(df, x_col, y_col, chart_type, time_interval)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("Custom SQL Query")
            if query:
                result = execute_duckdb_query(df, query)
                if result is not None:
                    st.write(f"Query returned {len(result)} rows")
                    st.write(result)
