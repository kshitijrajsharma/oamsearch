import json

import geopandas as gpd
import pandas as pd
import requests
import streamlit as st


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


def fetch_openaerialmap_data(bbox, from_date=None, to_date=None):
    base_url = "https://api.openaerialmap.org/meta"
    params = {
        "bbox": ",".join(map(str, bbox)),
    }

    if from_date:
        params["acquisition_from"] = from_date.strftime("%Y-%m-%d")

    if to_date:
        params["acquisition_to"] = to_date.strftime("%Y-%m-%d")

    response = requests.get(base_url, params=params)
    data = response.json()
    return data


def fetch_openaerialmap_data(bbox, from_date=None, to_date=None):
    base_url = "https://api.openaerialmap.org/meta"
    params = {"bbox": ",".join(map(str, bbox)), "limit": 100}

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

    gdf = gpd.GeoDataFrame.from_features(features)
    return gdf


st.set_page_config(page_title="Search OpenAerialMap Metadata", layout="wide")

st.title("Search OpenAerialMap Metadata")

uploaded_file = st.file_uploader("Upload a GeoJSON file", type=["geojson"])

geojson_text = st.text_area("or paste GeoJSON here")

geojson = None

if uploaded_file is not None:
    geojson = json.load(uploaded_file)

elif geojson_text:
    try:
        geojson = json.loads(geojson_text)
    except json.JSONDecodeError:
        st.error("Invalid GeoJSON")

if geojson:
    bbox = calculate_bbox(geojson)
    st.write(f"Calculated Bounding Box: {bbox}")

    from_date = st.date_input("From Date (optional)", value=None, key="from_date")
    to_date = st.date_input("To Date (optional)", value=None, key="to_date")

    if st.button("Fetch Data"):
        with st.spinner("Fetching data..."):
            data = fetch_openaerialmap_data(bbox, from_date, to_date)
            gdf = create_geodataframe(data)

            st.subheader("Result")
            st.text(f"Total Features : {len(gdf)}")
            df = pd.DataFrame(gdf)
            df.drop("geometry", axis=1, inplace=True)
            st.write(df.head(100))
            if len(gdf) > 0:

                geojson_data = gdf.to_json()
                st.download_button(
                    label="Download GeoJSON",
                    data=geojson_data,
                    file_name="openaerialmap_data.geojson",
                    mime="application/json",
                )

                csv_data = gdf.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv_data,
                    file_name="openaerialmap_data.csv",
                    mime="text/csv",
                )
