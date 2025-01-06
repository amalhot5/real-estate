# specify buildings dataset to use via command line args
# e.g. python buildings_to_streets.py data/nyc_geos.csv
# the data needed to run this script can be found here:
# https://drive.google.com/file/d/1VayQEMG6d9jwonLbfLJyBPdwCiD2fw6l/view?usp=sharing
# download and save the data/ folder in the same folder as these scripts
# Note: this contains a mapping for zip codes to boroughs and the NYC streets data pulled in Q2-2024
import geopandas as gpd
import shapely
import folium
import pandas as pd
import json

from sys import argv
from tqdm.auto import tqdm
import warnings

warnings.filterwarnings(action="once")

STREET_BUFFER_STANDARD = 15
STREET_BUFFER_NO_MATCHES = 40


def load_shape(x):
    try:
        return shapely.wkt.loads(x)
    except:
        return shapely.geometry.shape(json.loads(x))


def load_geos(df: pd.DataFrame) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame(df[(~pd.isna(df.geometry))])
    gdf["geometry"] = [load_shape(x) for x in tqdm(gdf["geometry"], total=len(gdf))]
    gdf = gdf.set_geometry("geometry")
    gdf.set_crs(crs="EPSG:4326", inplace=True)
    return gdf


def line_to_polygon(
    geo: shapely.LineString, street_width: float, feet_buffer=0
) -> shapely.Polygon:
    return gpd.GeoSeries(
        shapely.buffer(geo, (street_width + feet_buffer) / 364567.2, cap_style="flat"),
        crs="EPSG:4326",
    )[0]


def add_borough(s_segments: pd.DataFrame) -> pd.DataFrame:
    print("adding borough")
    zips_x_borough = pd.read_csv("data/nyc_borough_by_zip.csv")
    zips_x_borough.drop_duplicates(inplace=True)
    s_segments["ZIP"] = s_segments["L_ZIP"].combine_first(s_segments["R_ZIP"])
    correct_zip = (
        s_segments[
            (pd.isna(s_segments["ZIP"])) & (s_segments["ST_NAME"] != "CONNECTOR")
        ]
        .to_crs(crs="3857")
        .sjoin_nearest(
            s_segments[
                (~pd.isna(s_segments["ZIP"])) & (s_segments["ST_NAME"] != "CONNECTOR")
            ].to_crs(crs="3857")
        )[["ZIP_right", "PHYSICALID_left"]]
        .drop_duplicates(subset="PHYSICALID_left")
    )

    print(len(s_segments))
    ret_df = s_segments.merge(
        correct_zip, how="left", left_index=True, right_index=True
    )
    ret_df["ZIP"] = ret_df["ZIP"].combine_first(ret_df["ZIP_right"])
    ret_df.dropna(subset="ZIP", inplace=True)
    ret_df["ZIP"] = ret_df["ZIP"].astype(int)
    ret_df = ret_df.merge(
        zips_x_borough, how="inner", left_on="ZIP", right_on="zip_code"
    )
    print(len(ret_df))
    return ret_df


def load_streets(with_borough=False):
    streets = gpd.read_file("data/Centerline_20240520")
    streets.to_crs(crs="EPSG:4326", inplace=True)
    # remove irreveleant street segments
    streets = streets[
        ~(
            (
                streets["ST_LABEL"].isin(
                    [
                        "ALLEY",
                        "DRIVEWAY",
                        "CONNECTOR",
                        "UNNAMED ST",
                        "WHITESTONE EXPRESSWAY NB EN",
                        "THROGS NECK EXPRESSWAY SB ET",
                        "United Nations Avenue N",
                        "United Nations Avenue S",
                    ]
                )
            )
            | (streets["ST_LABEL"].str.contains("FERRY RTE"))
            | (streets["ST_LABEL"].str.contains("FERRY MAINTENANCE"))
            | (streets["ST_LABEL"].str.contains("FERRY ROUTE"))
            | (streets["ST_LABEL"].str.contains("BIKE PTH"))
            | (streets["ST_LABEL"].str.contains("BIKE PATH"))
            | (streets["ST_LABEL"].str.contains("BIKE CONN"))
            | (streets["ST_LABEL"].str.contains("TUNL"))
            | (streets["ST_LABEL"].str.contains(" STEPS"))
            | (streets["ST_LABEL"].str.contains("BELT PARKWAY"))
            | (streets["ST_LABEL"].str.contains("HUTCHINSON RIVER"))
            | (streets["ST_LABEL"].str.contains("HUTCHINSON RVR"))
            | (streets["ST_LABEL"].str.contains("DRIVEWY"))
            | (streets["ST_LABEL"].str.contains("PEDESTIRAN"))
            | (streets["ST_LABEL"].str.contains("PORT RICHMOND WATER PLANT"))
            |
            # (streets['ST_LABEL'].str.contains('HWY')) |
            # (streets['ST_LABEL'].str.contains('EXPY')) |
            (streets["ST_LABEL"].str.contains(" ET "))  # exit abbreviation
            | (streets["ST_LABEL"].str.contains(" EN "))  # entrance abbreviation
            | (streets["ST_LABEL"].str.contains("NB EN"))
            | (streets["ST_LABEL"].str.contains("SB EN"))
            | (streets["ST_LABEL"].str.contains("NB ET"))
            | (streets["ST_LABEL"].str.contains("SB ET"))
            | (streets["ST_LABEL"].str.contains("PARK EN"))
            | (streets["ST_LABEL"].str.contains("PARK ET"))
            | (streets["ST_LABEL"].str.contains("EXIT"))
            | (streets["ST_LABEL"].str.contains("RAMP"))
            | (streets["ST_LABEL"].str.contains("ENTRANCE"))
            | (streets["ST_LABEL"].str.contains("DVWY"))
            | (streets["ST_LABEL"].str.contains("BQE"))
            | (streets["ST_LABEL"].str.contains("BROOKLYN QUEENS EXPY"))
            | (streets["ST_LABEL"].str.contains("LONG ISLAND EXPY"))
            | (streets["ST_LABEL"].str.contains("NASSAU EXPY"))
            | (streets["ST_LABEL"].str.contains("CROSS BRONX EXPY"))
            | (streets["ST_LABEL"].str.contains("GOWANUS EXPY"))
            | (streets["ST_LABEL"].str.contains("CLEARVIEW EXPY"))
            | (streets["ST_LABEL"].str.contains("WHITESTONE EXPY"))
            | (streets["ST_LABEL"].str.contains("JOHN F KENNEDY EXPY"))
            | (streets["ST_LABEL"].str.contains("MAJOR DEEGAN EXPY"))
            | (streets["ST_LABEL"].str.contains("CROSS BRONX EXPY"))
            | (streets["ST_LABEL"].str.contains("THROGS NECK EXPY"))
            | (streets["ST_LABEL"].str.contains("PARK TRAIL"))
            | (streets["ST_LABEL"].str.contains(" BRG"))
            | (streets["ST_LABEL"].str.contains(" BRIDGE"))
            | (streets["ST_LABEL"].str.contains("TRL"))
            | (streets["ST_LABEL"].str.contains("PED PTH"))
            | (streets["ST_LABEL"].str.contains("PED PATH"))
            | (streets["ST_LABEL"].str.contains("PARK PTH"))
            | (streets["ST_LABEL"].str.contains("BIKE AND PED PTH"))
            | (streets["ST_LABEL"].str.contains("PARK PATH"))
            | (streets["ST_LABEL"].str.contains("ACCESS RD"))
            | (streets["ST_LABEL"].str.contains("ACCESS ROAD"))
            | (streets["ST_LABEL"].str.contains(" CONNECTOR"))
            | (streets["ST_LABEL"].str.contains("CORRECTIONAL"))
            | (streets["ST_LABEL"].str.contains("OVERPASS"))
            | (streets["ST_LABEL"].str.contains("UNDERPASS"))
            | (streets["ST_LABEL"].str.contains(" OPAS"))
            | (streets["ST_LABEL"].str.contains("GREENWAY"))
            |
            # (streets['ST_LABEL'].str.contains('PARKWAY')) |
            # (streets['ST_LABEL'].str.contains('PKWY')) |
            (streets["ST_LABEL"].str.contains("BELT PKWY"))
            | (streets["ST_LABEL"].str.contains("HENRY HUDSON PKWY"))
            | (streets["ST_LABEL"].str.contains("JACKIE ROBINSON PKWY"))
            | (streets["ST_LABEL"].str.contains("WWTP"))
            | (streets["ST_LABEL"].str.contains("FOOTBRIDGE"))
            | (streets["ST_LABEL"].str.contains("TURNAROUND"))
            | (streets["ST_LABEL"].str.contains(" PED "))
            | (streets["ST_LABEL"].str.contains("PEDESTRIAN"))
        )
    ]
    streets.loc[
        (streets["ST_LABEL"] == "PARK AVE VIADUCT")
        | (streets["ST_LABEL"] == "PARK AVE S"),
        "ST_LABEL",
    ] = "PARK AVE"
    streets.loc[
        (
            streets["ST_LABEL"].isin(
                [
                    "WASHINGTON SQ E",
                    "WASHINGTON SQ W",
                    "WASHINGTON SQ N",
                    "WASHINGTON SQ S",
                ]
            )
        ),
        "ST_LABEL",
    ] = "WASHINGTON SQ"
    streets.loc[
        (streets["ST_LABEL"].isin(["STADIUM PL N", "STADIUM PL S"])), "ST_LABEL"
    ] = "STADIUM PL"
    streets.loc[
        (streets["ST_LABEL"].isin(["AVE OF THE STATES N", "AVE OF THE STATES S"])),
        "ST_LABEL",
    ] = "AVE OF THE STATES"
    streets.loc[
        (streets["ST_LABEL"] == "PORT AUTHORITY BUS TERMINAL EN"), "ST_LABEL"
    ] = "PORT AUTHORITY BUS TERMINAL ENTRANCE"
    streets.loc[(streets["ST_LABEL"] == "E MOUNT EDEN AVENUE"), "ST_LABEL"] = (
        "EAST MOUNT EDEN AVENUE"
    )
    if with_borough:
        return add_borough(streets)
    else:
        return streets


def first_round(buildings):
    streets = load_streets()
    streets["geometry"] = [
        line_to_polygon(x, y, STREET_BUFFER_STANDARD)
        for x, y in tqdm(
            zip(streets["geometry"], streets["ST_WIDTH"]), total=len(streets)
        )
    ]

    buildings_x_streets = buildings.sjoin(streets, how="left", predicate="intersects")
    return buildings_x_streets[["id", "display_address", "PHYSICALID", "ST_LABEL"]]


# load buildings
buildings = pd.read_csv(argv[1])
buildings.dropna(subset="geometry", inplace=True)
buildings = load_geos(buildings)

# match buildings to streets as is, with standard street widths
buildings_x_streets = first_round(buildings)
matches = buildings_x_streets[~pd.isna(buildings_x_streets["PHYSICALID"])]
no_matches = buildings_x_streets[pd.isna(buildings_x_streets["PHYSICALID"])]
matched_list = set(matches["id"].values)
buildings["found_street"] = [
    True if x in matched_list else False for x in buildings["id"]
]
print("First Round Matching Results:")
print(f'\tnumber of buildings with street matches: {matches["id"].nunique()}')
print(f'\tnumber of buildings without street matches: {no_matches["id"].nunique()}')
print(
    f'\tpercent of matches: {matches["id"].nunique()/(matches["id"].nunique() + no_matches["id"].nunique()) * 100:.2f}%'
)
# find the closest street segment to the unmatched buildings
streets = load_streets(with_borough=True)
buildings_nearest_street = (
    buildings[buildings["found_street"] == False]
    .to_crs(crs="3857")
    .sjoin_nearest(streets.to_crs(crs="3857"), how="inner")
)
streets["extra_width"] = streets["PHYSICALID"].isin(
    buildings_nearest_street["PHYSICALID"].unique()
)
# increase the previously found street segments by an extra amount and rematch all buildings
streets["geometry"] = [
    (
        line_to_polygon(x, y, STREET_BUFFER_NO_MATCHES)
        if extra
        else line_to_polygon(x, y, STREET_BUFFER_STANDARD)
    )
    for x, y, extra in tqdm(
        zip(streets["geometry"], streets["ST_WIDTH"], streets["extra_width"]),
        total=len(streets),
    )
]
buildings_x_streets = buildings.sjoin(streets, how="left", predicate="intersects")

matches = buildings_x_streets[~pd.isna(buildings_x_streets["PHYSICALID"])]
no_matches = buildings_x_streets[pd.isna(buildings_x_streets["PHYSICALID"])]
buildings_x_streets["street_segment"] = (
    buildings_x_streets["PHYSICALID"].astype(str)
    + ": "
    + buildings_x_streets["ST_LABEL"]
)
matched_list = set(matches["id"].values)

buildings["found_street2"] = [
    True if x in matched_list else False for x in buildings["id"]
]
buildings_count_by_street = buildings_x_streets.groupby("PHYSICALID")["id"].nunique()
print("Second Round Matching Results:")
print(f'\tnumber of buildings with street matches: {matches["id"].nunique()}')
print(f'\tnumber of buildings without street matches: {no_matches["id"].nunique()}')
print(
    f'\tpercent of matches: {matches["id"].nunique()/(matches["id"].nunique() + no_matches["id"].nunique()) * 100:.2f}%'
)
buildings_x_streets[
    ["id", "display_address", "PHYSICALID", "ST_LABEL", "borough"]
].to_csv("data/buildings_x_streets.csv")

print("creating maps for checking")
streets = streets.merge(
    buildings_count_by_street.rename("building_count"),
    how="left",
    left_on="PHYSICALID",
    right_index=True,
)
m = folium.Map(location=[40.70, -73.94], zoom_start=10, tiles="CartoDB positron")
for _, r in tqdm(streets.iterrows(), total=len(streets)):
    sim_geo = gpd.GeoSeries(r["geometry"])
    geo_j = sim_geo.to_json()
    if r["extra_width"]:
        geo_j = folium.GeoJson(
            data=geo_j,
            highlight_function=lambda x: {"fillOpacity": 0.8},
            style_function=lambda x: {"color": "red"},
        )
    else:
        geo_j = folium.GeoJson(
            data=geo_j, highlight_function=lambda x: {"fillOpacity": 0.8}
        )

    folium.Tooltip(
        f"{r['PHYSICALID']}:  {r['ST_LABEL']}, buildings matched: {r['building_count']}, borough: {r['borough']}"
    ).add_to(geo_j)
    geo_j.add_to(m)

building_style = lambda x: {
    "color": "green" if x["properties"]["found_street2"] else "orange",
    "opacity": 0.50,
    "weight": 2,
}
buildings_sample = buildings[
    (
        buildings["id"].isin(
            buildings_x_streets[buildings_x_streets["found_street"] == False]["id"]
        )
    )
    | (buildings["id"].isin(no_matches["id"]))
]

buildings_viz = folium.GeoJson(
    buildings_sample[
        ["id", "display_address", "geometry", "found_street2"]
    ].reset_index(),
    style_function=building_style,
    highlight_function=lambda x: {"fillOpacity": 0.8},
    zoom_on_click=True,
    tooltip=folium.GeoJsonTooltip(fields=["id", "display_address"]),
)
buildings_viz.add_to(m)
m.keep_in_front(buildings_viz)
m.save(f"data/unmatched_round2_buildings_{STREET_BUFFER_NO_MATCHES}.html")

print("creating corner false postive check")
buildings_by_street = buildings_x_streets.groupby("id")["ST_LABEL"].nunique()
buildings_with_street = buildings_x_streets.groupby("id")["ST_LABEL"].apply(list)
try:
    buildings_sample = (
        buildings[
            buildings["id"].isin(
                buildings_by_street[buildings_by_street > 1].reset_index()["id"]
            )
        ]
        .merge(buildings_with_street, how="left", right_index=True, left_on="id")
        .sample(50000, random_state=42)
    )
except:
    buildings_sample = buildings[
        buildings["id"].isin(
            buildings_by_street[buildings_by_street > 1].reset_index()["id"]
        )
    ].merge(buildings_with_street, how="left", right_index=True, left_on="id")

m = folium.Map(location=[40.70, -73.94], zoom_start=10, tiles="CartoDB positron")
buildings_viz = folium.GeoJson(
    buildings_sample[
        ["id", "display_address", "geometry", "found_street2", "ST_LABEL"]
    ].reset_index(),
    style_function=building_style,
    highlight_function=lambda x: {"fillOpacity": 0.8},
    zoom_on_click=True,
    tooltip=folium.GeoJsonTooltip(fields=["id", "display_address", "ST_LABEL"]),
)
buildings_viz.add_to(m)
m.keep_in_front(buildings_viz)
m.save(f"data/corner_buildings_false_positive_check_{STREET_BUFFER_NO_MATCHES}.html")
