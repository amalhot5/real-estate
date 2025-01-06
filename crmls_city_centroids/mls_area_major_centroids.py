import pandas as pd
import shapely
import geopandas as gpd

from tqdm.auto import tqdm


def get_radius(
    listings: gpd.GeoDataFrame, centroid: shapely.Point, thresh=0.95, radius_incr=0.005
) -> float:
    if len(listings) == 0:
        return 0
    radius = 0 + radius_incr
    check = gpd.GeoDataFrame(
        {"check": [0], "geometry": [centroid.buffer(radius)]}, crs="EPSG:4326"
    )
    while len(listings.sjoin(check, predicate="intersects")) / len(listings) < thresh:
        radius += radius_incr
        check = gpd.GeoDataFrame(
            {"check": [0], "geometry": [centroid.buffer(radius)]}, crs="EPSG:4326"
        )
    # print(len(listings.sjoin(check, predicate='intersects'))/len(listings))
    return radius


def main(df: pd.DataFrame, mls_name: str):
    df["geometry"] = [
        shapely.Point(long, lat)
        for lat, long in tqdm(zip(df["longitude"], df["latitude"]), total=len(df))
    ]
    df = gpd.GeoDataFrame(df, crs="EPSG:4326")

    print("starting to find centroids")

    mls_area_major = {"mls_area_major": df["mls_area_major"].unique()}
    centroids = [
        shapely.MultiPoint(df[df["mls_area_major"] == area]["geometry"].values).centroid
        for area in tqdm(
            mls_area_major["mls_area_major"], total=len(df["mls_area_major"].unique())
        )
    ]
    mls_area_major["centroid"] = centroids
    mls_area_major = pd.DataFrame(mls_area_major)

    city = {"city": df["city"].unique()}
    centroids = [
        shapely.MultiPoint(df[df["city"] == area]["geometry"].values).centroid
        for area in tqdm(city["city"], total=len(df["city"].unique()))
    ]
    city["centroid"] = centroids
    city = pd.DataFrame(city)

    county_or_parish = {"county_or_parish": df["county_or_parish"].unique()}
    centroids = [
        shapely.MultiPoint(
            df[df["county_or_parish"] == area]["geometry"].values
        ).centroid
        for area in tqdm(
            county_or_parish["county_or_parish"],
            total=len(df["county_or_parish"].unique()),
        )
    ]
    county_or_parish["centroid"] = centroids
    county_or_parish = pd.DataFrame(county_or_parish)

    print("done getting centroids\nstarting to find radii")

    radii = [
        get_radius(df[df["mls_area_major"] == l], c)
        for l, c in tqdm(
            zip(mls_area_major["mls_area_major"], mls_area_major["centroid"]),
            total=len(mls_area_major),
        )
    ]
    mls_area_major["radius"] = radii

    radii = [
        get_radius(df[df["city"] == l], c)
        for l, c in tqdm(zip(city["city"], city["centroid"]), total=len(city))
    ]
    city["radius"] = radii

    radii = [
        get_radius(df[df["county_or_parish"] == l], c, radius_incr=0.01)
        for l, c in tqdm(
            zip(county_or_parish["county_or_parish"], city["centroid"]),
            total=len(county_or_parish),
        )
    ]
    county_or_parish["radius"] = radii

    mls_area_major.to_csv(f"data/mls_area_major_{mls_name}.csv", index=False)
    city.to_csv(f"data/city_{mls_name}.csv", index=False)
    county_or_parish.to_csv(f"data/county_or_parish_{mls_name}.csv", index=False)
    print(
        f"output files:\n\tdata/mls_area_major_{mls_name}.csv\n\tdata/city_{mls_name}.csv\n\tdata/county_or_parish_{mls_name}.csv"
    )


if __name__ == "__main__":
    """
    crmls = pd.read_csv('data/all_listings_lat_long_city_crmls.csv')
    for i in range(2,47):
        crmls = pd.concat([crmls, pd.read_csv(f'data/all_listings_lat_long_city_crmls_{i}.csv')])
    print('done loading data')
    main(crmls, 'crmls')
    """
    cincy = pd.read_csv("data/all_listings_lat_long_city_cincy.csv")
    for i in range(2, 8):
        cincy = pd.concat(
            [cincy, pd.read_csv(f"data/all_listings_lat_long_city_cincy_{i}.csv")]
        )
    print(len(cincy))
    cincy = cincy[cincy["latitude"] != 0]
    print(len(cincy))
    print("done loading data")
    main(cincy, "cincy")
