import pandas as pd
import geopandas as gpd
import shapely
import json
from re import sub
from tqdm.auto import tqdm


def add_demotions(
    layer_2: gpd.GeoDataFrame,
    layer_3: gpd.GeoDataFrame,
    to_demote: list,
    precisely_all: gpd.GeoDataFrame,
):
    macros_layer3 = precisely_all[
        (precisely_all["COUNTYFIPS"] == "06037")
        & (precisely_all["OBJ_SUBTYP"].str.contains("Macro"))
        & (precisely_all["OBJ_NAME"].isin(to_demote))
    ]
    overlaps = macros_layer3.sjoin(layer_3, predicate="covers")[
        ["OBJ_ID", "OBJ_NAME", "PID", "Display Name", "geometry"]
    ]

    geos_to_add = [x for x in to_demote if not x in overlaps["OBJ_NAME"].values]
    parent_ids = ["90015"] * len(geos_to_add)

    # add demotions
    count = 0
    geo_name = []
    geos = []
    pid = []
    key = []
    max_pid = layer_3["PID"].astype(int).max() + 1
    parent_key = []
    for parent_name, parent_id in zip(geos_to_add, parent_ids):
        shape = macros_layer3[macros_layer3["OBJ_NAME"] == parent_name][
            "geometry"
        ].values[0]
        # if not shapely.is_empty(exclude):
        # if not shapely.equals(all_childs, layer2[layer2['PID'] == parent_id]['geometry'].values[0]):
        # if shapely.is_empty(shapely.difference(all_childs, layer2[layer2['PID'] == parent_id]['geometry'].values[0])):
        geo_name.append(parent_name)
        geos.append(shape)
        pid.append(max_pid)
        max_pid += 1
        k = (
            "_".join(
                sub(
                    "([A-Z][a-z]+)",
                    r" \1",
                    sub("([A-Z]+)", r" \1", parent_name.replace("-", " ")),
                ).split()
            )
            + "_"
            + str(max_pid)
        )
        key.append(k)
        try:
            parent_key.append(
                layer_2.sjoin(
                    macros_layer3[macros_layer3["OBJ_NAME"] == parent_name],
                    predicate="within",
                )["PID"].values[0]
            )
        except:
            parent_key.append(parent_id)

    add_to_layer3 = pd.DataFrame(
        {
            "PID": pid,
            "Display Name": geo_name,
            "geometry": geos,
            "Selectable": [False] * len(geo_name),
            "Key": key,
            "Layer Name": [3] * len(geo_name),
            "LAYER_3_PARENT_PID": parent_key,
        }
    )
    layer_3 = pd.concat(
        [layer_3, gpd.GeoDataFrame(add_to_layer3, crs="EPSG:4326")], ignore_index=True
    )

    changed_shape = []
    removed_shapes = []
    for new_shape, old_shape in zip(overlaps["OBJ_NAME"], overlaps["PID"]):
        if not new_shape in changed_shape:
            layer_3.loc[layer_3["PID"] == old_shape, "geometry"] = macros_layer3[
                macros_layer3["OBJ_NAME"] == new_shape
            ]["geometry"].values[0]
            layer_3.loc[layer_3["PID"] == old_shape, "Key"] = (
                "_".join(
                    sub(
                        "([A-Z][a-z]+)",
                        r" \1",
                        sub("([A-Z]+)", r" \1", new_shape.replace("-", " ")),
                    ).split()
                )
                + "_"
                + str(old_shape)
            )
            layer_3.loc[layer_3["PID"] == old_shape, "Display Name"] = new_shape
            changed_shape.append(new_shape)
        else:
            # print(f'dropping {old_shape}')
            # removed_shapes['name'].append(old_shape)
            removed_shapes.append(layer_3[layer_3["PID"] == old_shape])
            layer_3.drop(layer_3[layer_3["PID"] == old_shape].index, inplace=True)

    # print(changed_shape)
    # print(len(removed_shapes))

    return layer_2, layer_3


def modify_layer2(layer_2, layer_3):
    parent_check = layer_3.sjoin(layer_2, predicate="within")
    parents_to_fix = parent_check[
        parent_check["LAYER_2_PARENT_PID"] != parent_check["PID_right"]
    ]
    for child_id, parent_id in zip(
        parents_to_fix["PID_left"], parents_to_fix["PID_right"]
    ):
        layer_3.loc[layer_3["PID"] == child_id, "LAYER_3_PARENT_PID"] = parent_id

    overlaps = layer_3.sjoin(layer_2, predicate="overlaps").reset_index(drop=True)[
        ["PID_left", "Display Name_left", "LAYER_2_PARENT_PID", "PID_right"]
    ]
    overlaps.columns = ["PID_child", "name_child", "parent_id_child", "PID_parent"]
    overlaps_correct = overlaps[(overlaps["parent_id_child"] == overlaps["PID_parent"])]
    messed_up_layer3 = []
    messed_up_layer2 = []
    for layer3_id, layer2_id in zip(
        overlaps_correct["PID_child"], overlaps_correct["PID_parent"]
    ):
        real_parent_geo = layer_2[layer_2["PID"] == layer2_id]["geometry"].values[0]
        child = layer_3[layer_3["PID"] == layer3_id]["geometry"].values[0]
        try:
            layer_2.loc[layer_2["PID"] == layer2_id, "geometry"] = shapely.union(
                real_parent_geo, shapely.difference(child, real_parent_geo)
            )
        except:
            print(layer2_id, layer3_id)
            messed_up_layer2.append(layer2_id)
            messed_up_layer3.append(layer3_id)

    overlaps_incorrect = overlaps[
        (overlaps["parent_id_child"] != overlaps["PID_parent"])
    ]
    for layer3_id, layer2_id in zip(
        overlaps_incorrect["PID_child"], overlaps_incorrect["PID_parent"]
    ):
        # if int(layer2_id) == 93731:
        #   continue
        geo_to_change = layer_2[layer_2["PID"] == layer2_id]["geometry"].values[0]
        child = layer_3[layer_3["PID"] == layer3_id]["geometry"].values[0]
        try:
            layer_2.loc[layer_2["PID"] == layer2_id, "geometry"] = shapely.difference(
                geo_to_change, child
            )
        except:
            print(layer2_id, layer3_id)

    wrong_type = []
    for g, id in zip(layer_2.geometry, layer_2["PID"]):
        if type(g) == shapely.GeometryCollection:
            wrong_type.append(id)
    for id in wrong_type:
        poly = []
        for g in layer_2.loc[layer_2["PID"] == id, "geometry"].values[0].geoms:
            if type(g) == shapely.geometry.Polygon:
                poly.append(g)
        layer_2.loc[layer_2["PID"] == id, "geometry"] = shapely.MultiPolygon(poly)

    return layer_2, layer_3


def create_excludes(layer_2, layer_3):
    # add excludes
    count = 0
    geo_name = []
    geos = []
    pid = []
    key = []
    max_pid = layer_3["PID"].astype(int).max() + 1
    parent_key = []
    for parent_id in layer_2["PID"]:
        all_childs = shapely.union_all(
            layer_3[layer_3["LAYER_3_PARENT_PID"] == parent_id]["geometry"].values
        )
        exclude = (
            layer_2[layer_2["PID"] == parent_id]["geometry"]
            .values[0]
            .difference(all_childs)
        )
        # if not shapely.is_empty(exclude):
        # if not shapely.equals(all_childs, layer2[layer2['PID'] == parent_id]['geometry'].values[0]):
        # if shapely.is_empty(shapely.difference(all_childs, layer2[layer2['PID'] == parent_id]['geometry'].values[0])):
        n = layer_2[layer_2["PID"] == parent_id]["Display Name"].values[0] + " Exclude"
        geo_name.append(n)
        geos.append(exclude)
        pid.append(max_pid)
        max_pid += 1
        k = (
            "_".join(
                sub(
                    "([A-Z][a-z]+)",
                    r" \1",
                    sub("([A-Z]+)", r" \1", n.replace("-", " ")),
                ).split()
            )
            + "_"
            + str(max_pid)
        )
        key.append(k)
        parent_key.append(parent_id)

    exclude_geos = pd.DataFrame(
        {
            "PID": pid,
            "Display Name": geo_name,
            "geometry": geos,
            "Selectable": [False] * len(geo_name),
            "Key": key,
            "Layer Name": [3] * len(geo_name),
            "LAYER_3_PARENT_PID": parent_key,
        }
    )
    exclude_geos = gpd.GeoDataFrame(exclude_geos, crs="EPSG:4326")
    exclude_geos = exclude_geos[
        (~exclude_geos["geometry"].is_empty)
        & (exclude_geos["Display Name"] != "Compton Exclude")
    ]

    wrong_type = []
    to_drop = []
    for g, id in zip(exclude_geos.geometry, exclude_geos["PID"]):
        if type(g) == shapely.GeometryCollection:
            wrong_type.append(id)
        elif type(g) in [shapely.LineString, shapely.MultiLineString]:
            to_drop.append(id)
    for id in wrong_type:
        poly = []
        for g in (
            exclude_geos.loc[exclude_geos["PID"] == id, "geometry"].values[0].geoms
        ):
            if type(g) == shapely.geometry.Polygon:
                poly.append(g)
        exclude_geos.loc[exclude_geos["PID"] == id, "geometry"] = shapely.MultiPolygon(
            poly
        )
    exclude_geos = exclude_geos[~exclude_geos["Display Name"].isin(to_drop)]

    return exclude_geos


def fit_layer1(layer1, layer2):
    childs_to_fix = layer2.sjoin(layer1, predicate="within")
    childs_to_fix = layer2[~layer2["Key"].isin(childs_to_fix["Key"])]
    for layer3_id, layer2_id in zip(
        childs_to_fix["PID"], childs_to_fix["LAYER_2_PARENT_PID"]
    ):
        geo_to_change = layer1[layer1["PID"] == layer2_id]["geometry"].values[0]
        child = layer2[layer2["PID"] == layer3_id]["geometry"].values[0]
        try:
            layer1.loc[layer1["PID"] == layer2_id, "geometry"] = shapely.union_all(
                [geo_to_change, shapely.difference(child, geo_to_change)]
            )
        except:
            print(
                layer1[layer1["PID"] == layer2_id]["name"].values[0],
                layer2[layer2["PID"] == layer3_id]["Display Name"].values[0],
            )

    return layer1


def clean_gdf(ret_df):
    ret_df["geometry"] = [
        (
            shapely.MultiPolygon([feature])
            if isinstance(feature, shapely.Polygon)
            else feature
        )
        for feature in ret_df["geometry"]
    ]

    return ret_df.to_crs(crs="EPSG:4326")


def generate_LA():
    # open layer 3 file
    layer_3 = gpd.read_file("data/la_layer3 (4).geojson")
    # open layer 2 file and make sure it has the correct column names
    layer_2 = gpd.read_file("data/la_layer2.geojson")
    layer_2.columns = [
        "PID",
        "Display Name",
        "Selectable",
        "Key",
        "LAYER_2_PARENT_PID",
        "geometry",
    ]
    layer_2["Layer Name"] = [2] * len(layer_2)

    # specify demotes
    to_demote = [
        "Pomona",
        "Arcadia",
        "Monrovia",
        "San Gabriel",
        "Temple City",
        "Alhambra",
        "Hawthorne",
        "North Hollywood",
        "West Covina",
        "La Verne",
        "Claremont",
        "Manhattan Beach",
        "Redondo Beach",
        "Topanga",
        "Culver City",
    ]
    # load all of precisely's dataset to get shapes for demotes
    precisely_all = gpd.read_file(
        "data/NEIGHBORHOOD_BOUNDARIES_USA_202405_SHP/data",
        layer="neighborhood_objects_usa",
    ).merge(
        gpd.read_file(
            "data/NEIGHBORHOOD_BOUNDARIES_USA_202405_SHP/data",
            layer="neighborhood_census_usa",
        ),
        on="OBJ_ID",
    )

    layer1 = pd.read_csv("data/2110263_2024_08_20.csv")
    layer1["geometry"] = [
        shapely.geometry.shape(json.loads(layer1["st_asgeojson"].values[0]))
    ]
    layer1 = gpd.GeoDataFrame(layer1, crs="EPSG:4326")
    layer1 = layer1[["id", "name", "geometry"]]
    layer1.columns = ["PID", "Display Name", "geometry"]

    return precisely_all, layer1, layer_2, layer_3, to_demote


# def generate_ventura():


def main(layer_1, layer_2, layer_3, to_demote, precisely_all):
    print("done loading data")
    layer_2, layer_3 = add_demotions(layer_2, layer_3, to_demote, precisely_all)
    print("done adding demotions")
    layer_2, layer_3 = modify_layer2(layer_2, layer_3)
    print("done modifying layer 2")
    exclude_geos = create_excludes(layer_2, layer_3)
    print("done creating excludes")
    layer_1 = fit_layer1(layer_1, layer_2)
    layer_3 = pd.concat([layer_3, exclude_geos], ignore_index=True).to_crs("EPSG:4326")
    layer_2 = clean_gdf(layer_2)
    layer_3 = clean_gdf(layer_3)
    layer_3.to_file("data/output/layer_3.geojson", driver="GeoJSON")
    layer_2.to_file("data/output/layer_2.geojson", driver="GeoJSON")
    layer_1.to_file("data/output/layer_1.geojson", driver="GeoJSON")


if __name__ == "__main__":
    precisely_all, layer_1, layer_2, layer_3, to_demote = generate_LA()
    main(layer_1, layer_2, layer_3, to_demote, precisely_all)
