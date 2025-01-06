import a_parcel_matching
import b_first_match
import c_second_match
import d_fuzzy_match
import e_threshold_matching

from e_dedupe_oms import dedupe

from tqdm.auto import tqdm
from datetime import timedelta

import pandas as pd
import pickle as pkl
import os

base_fp = os.path.dirname(os.path.abspath(__file__))


def load_lightbox_data(fpath: str, service_zips: list) -> pd.DataFrame:
    df = pd.read_csv(fpath)
    df = df[
        [
            "DEED_LID",
            "ASSESSMENT_LID",
            "DATE_TRANSFER",
            "SITE_ADDR",
            "SITE_HOUSE_NUMBER",
            "SITE_DIRECTION",
            "SITE_STREET_NAME",
            "SITE_MODE",
            "SITE_UNIT_NUMBER",
            "SITE_CITY",
            "SITE_STATE",
            "SITE_ZIP",
            "SITE_PLUS_4",
            "DATE_FILING",
            "VAL_TRANSFER",
            "LONGITUDE",
            "LATITUDE",
        ]
    ]
    df = df[df["SITE_ZIP"].isin(service_zips)]
    df.dropna(subset="SITE_ADDR", inplace=True)
    df.dropna(subset="VAL_TRANSFER", inplace=True)
    return df[~(df["SITE_ADDR"] == "N A")]


def load_pw_data(s: str) -> pd.DataFrame:
    if ".csv" in s:
        return pd.read_csv(s)


def dedupe_oms(day_threshold, price_threshold, state):
    with open(base_fp + f"/data/off_market_sales_{state}.pkl", "rb") as f:
        deeds_df = pkl.load(f)

    dupe_oms = dedupe(
        deeds_df[
            [
                "DEED_LID",
                "normalized_address",
                "DATE_TRANSFER",
                "VAL_TRANSFER",
                "ASSESSMENT_LID",
            ]
        ],
        day_threshold,
        price_threshold,
    )

    with open(f"{base_fp}/data/dupe_oms.pkl", "wb") as f:
        pkl.dump(dupe_oms, f)

    deeds_df[~(deeds_df["DEED_LID"].isin(dupe_oms))].to_csv(
        f"{base_fp}/data/output/off_market_sales.csv", index=False
    )

    print(f"\ttotal oms: {len(deeds_df[~(deeds_df['DEED_LID'].isin(dupe_oms))])}")


def get_lightbox_files(service_zips: list, state: str):
    lb_dirs = []
    for root, dirs, files in os.walk(f"{base_fp}/data"):
        lb_dirs.extend(dirs)
    # for state in states:
    deeds_dirs = [x for x in lb_dirs if state in x and "HistoricalTransaction" in x]
    pax_dirs = [x for x in lb_dirs if state in x and "Professional" in x]
    # print(states, deeds_dirs)
    deeds_df = load_lightbox_data(
        f"{base_fp}/data/{deeds_dirs[0]}/Deed_{state}.csv", service_zips
    )
    parcels_x_assessments = pd.read_csv(
        f"{base_fp}/data/{pax_dirs[0]}/ParcelAssessmentRelation_{state}.csv"
    )
    """
    if len(lb_dirs) > 1:
        for lb_fpath, state in tqdm(
            zip(deeds_dirs[1:], states[1:]), total=len(states[1:])
        ):
            deeds_df = pd.concat(
                [
                    load_lightbox_data(
                        f"{base_fp}/data/{lb_fpath}/Deed_{state}.csv", service_zips
                    ),
                    deeds_df,
                ]
            )
        for lb_fpath, state in tqdm(zip(pax_dirs[1:], states[1:]), total=len(states)):
            parcels_x_assessments = pd.concat(
                [
                    load_lightbox_data(
                        f"{base_fp}/data/{lb_fpath}/ParcelAssessmentRelation_{state}.csv",
                        service_zips,
                    ),
                    parcels_x_assessments,
                ]
            )
    """
    return deeds_df, parcels_x_assessments


def main(
    day_range: tuple,
    price_range: tuple,
    service_zips: list,
    state: str,
    pw_closings: pd.DataFrame,
    threshold_matrix=False,
):
    print("\tstarting OMS")
    # states.sort()
    try:
        with open(f"{base_fp}/data/off_market_sales_{state}.pkl", "rb") as f:
            deeds_df = pkl.load(f)
        with open(f"{base_fp}/data/parcels_x_assessments_{state}.pkl", "rb") as f:
            parcels_x_assessments = pkl.load(f)
    except:
        print("\tloading lightbox")
        deeds_df, parcels_x_assessments = get_lightbox_files(service_zips, state)
        with open(f"{base_fp}/data/off_market_sales_{state}.pkl", "wb") as f:
            pkl.dump(deeds_df, f)
        with open(f"{base_fp}/data/parcels_x_assessments_{state}.pkl", "wb") as f:
            pkl.dump(parcels_x_assessments, f)
    print("\tstarting matching")

    parcel_match = a_parcel_matching.main(
        day_range=day_range,
        price_range=price_range,
        deeds_df=deeds_df,
        parcels_x_assessments=parcels_x_assessments,
        pw_closings=pw_closings,
        base_fp=base_fp,
        state=state,
    )
    print("\tstarting address match")
    first_match = b_first_match.main(day_range, price_range, base_fp, state)
    print("\tstarting second address match")
    second_match = c_second_match.main(day_range, price_range, base_fp, state)
    # print("starting fuzzy match")
    # third_match = d_fuzzy_match.main(day_range, price_range, base_fp, state)
    parcel_match["type_of_match"] = ["0_parcel_match"] * len(parcel_match)
    first_match["type_of_match"] = ["1_exact_address"] * len(first_match)
    second_match["type_of_match"] = ["2_standardized_address"] * len(second_match)
    # third_match["type_of_match"] = ["3_fuzzy_match"] * len(third_match)
    if not threshold_matrix:
        print("\tstarting threshold match")
        # threshold_match = e_threshold_matching.main(
        #   (timedelta(days=0), timedelta(days=30)), (0, 0.05), base_fp, state
        # )
        # threshold_match["type_of_match"] = ["4_threshold_match"] * len(threshold_match)

        all_matches = pd.concat(
            [parcel_match, first_match, second_match]  # , threshold_match]
        )
        all_matches.to_csv(
            f"{base_fp}/data/output/matched_deeds_with_closings_{state}.csv",
            index=False,
        )
        print("\tstarting dedupe")
        # dedupe_oms(day_range[1], price_range[1], state)
        with open(base_fp + f"/data/off_market_sales_{state}.pkl", "rb") as f:
            deeds_df = pkl.load(f)
        deeds_df.to_csv(
            f"{base_fp}/data/output/off_market_sales_{state}.csv", index=False
        )
        print(f"Number of OMS: {len(deeds_df)}")
    else:
        all_matches = pd.concat([parcel_match, first_match, second_match])
        all_matches.to_csv(
            f"{base_fp}/data/output/matched_deeds_with_closings_{state}_{price_range}_{day_range}.csv",
            index=False,
        )
        with open(f"{base_fp}/data/matches_matrix.txt", "a") as f:
            print(len(all_matches), file=f)
        return all_matches
