import pandas as pd
import pickle as pkl
import os

from datetime import timedelta

DAY_THRESH = (timedelta(days=0), None)
PRICE_THRESH = (0, None)


def parcel_matching(
    lb_closings: pd.DataFrame,
    pw_closings: pd.DataFrame,
    day_threshold: tuple,
    price_threshold: tuple,
    parcels_x_assessments: pd.DataFrame,
) -> pd.DataFrame:
    parcels_x_assessments.columns = [
        "FIPS_CODE",
        "ASSESSMENT_LID",
        "PARCEL_LID".lower(),
    ]
    listings_with_oms = lb_closings.merge(
        parcels_x_assessments, how="inner", on="ASSESSMENT_LID"
    ).merge(pw_closings, how="inner", on="parcel_lid")
    # print(f"matches before thresholds: {listings_with_oms['id'].nunique()}")
    if price_threshold[1]:
        if day_threshold[1]:
            return listings_with_oms[
                (
                    abs(
                        listings_with_oms["VAL_TRANSFER"]
                        - listings_with_oms["sold_price"]
                    )
                    / listings_with_oms["VAL_TRANSFER"]
                    < price_threshold[1]
                )
                & (
                    abs(
                        listings_with_oms["VAL_TRANSFER"]
                        - listings_with_oms["sold_price"]
                    )
                    / listings_with_oms["VAL_TRANSFER"]
                    >= price_threshold[0]
                )
                & (
                    abs(
                        listings_with_oms["DATE_TRANSFER"]
                        - listings_with_oms["last_event"]
                    )
                    < day_threshold[1]
                )
                & (
                    abs(
                        listings_with_oms["DATE_TRANSFER"]
                        - listings_with_oms["last_event"]
                    )
                    >= day_threshold[0]
                )
            ][["id", "DEED_LID"]]
        else:
            return listings_with_oms[
                (
                    abs(
                        listings_with_oms["VAL_TRANSFER"]
                        - listings_with_oms["sold_price"]
                    )
                    / listings_with_oms["VAL_TRANSFER"]
                    < price_threshold[1]
                )
                & (
                    abs(
                        listings_with_oms["VAL_TRANSFER"]
                        - listings_with_oms["sold_price"]
                    )
                    / listings_with_oms["VAL_TRANSFER"]
                    >= price_threshold[0]
                )
                & (
                    abs(
                        listings_with_oms["DATE_TRANSFER"]
                        - listings_with_oms["last_event"]
                    )
                    >= day_threshold[0]
                )
            ][["id", "DEED_LID"]]
    else:
        if day_threshold[1]:
            return listings_with_oms[
                (
                    abs(
                        listings_with_oms["VAL_TRANSFER"]
                        - listings_with_oms["sold_price"]
                    )
                    / listings_with_oms["VAL_TRANSFER"]
                    >= price_threshold[0]
                )
                & (
                    abs(
                        listings_with_oms["DATE_TRANSFER"]
                        - listings_with_oms["last_event"]
                    )
                    < day_threshold[1]
                )
                & (
                    abs(
                        listings_with_oms["DATE_TRANSFER"]
                        - listings_with_oms["last_event"]
                    )
                    >= day_threshold[0]
                )
            ][["id", "DEED_LID"]]
        else:
            return listings_with_oms[
                (
                    abs(
                        listings_with_oms["VAL_TRANSFER"]
                        - listings_with_oms["sold_price"]
                    )
                    / listings_with_oms["VAL_TRANSFER"]
                    >= price_threshold[0]
                )
                & (
                    abs(
                        listings_with_oms["DATE_TRANSFER"]
                        - listings_with_oms["last_event"]
                    )
                    >= day_threshold[0]
                )
            ][["id", "DEED_LID"]]


def main(
    day_range: tuple,
    price_range: tuple,
    parcels_x_assessments: pd.DataFrame,
    deeds_df: pd.DataFrame,
    pw_closings: pd.DataFrame,
    base_fp: str,
    state: str,
):

    pw_closings["address"] = pw_closings["address"].str.replace(".", "")
    pw_closings["address"] = pw_closings["address"].str.strip()
    pw_closings["last_event"] = pd.to_datetime(pw_closings["last_event"], dayfirst=True)

    deeds_df["DATE_TRANSFER"] = pd.to_datetime(deeds_df["DATE_TRANSFER"])
    # print('done loading pw data')

    parcel_match = parcel_matching(
        deeds_df, pw_closings, day_range, price_range, parcels_x_assessments
    )

    print(
        f"\t\tdone parcel match: {len(pw_closings[pw_closings['id'].isin(parcel_match['id'])]['id'].unique())} out of {len(pw_closings['id'].unique())}"
    )
    print(
        f"\t\t{len(pw_closings[pw_closings['id'].isin(parcel_match['id'])]['id'].unique())/len(pw_closings['id'].unique()) * 100}%"
    )

    with open(f"{base_fp}/data/parcel_matching.pkl", "wb") as f:
        pkl.dump(parcel_match, f)
    with open(f"{base_fp}/data/unmatched_closings.pkl", "wb") as f:
        pkl.dump(pw_closings[~pw_closings["id"].isin(parcel_match["id"])], f)
    with open(f"{base_fp}/data/off_market_sales_{state}.pkl", "wb") as f:
        pkl.dump(deeds_df[~deeds_df["DEED_LID"].isin(parcel_match["DEED_LID"])], f)

    return parcel_match


if __name__ == "__main__":
    main(DAY_THRESH, PRICE_THRESH)
