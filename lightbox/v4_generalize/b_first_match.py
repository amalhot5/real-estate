import pandas as pd
import pickle as pkl
import os

from datetime import timedelta

DAY_THRESH = (timedelta(days=0), None)
PRICE_THRESH = (0, None)


def first_round_match(
    first_match, off_market_sales, day_threshold: tuple, price_threshold: tuple
):
    first_match["match_key"] = [x.upper() for x in first_match["address"]]
    first_match = first_match.merge(
        off_market_sales, how="inner", left_on="match_key", right_on="SITE_ADDR"
    )
    # print(f"matches before thresholds: {first_match['id'].nunique()}")
    if price_threshold[1]:
        if day_threshold[1]:
            first_match = first_match[
                (
                    abs(first_match["last_event"] - first_match["DATE_TRANSFER"])
                    < day_threshold[1]
                )
                & (
                    abs(first_match["last_event"] - first_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
                & (
                    abs(
                        (first_match["sold_price"] - first_match["VAL_TRANSFER"])
                        / first_match["sold_price"]
                    )
                    < price_threshold[1]
                )
                & (
                    abs(
                        (first_match["sold_price"] - first_match["VAL_TRANSFER"])
                        / first_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
            ]
        else:
            first_match = first_match[
                (
                    abs(first_match["last_event"] - first_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
                & (
                    abs(
                        (first_match["sold_price"] - first_match["VAL_TRANSFER"])
                        / first_match["sold_price"]
                    )
                    < price_threshold[1]
                )
                & (
                    abs(
                        (first_match["sold_price"] - first_match["VAL_TRANSFER"])
                        / first_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
            ]
    else:
        if day_threshold[1]:
            first_match = first_match[
                (
                    abs(first_match["last_event"] - first_match["DATE_TRANSFER"])
                    < day_threshold[1]
                )
                & (
                    abs(first_match["last_event"] - first_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
                & (
                    abs(
                        (first_match["sold_price"] - first_match["VAL_TRANSFER"])
                        / first_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
            ]
        else:
            first_match = first_match[
                (
                    abs(first_match["last_event"] - first_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
                & (
                    abs(
                        (first_match["sold_price"] - first_match["VAL_TRANSFER"])
                        / first_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
            ]
    return first_match[["id", "DEED_LID"]]


def main(day_range: tuple, price_range: tuple, base_fp: str, state: str):
    with open(base_fp + "/data/unmatched_closings.pkl", "rb") as f:
        pw_closings = pkl.load(f)
    with open(base_fp + f"/data/off_market_sales_{state}.pkl", "rb") as f:
        deeds_df = pkl.load(f)
    # matching first pass, direct address matches only
    first_match = first_round_match(pw_closings, deeds_df, day_range, price_range)
    print(
        f"\t\tdone first match: {len(pw_closings[pw_closings['id'].isin(first_match['id'])]['id'].unique())} out of {len(pw_closings['id'].unique())}"
    )
    print(
        f"\t\t{len(pw_closings[pw_closings['id'].isin(first_match['id'])]['id'].unique())/(len(pw_closings['id'].unique())) * 100}%"
    )

    with open(base_fp + "/data/first_match.pkl", "wb") as f:
        pkl.dump(first_match, f)
    with open(base_fp + "/data/unmatched_closings.pkl", "wb") as f:
        pkl.dump(pw_closings[~pw_closings["id"].isin(first_match["id"])], f)
    with open(base_fp + f"/data/off_market_sales_{state}.pkl", "wb") as f:
        pkl.dump(deeds_df[~deeds_df["DEED_LID"].isin(first_match["DEED_LID"])], f)

    return first_match


if __name__ == "__main__":
    main(DAY_THRESH, PRICE_THRESH)
