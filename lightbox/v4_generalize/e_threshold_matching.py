import pandas as pd
import pickle as pkl
import os

from datetime import timedelta
from tqdm.auto import tqdm

DAY_THRESH = (timedelta(days=0), timedelta(days=14))
PRICE_THRESH = (0, 0.05)


def threshold_match(
    pw_closings: pd.DataFrame,
    lb_deeds: pd.DataFrame,
    day_threshold: tuple,
    price_threshold: tuple,
) -> pd.DataFrame:

    matched_pw_ids = []
    matched_lb_ids = []
    for pw_zip, pw_price, pw_date, pw_id in tqdm(
        zip(
            pw_closings["check_zip"],
            pw_closings["sold_price"],
            pw_closings["last_event"],
            pw_closings["id"],
        ),
        total=len(pw_closings),
    ):
        check = lb_deeds[lb_deeds["SITE_ZIP"] == pw_zip]
        found_lb_id = []
        try:
            for lb_price, lb_date, lb_id in zip(
                check["VAL_TRANSFER"], check["DATE_TRANSFER"], check["DEED_LID"]
            ):
                if (
                    price_threshold[0]
                    <= (abs(pw_price - lb_price) / pw_price)
                    < price_threshold[1]
                ) and (day_threshold[0] <= abs(pw_date - lb_date) < day_threshold[1]):

                    found_lb_id.append(lb_id)

        except ZeroDivisionError:
            continue

        if len(found_lb_id) >= 0:
            matched_pw_ids += [pw_id] * len(found_lb_id)
            matched_lb_ids += found_lb_id

    return pd.DataFrame({"id": matched_pw_ids, "DEED_LID": matched_lb_ids})


def main(day_range: tuple, price_range: tuple, base_fp: str, state: str) -> None:
    # print('starting fuzzy match')
    with open(base_fp + "/data/unmatched_closings.pkl", "rb") as f:
        pw_closings = pkl.load(f)
    with open(base_fp + f"/data/off_market_sales_{state}.pkl", "rb") as f:
        deeds_df = pkl.load(f)

    fourth_match = threshold_match(pw_closings, deeds_df, day_range, price_range)
    print(
        f"\t\tdone threshold match: {len(pw_closings[(pw_closings['id'].isin(fourth_match['id']))]['id'].unique())} out of {len(pw_closings['id'].unique())}"
    )
    print(
        f"\t\t{len(pw_closings[pw_closings['id'].isin(fourth_match['id'])]['id'].unique())/(len(pw_closings['id'].unique())) * 100}%"
    )

    with open(base_fp + "/data/threshold_match.pkl", "wb") as f:
        pkl.dump(fourth_match, f)
    with open(base_fp + "/data/unmatched_closings.pkl", "wb") as f:
        pkl.dump(pw_closings[~pw_closings["id"].isin(fourth_match["id"])], f)
    with open(f"{base_fp}/data/off_market_sales_{state}.pkl", "wb") as f:
        pkl.dump(deeds_df[~deeds_df["DEED_LID"].isin(fourth_match["DEED_LID"])], f)

    return fourth_match


if __name__ == "__main__":
    main(DAY_THRESH, PRICE_THRESH)
