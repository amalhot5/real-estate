import pandas as pd
import pickle as pkl
import os

from datetime import timedelta
from tqdm.auto import tqdm

DAY_THRESH = timedelta(days=180)
PRICE_THRESH = 0.3


def dedupe(df: pd.DataFrame, day_threshold: timedelta, price_threshold: float) -> list:
    dupe_sales = []
    checked = []
    for index, row in tqdm(df.iterrows(), total=len(df)):
        if row["DEED_LID"] not in dupe_sales or row["DEED_LID"] in checked:
            dupes = df[
                (row["normalized_address"] == df["normalized_address"])
                & (abs(row["DATE_TRANSFER"] - df["DATE_TRANSFER"]) <= day_threshold)
                & (
                    abs(
                        (row["VAL_TRANSFER"] - df["VAL_TRANSFER"]) / row["VAL_TRANSFER"]
                    )
                    <= price_threshold
                )
            ]
            if len(dupes) > 1:
                if row["ASSESSMENT_LID"]:
                    dupe_sales += list(
                        dupes[dupes["DEED_LID"] != row["DEED_LID"]]["DEED_LID"].values
                    )
                else:
                    if len(dupe_sales[~pd.isna(dupe_sales["ASSESSMENT_LID"])]) > 0:
                        retval = dupe_sales[~pd.isna(dupe_sales["ASSESSMENT_LID"])][
                            "DEED_LID"
                        ].iloc[0]
                        checked.append(retval)
                        dupe_sales += list(
                            dupes[dupes["DEED_LID"] != retval]["DEED_LID"].values
                        )
                    else:
                        dupe_sales += list(
                            dupes[dupes["DEED_LID"] != row["DEED_LID"]][
                                "DEED_LID"
                            ].values
                        )

    return dupe_sales


if __name__ == "__main__":
    base_fp = os.path.dirname(os.path.abspath(__file__))
    with open(base_fp + "/data/off_market_sales.pkl", "rb") as f:
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
        DAY_THRESH,
        PRICE_THRESH,
    )
    deeds_df[~(deeds_df["DEED_LID"].isin(dupe_oms))].to_csv(
        f"{base_fp}/data/output/off_market_sales.csv", index=False
    )

    with open(base_fp + "/data/unmatched_closings3.pkl", "rb") as f:
        pw_closings = pkl.load(f)

    print(
        f"total oms: {len(deeds_df[~(deeds_df['DEED_LID'].isin(dupe_oms))])}, unmatched pw listings: {len(pw_closings)}"
    )
