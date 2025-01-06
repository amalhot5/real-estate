from main import main
from time import timedelta

import pandas as pd
import pickle as pkl

import os
import e_threshold_matching
import warnings

warnings.filterwarnings("ignore")


DAY_THRESHOLD = (timedelta(days=0), timedelta(days=365))
PRICE_THRESHOLD = (0, 0.1)
base_fp = os.path.dirname(os.path.abspath(__file__))


def get_cincy_closings():
    cincy = pd.read_csv("data/Cincy_All_Listings_Closings_Only_2024_08_30.csv")
    lb_buildings = pd.read_csv("data/lightbox_buildings/cincy.csv")
    lb_buildings = pd.read_csv("data/lightbox_buildings/cincy.csv")
    for i in range(2, 14):
        lb_buildings = pd.concat(
            [lb_buildings, pd.read_csv(f"data/lightbox_buildings/cincy_{i}.csv")]
        )
    return cincy.merge(lb_buildings, left_on="listing_key", right_on="listingkey")


def init_cincy(threshold_matrix):
    service_zips = pd.read_csv("data/cincy_service_zips_2024_08_28.csv").dropna()["zip"]
    states = ["OH", "IN"]  # , "KY"]
    pw_closings = get_cincy_closings()
    pw_closings["check_zip"] = pd.to_numeric(pw_closings["zip"], errors="coerce")
    if threshold_matrix:
        day_thresh = [
            timedelta(days=0),
            timedelta(days=14),
            timedelta(days=30),
            timedelta(days=90),
            timedelta(days=180),
            timedelta(days=365),
            timedelta(days=365 * 2),
            None,
        ]
        price_thresh = [0, 0.1]  # , 1, None]  # , 0.3, 0.6, 1]  # , None]
        for state in states:
            with open(f"{base_fp}/data/matches_matrix.txt", "a") as f:
                print(state, file=f)
            try:
                with open(f"{base_fp}/data/matches_buckets_{state}.pkl", "rb") as f:
                    retdict = pkl.load(f)
            except:
                retdict = {}

            print(state)
            for i in range(len(day_thresh) - 1):
                for j in range(len(price_thresh) - 1):
                    day_range = (day_thresh[i], day_thresh[i + 1])
                    price_range = (price_thresh[j], price_thresh[j + 1])
                    try:
                        if retdict[(day_range, price_range)]:
                            print(len(retdict[(day_range, price_range)]))
                    except:
                        print(
                            f"\tday_threshold: {day_range}\n\tprice_threshold: {price_range}"
                        )
                        retdict[(day_range, price_range)] = main(
                            day_range,
                            price_thresh,
                            service_zips,
                            state,
                            pw_closings,
                            True,
                        )
                        with open(
                            f"{base_fp}/data/matches_buckets_{state}.pkl", "wb"
                        ) as f:
                            pkl.dump(retdict, f)

                    pw_closings = pw_closings[
                        ~pw_closings["id"].isin(retdict[(day_range, price_range)]["id"])
                    ]

            try:
                with open(
                    f"{base_fp}/data/threshold_matches_buckets_{state}.pkl", "rb"
                ) as f:
                    retdict = pkl.load(f)
            except:
                retdict = {}

            day_thresh1 = [
                timedelta(days=0),
                timedelta(days=7),
                timedelta(days=14),
            ]  # ,timedelta(days=30),timedelta(days=60),timedelta(days=90),]
            price_thresh1 = [0, 0.05, 0.10]  # , 0.15]
            with open(f"{base_fp}/data/matches_matrix.txt", "a") as f:
                print(f"starting threshold matching")
            print(f"\tstarting threshold matching")
            for i in range(len(day_thresh1) - 1):
                for j in range(len(price_thresh1) - 1):
                    day_range = (day_thresh1[i], day_thresh1[i + 1])
                    price_range = (price_thresh1[j], price_thresh1[j + 1])
                    retdict[(day_range, price_range)] = e_threshold_matching.main(
                        day_range,
                        price_range,
                        base_fp,
                        state,
                    )
                    with open(f"{base_fp}/data/matches_matrix.txt", "a") as f:
                        print(len(retdict[(day_range, price_range)]), file=f)
                    with open(
                        f"{base_fp}/data/threshold_matches_buckets_{state}.pkl", "wb"
                    ) as f:
                        pkl.dump(retdict, f)

    else:
        for state in states:
            print(state)
            main(DAY_THRESHOLD, PRICE_THRESHOLD, service_zips, state, pw_closings)


if __name__ == "__main__":
    # init_cincy(True)
    init_cincy(False)
