import pandas as pd
import pickle as pkl
import os

from datetime import timedelta
from tqdm.auto import tqdm

DAY_THRESH = (timedelta(days=0), None)
PRICE_THRESH = (0, None)

map_dict = {
    "ALLEE": "ALY",
    "ALLEY": "ALY",
    "ALLY": "ALY",
    "ALY": "ALY",
    "ANEX": "ANX",
    "ANNEX": "ANX",
    "ANNX": "ANX",
    "ANX": "ANX",
    "ARC": "ARC",
    "ARCADE": "ARC",
    "AV": "AVE",
    "AVE": "AVE",
    "AVEN": "AVE",
    "AVENU": "AVE",
    "AVENUE": "AVE",
    "AVN": "AVE",
    "AVNUE": "AVE",
    "BAYOO": "BYU",
    "BAYOU": "BYU",
    "BCH": "BCH",
    "BEACH": "BCH",
    "BEND": "BND",
    "BND": "BND",
    "BLF": "BLF",
    "BLUF": "BLF",
    "BLUFF": "BLF",
    "BLUFFS": "BLFS",
    "BOT": "BTM",
    "BTM": "BTM",
    "BOTTM": "BTM",
    "BOTTOM": "BTM",
    "BLVD": "BLVD",
    "BOUL": "BLVD",
    "BOULEVARD": "BLVD",
    "BOULV": "BLVD",
    "BR": "BR",
    "BRNCH": "BR",
    "BRANCH": "BR",
    "BRDGE": "BRG",
    "BRG": "BRG",
    "BRIDGE": "BRG",
    "BRK": "BRK",
    "BROOK": "BRK",
    "BROOKS": "BRKS",
    "BURG": "BG",
    "BURGS": "BGS",
    "BYP": "BYP",
    "BYPA": "BYP",
    "BYPAS": "BYP",
    "BYPASS": "BYP",
    "BYPS": "BYP",
    "CAMP": "CP",
    "CP": "CP",
    "CMP": "CP",
    "CANYN": "CYN",
    "CANYON": "CYN",
    "CNYN": "CYN",
    "CAPE": "CPE",
    "CPE": "CPE",
    "CAUSEWAY": "CSWY",
    "CAUSWA": "CSWY",
    "CSWY": "CSWY",
    "CEN": "CTR",
    "CENT": "CTR",
    "CENTER": "CTR",
    "CENTR": "CTR",
    "CENTRE": "CTR",
    "CNTER": "CTR",
    "CNTR": "CTR",
    "CTR": "CTR",
    "CENTERS": "CTRS",
    "CIR": "CIR",
    "CIRC": "CIR",
    "CIRCL": "CIR",
    "CIRCLE": "CIR",
    "CRCL": "CIR",
    "CRCLE": "CIR",
    "CIRCLES": "CIRS",
    "CLF": "CLF",
    "CLIFF": "CLF",
    "CLFS": "CLFS",
    "CLIFFS": "CLFS",
    "CLB": "CLB",
    "CLUB": "CLB",
    "COMMON": "CMN",
    "COMMONS": "CMNS",
    "COR": "COR",
    "CORNER": "COR",
    "CORNERS": "CORS",
    "CORS": "CORS",
    "COURSE": "CRSE",
    "CRSE": "CRSE",
    "COURT": "CT",
    "CT": "CT",
    "COURTS": "CTS",
    "CTS": "CTS",
    "COVE": "CV",
    "CV": "CV",
    "COVES": "CVS",
    "CREEK": "CRK",
    "CRK": "CRK",
    "CRESCENT": "CRES",
    "CRES": "CRES",
    "CRSENT": "CRES",
    "CRSNT": "CRES",
    "CREST": "CRST",
    "CROSSING": "XING",
    "CRSSNG": "XING",
    "XING": "XING",
    "CROSSROAD": "XRD",
    "CROSSROADS": "XRDS",
    "CURVE": "CURV",
    "DALE": "DL",
    "DL": "DL",
    "DAM": "DM",
    "DM": "DM",
    "DIV": "DV",
    "DIVIDE": "DV",
    "DV": "DV",
    "DVD": "DV",
    "DR": "DR",
    "DRIV": "DR",
    "DRIVE": "DR",
    "DRV": "DR",
    "DRIVES": "DRS",
    "EST": "EST",
    "ESTATE": "EST",
    "ESTATES": "ESTS",
    "ESTS": "ESTS",
    "EXP": "EXPY",
    "EXPR": "EXPY",
    "EXPRESS": "EXPY",
    "EXPRESSWAY": "EXPY",
    "EXPW": "EXPY",
    "EXPY": "EXPY",
    "EXT": "EXT",
    "EXTENSION": "EXT",
    "EXTN": "EXT",
    "EXTNSN": "EXT",
    "EXTS": "EXTS",
    "FALL": "FALL",
    "FALLS": "FLS",
    "FLS": "FLS",
    "FERRY": "FRY",
    "FRRY": "FRY",
    "FRY": "FRY",
    "FIELD": "FLD",
    "FLD": "FLD",
    "FIELDS": "FLDS",
    "FLDS": "FLDS",
    "FLAT": "FLT",
    "FLT": "FLT",
    "FLATS": "FLTS",
    "FLTS": "FLTS",
    "FORD": "FRD",
    "FRD": "FRD",
    "FORDS": "FRDS",
    "FOREST": "FRST",
    "FORESTS": "FRST",
    "FRST": "FRST",
    "FORG": "FRG",
    "FORGE": "FRG",
    "FRG": "FRG",
    "FORGES": "FRGS",
    "FORK": "FRK",
    "FRK": "FRK",
    "FORKS": "FRKS",
    "FRKS": "FRKS",
    "FORT": "FT",
    "FRT": "FT",
    "FT": "FT",
    "FREEWAY": "FWY",
    "FREEWY": "FWY",
    "FRWAY": "FWY",
    "FRWY": "FWY",
    "FWY": "FWY",
    "GARDEN": "GDN",
    "GARDN": "GDN",
    "GRDEN": "GDN",
    "GRDN": "GDN",
    "GARDENS": "GDNS",
    "GDNS": "GDNS",
    "GRDNS": "GDNS",
    "GATEWAY": "GTWY",
    "GATEWY": "GTWY",
    "GATWAY": "GTWY",
    "GTWAY": "GTWY",
    "GTWY": "GTWY",
    "GLEN": "GLN",
    "GLN": "GLN",
    "GLENS": "GLNS",
    "GREEN": "GRN",
    "GRN": "GRN",
    "GREENS": "GRNS",
    "GROV": "GRV",
    "GROVE": "GRV",
    "GRV": "GRV",
    "GROVES": "GRVS",
    "HARB": "HBR",
    "HARBOR": "HBR",
    "HARBR": "HBR",
    "HBR": "HBR",
    "HRBOR": "HBR",
    "HARBORS": "HBRS",
    "HAVEN": "HVN",
    "HVN": "HVN",
    "HT": "HTS",
    "HTS": "HTS",
    "HIGHWAY": "HWY",
    "HIGHWY": "HWY",
    "HIWAY": "HWY",
    "HIWY": "HWY",
    "HWAY": "HWY",
    "HWY": "HWY",
    "HILL": "HL",
    "HL": "HL",
    "HILLS": "HLS",
    "HLS": "HLS",
    "HLLW": "HOLW",
    "HOLLOW": "HOLW",
    "HOLLOWS": "HOLW",
    "HOLW": "HOLW",
    "HOLWS": "HOLW",
    "INLT": "INLT",
    "IS": "IS",
    "ISLAND": "IS",
    "ISLND": "IS",
    "ISLANDS": "ISS",
    "ISLNDS": "ISS",
    "ISS": "ISS",
    "ISLE": "ISLE",
    "ISLES": "ISLE",
    "JCT": "JCT",
    "JCTION": "JCT",
    "JCTN": "JCT",
    "JUNCTION": "JCT",
    "JUNCTN": "JCT",
    "JUNCTON": "JCT",
    "JCTNS": "JCTS",
    "JCTS": "JCTS",
    "JUNCTIONS": "JCTS",
    "KEY": "KY",
    "KY": "KY",
    "KEYS": "KYS",
    "KYS": "KYS",
    "KNL": "KNL",
    "KNOL": "KNL",
    "KNOLL": "KNL",
    "KNLS": "KNLS",
    "KNOLLS": "KNLS",
    "LK": "LK",
    "LAKE": "LK",
    "LKS": "LKS",
    "LAKES": "LKS",
    "LAND": "LAND",
    "LANDING": "LNDG",
    "LNDG": "LNDG",
    "LNDNG": "LNDG",
    "LANE": "LN",
    "LN": "LN",
    "LGT": "LGT",
    "LIGHT": "LGT",
    "LIGHTS": "LGTS",
    "LF": "LF",
    "LOAF": "LF",
    "LCK": "LCK",
    "LOCK": "LCK",
    "LCKS": "LCKS",
    "LOCKS": "LCKS",
    "LDG": "LDG",
    "LDGE": "LDG",
    "LODG": "LDG",
    "LODGE": "LDG",
    "LOOP": "LOOP",
    "LOOPS": "LOOP",
    "MALL": "MALL",
    "MNR": "MNR",
    "MANOR": "MNR",
    "MANORS": "MNRS",
    "MNRS": "MNRS",
    "MEADOW": "MDW",
    "MDW": "MDWS",
    "MDWS": "MDWS",
    "MEADOWS": "MDWS",
    "MEDOWS": "MDWS",
    "MEWS": "MEWS",
    "MILL": "ML",
    "MILLS": "MLS",
    "MISSN": "MSN",
    "MSSN": "MSN",
    "MOTORWAY": "MTWY",
    "MNT": "MT",
    "MT": "MT",
    "MOUNT": "MT",
    "MNTAIN": "MTN",
    "MNTN": "MTN",
    "MOUNTAIN": "MTN",
    "MOUNTIN": "MTN",
    "MTIN": "MTN",
    "MTN": "MTN",
    "MNTNS": "MTNS",
    "MOUNTAINS": "MTNS",
    "NCK": "NCK",
    "NECK": "NCK",
    "ORCH": "ORCH",
    "ORCHARD": "ORCH",
    "ORCHRD": "ORCH",
    "OVAL": "OVAL",
    "OVL": "OVAL",
    "OVERPASS": "OPAS",
    "PARK": "PARK",
    "PRK": "PARK",
    "PARKS": "PARK",
    "PARKWAY": "PKWY",
    "PARKWY": "PKWY",
    "PKWAY": "PKWY",
    "PKWY": "PKWY",
    "PKY": "PKWY",
    "PARKWAYS": "PKWY",
    "PKWYS": "PKWY",
    "PASS": "PASS",
    "PASSAGE": "PSGE",
    "PATH": "PATH",
    "PATHS": "PATH",
    "PIKE": "PIKE",
    "PIKES": "PIKE",
    "PINE": "PNE",
    "PINES": "PNES",
    "PNES": "PNES",
    "PL": "PL",
    "PLAIN": "PLN",
    "PLN": "PLN",
    "PLAINS": "PLNS",
    "PLNS": "PLNS",
    "PLAZA": "PLZ",
    "PLZ": "PLZ",
    "PLZA": "PLZ",
    "POINT": "PT",
    "PT": "PT",
    "POINTS": "PTS",
    "PTS": "PTS",
    "PORT": "PRT",
    "PRT": "PRT",
    "PORTS": "PRTS",
    "PRTS": "PRTS",
    "PR": "PR",
    "PRAIRIE": "PR",
    "PRR": "PR",
    "RAD": "RADL",
    "RADIAL": "RADL",
    "RADIEL": "RADL",
    "RADL": "RADL",
    "RAMP": "RAMP",
    "RANCH": "RNCH",
    "RANCHES": "RNCH",
    "RNCH": "RNCH",
    "RNCHS": "RNCH",
    "RAPID": "RPD",
    "RPD": "RPD",
    "RAPIDS": "RPDS",
    "RPDS": "RPDS",
    "REST": "RST",
    "RST": "RST",
    "RDG": "RDG",
    "RDGE": "RDG",
    "RIDGE": "RDG",
    "RDGS": "RDGS",
    "RIDGES": "RDGS",
    "RIV": "RIV",
    "RIVER": "RIV",
    "RVR": "RIV",
    "RIVR": "RIV",
    "RD": "RD",
    "ROAD": "RD",
    "ROADS": "RDS",
    "RDS": "RDS",
    "ROUTE": "RTE",
    "ROW": "ROW",
    "RUE": "RUE",
    "RUN": "RUN",
    "SHL": "SHL",
    "SHOAL": "SHL",
    "SHLS": "SHLS",
    "SHOALS": "SHLS",
    "SHOAR": "SHR",
    "SHORE": "SHR",
    "SHR": "SHR",
    "SHOARS": "SHRS",
    "SHORES": "SHRS",
    "SHRS": "SHRS",
    "SKYWAY": "SKWY",
    "SPG": "SPG",
    "SPNG": "SPG",
    "SPRING": "SPG",
    "SPRNG": "SPG",
    "SPGS": "SPGS",
    "SPNGS": "SPGS",
    "SPRINGS": "SPGS",
    "SPRNGS": "SPGS",
    "SPUR": "SPUR",
    "SPURS": "SPUR",
    "SQ": "SQ",
    "SQR": "SQ",
    "SQRE": "SQ",
    "SQU": "SQ",
    "SQUARE": "SQ",
    "SQRS": "SQS",
    "SQUARES": "SQS",
    "STA": "STA",
    "STATION": "STA",
    "STATN": "STA",
    "STN": "STA",
    "STRA": "STRA",
    "STRAV": "STRA",
    "STRAVEN": "STRA",
    "STRAVENUE": "STRA",
    "STRAVN": "STRA",
    "STRVN": "STRA",
    "STRVNUE": "STRA",
    "STREAM": "STRM",
    "STREME": "STRM",
    "STRM": "STRM",
    "STREET": "ST",
    "STRT": "ST",
    "ST": "ST",
    "STR": "ST",
    "STREETS": "STS",
    "SMT": "SMT",
    "SUMIT": "SMT",
    "SUMITT": "SMT",
    "SUMMIT": "SMT",
    "TER": "TER",
    "TERR": "TER",
    "TERRACE": "TER",
    "THROUGHWAY": "TRWY",
    "TRACE": "TRCE",
    "TRACES": "TRCE",
    "TRCE": "TRCE",
    "TRACK": "TRAK",
    "TRACKS": "TRAK",
    "TRAK": "TRAK",
    "TRK": "TRAK",
    "TRKS": "TRAK",
    "TRAFFICWAY": "TRFY",
    "TRAIL": "TRL",
    "TRAILS": "TRL",
    "TRL": "TRL",
    "TRLS": "TRL",
    "TRAILER": "TRLR",
    "TRLR": "TRLR",
    "TRLRS": "TRLR",
    "TUNEL": "TUNL",
    "TUNL": "TUNL",
    "TUNLS": "TUNL",
    "TUNNEL": "TUNL",
    "TUNNELS": "TUNL",
    "TUNNL": "TUNL",
    "TRNPK": "TPKE",
    "TURNPIKE": "TPKE",
    "TURNPK": "TPKE",
    "UNDERPASS": "UPAS",
    "UN": "UN",
    "UNION": "UN",
    "UNIONS": "UNS",
    "VALLEY": "VLY",
    "VALLY": "VLY",
    "VLLY": "VLY",
    "VLY": "VLY",
    "VALLEYS": "VLYS",
    "VLYS": "VLYS",
    "VDCT": "VIA",
    "VIA": "VIA",
    "VIADCT": "VIA",
    "VIADUCT": "VIA",
    "VIEW": "VW",
    "VW": "VW",
    "VIEWS": "VWS",
    "VWS": "VWS",
    "VILL": "VLG",
    "VILLAG": "VLG",
    "VILLAGE": "VLG",
    "VILLG": "VLG",
    "VILLIAGE": "VLG",
    "VLG": "VLG",
    "VILLAGES": "VLGS",
    "VLGS": "VLGS",
    "VILLE": "VL",
    "VL": "VL",
    "VIS": "VIS",
    "VIST": "VIS",
    "VISTA": "VIS",
    "VST": "VIS",
    "VSTA": "VIS",
    "WALK": "WALK",
    "WALKS": "WALK",
    "WALL": "WALL",
    "WY": "WAY",
    "WAY": "WAY",
    "WAYS": "WAYS",
    "WELL": "WL",
    "WELLS": "WLS",
    "WLS": "WLS",
    "PLACE": "PL",
    "NORTH": "N",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTH": "S",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "EAST": "E",
    "WEST": "W",
}


def standardize_address(address: str) -> str:
    address = address.upper()
    address = address.replace("(", "")
    address = address.replace(")", "")
    address = address.replace("#", "")
    address = address.replace("UNIT", "")
    address = address.replace("STE", "")
    address = address.replace("  ", " ")
    address_split = address.split()
    for i in range(len(address_split)):
        if address_split[i] in map_dict.keys():
            address_split[i] = map_dict[address_split[i]]
    # for index, row in suffixes.iterrows():
    #   if ' ' + row['Alias'] in address:
    #      address = address.replace(row['Alias'], row['Abbreviation'])
    return " ".join(address_split)


def second_round_match(second_match, off_market_sales, day_threshold, price_threshold):
    second_match["normalized_address"] = [
        standardize_address(x) for x in second_match["address"]
    ]
    off_market_sales["normalized_address"] = [
        standardize_address(x) for x in off_market_sales["SITE_ADDR"]
    ]
    second_match = second_match.merge(
        off_market_sales, how="inner", on="normalized_address"
    )
    if price_threshold[1]:
        if day_threshold[1]:
            second_match = second_match[
                (
                    abs(
                        (second_match["sold_price"] - second_match["VAL_TRANSFER"])
                        / second_match["sold_price"]
                    )
                    < price_threshold[1]
                )
                & (
                    abs(
                        (second_match["sold_price"] - second_match["VAL_TRANSFER"])
                        / second_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
                & (
                    abs(second_match["last_event"] - second_match["DATE_TRANSFER"])
                    < day_threshold[1]
                )
                & (
                    abs(second_match["last_event"] - second_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
            ]
        else:
            second_match = second_match[
                (
                    abs(
                        (second_match["sold_price"] - second_match["VAL_TRANSFER"])
                        / second_match["sold_price"]
                    )
                    < price_threshold[1]
                )
                & (
                    abs(
                        (second_match["sold_price"] - second_match["VAL_TRANSFER"])
                        / second_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
                & (
                    abs(second_match["last_event"] - second_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
            ]
    else:
        if day_threshold[1]:
            second_match = second_match[
                (
                    abs(
                        (second_match["sold_price"] - second_match["VAL_TRANSFER"])
                        / second_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
                & (
                    abs(second_match["last_event"] - second_match["DATE_TRANSFER"])
                    < day_threshold[1]
                )
                & (
                    abs(second_match["last_event"] - second_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
            ]
        else:
            second_match = second_match[
                (
                    abs(
                        (second_match["sold_price"] - second_match["VAL_TRANSFER"])
                        / second_match["sold_price"]
                    )
                    >= price_threshold[0]
                )
                & (
                    abs(second_match["last_event"] - second_match["DATE_TRANSFER"])
                    >= day_threshold[0]
                )
            ]

    return second_match[["id", "DEED_LID"]]


def main(day_range: tuple, price_range: tuple, base_fp: str, state: str):
    with open(base_fp + "/data/unmatched_closings.pkl", "rb") as f:
        pw_closings = pkl.load(f)
    with open(base_fp + f"/data/off_market_sales_{state}.pkl", "rb") as f:
        deeds_df = pkl.load(f)

    second_match = second_round_match(pw_closings, deeds_df, day_range, price_range)
    print(
        f"\t\tdone second match: {len(pw_closings[(pw_closings['id'].isin(second_match['id']))]['id'].unique())} out of {len(pw_closings['id'].unique())}"
    )
    print(
        f"\t\t{len(pw_closings[pw_closings['id'].isin(second_match['id'])]['id'].unique())/(len(pw_closings['id'].unique())) * 100}%"
    )

    with open(base_fp + "/data/second_match.pkl", "wb") as f:
        pkl.dump(second_match, f)
    with open(base_fp + "/data/unmatched_closings.pkl", "wb") as f:
        pkl.dump(pw_closings[~pw_closings["id"].isin(second_match["id"])], f)
    with open(base_fp + f"/data/off_market_sales_{state}.pkl", "wb") as f:
        pkl.dump(deeds_df[~deeds_df["DEED_LID"].isin(second_match["DEED_LID"])], f)

    return second_match


if __name__ == "__main__":
    main(DAY_THRESH, PRICE_THRESH)
