import datetime
import re

import pandas as pd


def add_ts(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    df[destination_cols[0]] = datetime.datetime.now(datetime.UTC).date()

    return df


def clean_floorarea(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    df[destination_cols[0]] = df[transform_cols[0]].apply(
        lambda x: int(x.replace(" sqft", "")) if x != "" else 999_999_999
    )

    return df


def remove_bad_data(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Calculate price per square foot (psf) and drop rows that do not meet certain criteria.

    Criteria:
    - Drop rows where 'district_code' does not fit the format 'D##' (where ## are digits).
    - Drop rows where 'price' is > 250,000,000 or < 100,000.
    - Drop rows where 'floor_area' is > 10,000 or < 200.
    """

    # Drop rows where district_code does not match the format D##
    df = df[df[transform_cols[0]].str.match(r"^D\d{2}$", na=False)]

    # Drop rows where price is out of range
    df = df[(df[transform_cols[1]] >= 100000) & (df["price"] <= 250000000)]

    # Drop rows where floor_area is out of range
    df = df[(df[transform_cols[2]] >= 200) & (df["floor_area"] <= 10000)]

    return df


def get_psf(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    df[destination_cols[0]] = round(df[transform_cols[0]] / df[transform_cols[1]])

    return df


def is_new_project(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    df[destination_cols[0]] = df[transform_cols[0]].apply(
        lambda x: 0 if x is None else (1 if "newProject" in x else 0)
    )

    return df


def is_turbo(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    df[destination_cols[0]] = df[transform_cols[0]].apply(
        lambda x: 0 if x is None else (1 if "Turbo" in x else 0)
    )

    return df


def form_property_type(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    def assign_type(category, search_type):
        if search_type == "CONDO,APT,WALK,CLUS,EXCON":
            return "Non-Landed", category
        elif (
            search_type == "TERRA,DETAC,SEMI,CORN,LBUNG,BUNG,SHOPH,RLAND,TOWN,CON,LCLUS"
        ):
            return "Landed", category
        else:
            hdb_mapping = {
                "1": "1/2 ROOM HDB",
                "3": "3 ROOM HDB",
                "4": "4 ROOM HDB",
                "5": "5 ROOM HDB",
                "T": "OTHER HDB",
            }
            p_type = hdb_mapping.get(search_type[0], None)
            return "HDB", p_type

    df[destination_cols] = df.apply(
        lambda row: pd.Series(
            assign_type(row[transform_cols[0]], row[transform_cols[1]]),
            index=destination_cols,
        ),
        axis=1,
    )

    return df


def clean_headline(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    df[destination_cols[0]] = df[transform_cols[0]].str.strip('"')

    return df


def parse_proximity(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:
    """
    Default transformation to record when the transform was done for the table
    """

    # Use re.search to apply the pattern
    def re_match(text):
        if text is None:
            return (None, None, None)
        pattern = r"(\d+)\s+mins\s+\((\d+)\s+m\)\s+to\s+(.*)"
        match = re.search(pattern, text)
        if match:
            # Extract the groups from the match object
            mins = match.group(1)
            dist = match.group(2)
            mrt = match.group(3)
            return (mrt, mins, dist)
        else:
            return (None, None, None)

    df[destination_cols[0]] = df[transform_cols[0]].apply(lambda x: re_match(x)[0])
    df[destination_cols[1]] = df[transform_cols[0]].apply(lambda x: re_match(x)[1])
    df[destination_cols[2]] = df[transform_cols[0]].apply(lambda x: re_match(x)[2])
    return df


def calculate_post_time(
    df: pd.DataFrame,
    transform_cols: list,
    destination_cols: list,
) -> pd.DataFrame:

    def parse_recency(t, date):

        interval_re = r"[a-zA-Z]+"
        val_re = r"\d+"

        # Extract numeric component
        val_match = re.search(val_re, t)
        val = val_match.group() if val_match else None
        interval_match = re.search(interval_re, t)
        interval = interval_match.group() if interval_match else None
        multiples = {
            "s": 1 / 86400,
            "m": 1 / 1440,
            "h": 1 / 24,
            "d": 1.0,
            "w": 7.0,
            "mon": 30.0,
            "yrs": 365.0,
            "yr": 365.0,
        }

        multiple = multiples.get(interval, None)
        if multiple:
            t_days = round(int(val) * multiple)
            return date - pd.Timedelta(days=t_days)
        else:
            print("!!!")
            return None

    df[destination_cols[0]] = df.apply(
        lambda row: parse_recency(
            row[transform_cols[0]],
            row[transform_cols[1]],
        ),
        axis=1,
    )
    return df
