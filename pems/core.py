import os
import typing
from pathlib import Path

import pandas as pd


pems_dir = r"Q:\Data\Observed\Streets\PeMS"
census_vclass_hour_dir = os.path.join(pems_dir, r"census\vclass\hour")
caltrans_district = 4  # Bay Area
sf_county_fips = 75


def _geo_filter(
    df: pd.DataFrame,
    sf_only: bool = True,
    caltrans_district: int = caltrans_district,
) -> pd.DataFrame:
    """
    sf_only: overrides caltrans_district if True
    """
    if sf_only:
        return df[df["fips_county"] == sf_county_fips]
    elif caltrans_district:  # if not None
        return df[df["caltrans_district"] == caltrans_district]
    else:
        return df


def read_vclass_hour(
    year: typing.Union[int, str],  # py3.10+: int | str
    sf_only: bool = True,
    total_flow_only: bool = True,
    type_dir: str = census_vclass_hour_dir,
    caltrans_district: int = caltrans_district,
):
    """
    type_dir: directory for this data type
    """
    if total_flow_only:
        column_names = (
            "timestamp",
            "census_station_id",
            "census_substation_id",
            "fwy_id",  # freeway ID
            "fwy_direction",  # freeway direction
            "fips_city",
            "fips_county",
            "caltrans_district",
            "abs_postmile",  # absolute postmile
            "station_type",
            "census_station_set_id",
            "total_flow",  # veh/hr, across all lanes
            "samples",  # number of samples taken
        )
    else:
        raise NotImplementedError(
            "Reading in individual vehicle class / lanes not implemented yet."
        )
    dir = os.path.join(type_dir, str(year))
    files = sorted(Path(dir).glob("*.txt.gz"))
    dfs = (
        _geo_filter(
            pd.read_csv(f, names=column_names, usecols=column_names),
            sf_only=sf_only,
            caltrans_district=caltrans_district,
        )
        for f in files
    )
    return pd.concat(dfs, ignore_index=True)
