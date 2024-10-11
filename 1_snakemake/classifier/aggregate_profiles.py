import logging

import polars as pl
import pycytominer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def aggregate_compound(method: str, dat: pl.DataFrame) -> pl.DataFrame:
    """Aggregate subset of profiles for each compound."""
    feat_cols = [i for i in dat.columns if "Metadata" not in i]

    if method == "all":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            ),
        )

    elif method == "allpod":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    pl.col("Metadata_Log10Conc") > pl.col("Metadata_POD"),
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            ),
        )

    elif method == "allpodcc":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    (pl.col("Metadata_Log10Conc") > pl.col("Metadata_POD"))
                    & (pl.col("Metadata_Log10Conc") < pl.col("Metadata_ccPOD")),
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            ),
        )

    elif method == "lastpod":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    (pl.col("Metadata_Log10Conc") > pl.col("Metadata_POD"))
                    & (pl.col("Metadata_Concentration") == 50.0),
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            ),
        )

    elif method == "firstpod":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    pl.col("Metadata_Concentration") == pl.col("Metadata_MinConc"),
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            ),
        )

    elif method == "lastpodcc":
        agg_df = pl.from_pandas(
            pycytominer.aggregate(
                dat.filter(
                    pl.col("Metadata_Concentration") == pl.col("Metadata_MaxConc"),
                ).to_pandas(),
                strata=["Metadata_Compound"],
                features=feat_cols,
            ),
        )

    # Annotate with aggregation and feature type
    agg_df = agg_df.with_columns(pl.lit(method).alias("Metadata_AggType"))

    return agg_df


def aggregate_profiles(
    prof_path: str,
    pod_path: str,
    agg_path: str,
) -> None:
    """Aggregate subset of profiles for each compound."""
    # 1. Read in data
    profiles = pl.read_parquet(prof_path)
    pods = pl.read_parquet(pod_path)

    # 2. Process metadata

    # remove controls
    controls = ["DMSO"]
    profiles = profiles.filter(~pl.col("Metadata_Compound").is_in(controls))

    # Add POD
    profiles = profiles.join(
        pods.select(["Metadata_Compound", "bmd", "cc_POD"]).rename({
            "bmd": "Metadata_POD",
            "cc_POD": "Metadata_ccPOD",
        }),
        on="Metadata_Compound",
        how="left",
    )

    # Identify first conc after POD and last conc below ccPOD
    min_conc = (
        profiles.filter(pl.col("Metadata_Log10Conc") > pl.col("Metadata_POD"))
        .group_by(["Metadata_Compound"])
        .agg(pl.min("Metadata_Concentration").alias("Metadata_MinConc"))
    )

    max_conc = (
        profiles.filter(
            (pl.col("Metadata_Log10Conc") > pl.col("Metadata_POD"))
            & (pl.col("Metadata_Log10Conc") < pl.col("Metadata_ccPOD")),
        )
        .group_by(["Metadata_Compound"])
        .agg(pl.max("Metadata_Concentration").alias("Metadata_MaxConc"))
    )

    profiles = profiles.join(min_conc, on="Metadata_Compound", how="left").join(
        max_conc,
        on="Metadata_Compound",
        how="left",
    )

    # 3. Aggregate profiles
    methods = ["all", "allpod", "allpodcc", "firstpod", "lastpod", "lastpodcc"]
    agg_df = []

    for method in methods:
        agg_df.append(aggregate_compound(method, profiles))

    agg_df = pl.concat(agg_df)

    # 4. Write out results
    agg_df.write_parquet(agg_path)
