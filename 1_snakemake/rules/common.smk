# Imports
import os
import preprocessing as pp
import classifier as cl
import concresponse as cr


# Rules
rule write_parquet:
    output:
        "outputs/{features}/{scenario}/profiles/raw.parquet",
    run:
        pp.io.write_parquet(config["input_path_profiles"], config["input_path_metadata"], *output)


rule compute_negcon_stats:
    input:
        "inputs/profiles/{features}/raw.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/neg_stats.parquet",
    run:
        pp.stats.compute_negcon_stats(*input, *output)


rule select_variant_feats:
    input:
        "inputs/profiles/{features}/raw.parquet",
        "outputs/{features}/{scenario}/profiles/neg_stats.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/variant_feats.parquet",
    run:
        pp.stats.select_variant_features(*input, *output)


rule compute_norm_stats:
    input:
        "outputs/{features}/{scenario}/profiles/mad.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/norm_stats.parquet",
    run:
        pp.stats.compute_stats(*input, *output)


rule iqr_outliers:
    input:
        "outputs/{features}/{scenario}/profiles/mad.parquet",
        "outputs/{features}/{scenario}/profiles/norm_stats.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/outliers.parquet",
    run:
        pp.outliers.iqr(config["iqr_scale"], *input, *output)