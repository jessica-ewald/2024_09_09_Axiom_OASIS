rule mad_normalize:
    input:
        "outputs/{features}/{scenario}/profiles/variant_feats.parquet",
        "outputs/{features}/{scenario}/profiles/neg_stats.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/mad.parquet",
    run:
        pp.normalize.mad(*input, *output)


rule spherize:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_spherize.parquet",
    run:
        pp.normalize.spherize(*input, *output)


rule drop:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
        "outputs/{features}/{scenario}/profiles/outliers.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_drop.parquet",
    run:
        pp.outliers.drop_cols(*input, *output)


rule clip:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
        "outputs/{features}/{scenario}/profiles/outliers.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_clip.parquet",
    params:
        clip_value=config["clip_value"],
    run:
        pp.outliers.clip_cols(*input, *params, *output)


rule INT:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_int.parquet",
    run:
        pp.transform.rank_int(*input, *output)


rule featselect:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_featselect.parquet",
    run:
        pp.select_features(*input, *output)


rule imputeknn:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
        "outputs/{features}/{scenario}/profiles/outliers.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_imputeknn.parquet",
    params:
        clip_value=config["clip_value"],
    run:
        pp.outliers.impute_knn(*input, *output)


rule imputemedian:
    input:
        "outputs/{features}/{scenario}/profiles/{pipeline}.parquet",
        "outputs/{features}/{scenario}/profiles/outliers.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{pipeline}_imputemedian.parquet",
    params:
        clip_value=config["clip_value"],
    run:
        pp.outliers.impute_median(*input, *output)