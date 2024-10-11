rule mad_normalize:
    input:
        f"outputs/{features}/{scenario}/profiles/variant_feats.parquet",
        f"outputs/{features}/{scenario}/profiles/neg_stats.parquet",
    output:
        f"outputs/{features}/{scenario}/profiles/mad.parquet",
    run:
        pp.normalize.mad(*input, *output)


rule featselect:
    input:
        f"outputs/{features}/{scenario}/profiles/mad.parquet",
    output:
        f"outputs/{features}/{scenario}/profiles/mad_featselect.parquet",
    run:
        pp.select_features(*input, *output)