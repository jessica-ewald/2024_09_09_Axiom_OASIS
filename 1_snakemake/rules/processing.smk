wildcard_constraints:
    pipeline=r"[_a-zA-Z.~0-9\-]*"

rule mad_normalize:
    input:
        f"outputs/{features}/{scenario}/profiles/variant_feats.parquet",
        f"outputs/{features}/{scenario}/profiles/neg_stats.parquet",
    output:
        f"outputs/{features}/{scenario}/profiles/mad.parquet",
    run:
        pp.normalize.mad(*input, *output)


rule int:
    input:
        f"outputs/{features}/{scenario}/profiles/{{pipeline}}.parquet",
    output:
        f"outputs/{features}/{scenario}/profiles/{{pipeline}}_int.parquet",
    run:
        pp.select_features(*input, *output)


rule featselect:
    input:
        f"outputs/{features}/{scenario}/profiles/{{pipeline}}.parquet",
    output:
        f"outputs/{features}/{scenario}/profiles/{{pipeline}}_featselect.parquet",
    run:
        pp.select_features(*input, *output)