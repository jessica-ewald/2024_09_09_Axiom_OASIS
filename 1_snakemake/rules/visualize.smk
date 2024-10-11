rule make_umaps:
    input:
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
        f"outputs/{features}/{scenario}/curves/pods.parquet",
        f"outputs/{features}/{scenario}/curves/ccpods.parquet",
        f"outputs/{features}/{scenario}/curves/ldhpods.parquet",
        f"outputs/{features}/{scenario}/curves/mttpods.parquet",
    output:
        f"outputs/{features}/{scenario}/figures/umaps.pdf",
    run:
        vs.umaps.make_umaps(*input, *output)