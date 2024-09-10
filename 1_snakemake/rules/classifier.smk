rule create_classifier_profiles:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet",
        "outputs/{features}/{scenario}/curves/pods.parquet",
        "outputs/{features}/{scenario}/gmd/global_rot.parquet",

    output:
        "outputs/{features}/{scenario}/aggregated_profiles/cpfeat.parquet",
        "outputs/{features}/{scenario}/aggregated_profiles/latent.parquet",

    run:
        cl.aggregate_profiles.aggregate_profiles(*input, *output)


rule classify:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/cpfeat.parquet",
        "outputs/{features}/{scenario}/aggregated_profiles/latent.parquet",
        # LABELS

    output:
        "outputs/{features}/{scenario}/classifier_results/predictions.parquet",

    run:
        cl.classify.classify(*input, *output)