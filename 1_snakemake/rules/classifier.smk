rule create_classifier_profiles:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet",
        "outputs/{features}/{scenario}/curves/pods.parquet",
    output:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",

    run:
        cl.aggregate_profiles.aggregate_profiles(*input, *output)


rule predict_seal_binary:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
        "inputs/annotations/seal_binary.parquet",
    output:
        "outputs/{features}/{scenario}/classifier_results/seal_binary_predictions.parquet",
    run:
        cl.classify.predict_binary(*input, *output)


rule seal_binary_null:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
        "inputs/annotations/seal_binary.parquet",
    output:
        "outputs/{features}/{scenario}/classifier_results/seal_binary_null.parquet",
    params:
        shuffle = True
    run:
        cl.classify.predict_binary(*input, *output, shuffle=params.shuffle)


rule predict_motive_binary:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
        "inputs/annotations/motive_binary.parquet",
    output:
        "outputs/{features}/{scenario}/classifier_results/motive_binary_predictions.parquet",
    run:
        cl.classify.predict_binary(*input, *output)


rule motive_binary_null:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
        "inputs/annotations/motive_binary.parquet",
    output:
        "outputs/{features}/{scenario}/classifier_results/motive_binary_null.parquet",
    params:
        shuffle = True
    run:
        cl.classify.predict_binary(*input, *output, shuffle=params.shuffle)


rule predict_axiom_assays:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet"
    output:
        "outputs/{features}/{scenario}/classifier_results/axiom_assay_predictions.parquet",
        "outputs/{features}/{scenario}/classifier_results/plots_continuous.pdf",
    run:
        cl.regression.predict_axiom_assays(*input, *output)