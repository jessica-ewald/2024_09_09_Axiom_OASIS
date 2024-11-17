rule create_classifier_profiles:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet",
        "outputs/{features}/{scenario}/curves/pods.parquet",
    output:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
    run:
        cl.aggregate_profiles.aggregate_profiles(*input, *output)


rule axiom_assay_hitcall:
    input:
        "outputs/{features}/{scenario}/curves/mttpods.parquet",
        "outputs/{features}/{scenario}/curves/ldhpods.parquet",
        "outputs/{features}/{scenario}/curves/ccpods.parquet",
    output:
        "outputs/{features}/{scenario}/curves/axiom_hits.parquet",
    run:
        cl.hitcalls.call_hits(*input, *output)


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


rule predict_axiom_continuous:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet"
    output:
        "outputs/{features}/{scenario}/classifier_results/axiom_continuous_predictions.parquet",
        "outputs/{features}/{scenario}/classifier_results/plots/axiom_continuous_predictions.pdf",
    run:
        cl.regression.predict_axiom_assays(*input, *output)

rule predict_axiom_continuous_null:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet"
    output:
        "outputs/{features}/{scenario}/classifier_results/axiom_continuous_null.parquet",
        "outputs/{features}/{scenario}/classifier_results/plots/axiom_continuous_null.pdf",
    params:
        shuffle = True
    run:
        cl.regression.predict_axiom_assays(*input, *output, shuffle=params.shuffle)

rule predict_axiom_binary:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
        "outputs/{features}/{scenario}/curves/axiom_hits.parquet",
    output:
        "outputs/{features}/{scenario}/classifier_results/axiom_binary_predictions.parquet",
    run:
        cl.classify.predict_binary(*input, *output)

rule predict_axiom_binary_null:
    input:
        "outputs/{features}/{scenario}/aggregated_profiles/agg.parquet",
        "outputs/{features}/{scenario}/curves/axiom_hits.parquet",
    output:
        "outputs/{features}/{scenario}/classifier_results/axiom_binary_null.parquet",
    params:
        shuffle = True
    run:
        cl.classify.predict_binary(*input, *output, shuffle=params.shuffle)