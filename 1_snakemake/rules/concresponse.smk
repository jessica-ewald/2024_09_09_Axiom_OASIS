rule compute_distances_R:
    input:
        f"outputs/{features}/{scenario}/profiles/{{pipeline}}.parquet",
    output:
        expand("outputs/{features}/{scenario}/distances/{method}.parquet", method=config["distances_R"], features=config["features"], scenario=config["workflow"]),
    params:
        cover_var=config["cover_var"],
        treatment=config["treatment"],
        categories=",".join(config["categories"]),
        distances=config["distances_R"],
    shell:
        """
        for method in {params.distances}; do
            method_name=$(echo $method | tr -d '[],"') 
            Rscript concresponse/compute_distances.R {input} outputs/{wildcards.features}/{wildcards.scenario}/distances/${{method_name}}.parquet {params.cover_var} {params.treatment} {params.categories} ${{method_name}}
        done
        """

rule compute_distances_python:
    input:
        f"outputs/{features}/{scenario}/profiles/{{pipeline}}.parquet",
    output:
        expand("outputs/{features}/{scenario}/distances/{method}.parquet", method=config["distances_python"], features=config["features"], scenario=config["workflow"]),
    params:
        distances=config["distances_python"],
    run:
        output_files = list(output)
        cr.ap.ap(*input, output_files, params.distances)


distances = config["distances_R"] + config["distances_python"]
rule compile_distances:
    input:
        [f"outputs/{features}/{scenario}/distances/{method}.parquet" for method in distances],
    output:
        f"outputs/{features}/{scenario}/distances/distances.parquet",
    run:
        input_files = list(input)
        cr.compile_dist.compile_dist(input_files, *output)


rule fit_curves:
    input:
        f"outputs/{features}/{scenario}/distances/distances.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/bmds.parquet",
    params:
        num_sds = config['num_sds']
    shell:
        "Rscript concresponse/fit_curves.R {input} {output} {params.num_sds}"

rule fit_curves_cc:
    input:
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/ccpods.parquet",
    params:
        num_sds = config['num_sds'],
        meta_nm = "Metadata_Count_Cells"
    shell:
        "Rscript concresponse/fit_curves_meta.R {input} {output} {params.num_sds} {params.meta_nm}"

rule fit_curves_mtt:
    input:
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/mttpods.parquet",
    params:
        num_sds = config['num_sds'],
        meta_nm = "Metadata_mtt_normalized"
    shell:
        "Rscript concresponse/fit_curves_meta.R {input} {output} {params.num_sds} {params.meta_nm}"

rule fit_curves_ldh:
    input:
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/ldhpods.parquet",
    params:
        num_sds = config['num_sds'],
        meta_nm = "Metadata_ldh_abs_signal"
    shell:
        "Rscript concresponse/fit_curves_meta.R {input} {output} {params.num_sds} {params.meta_nm}"

rule select_pod:
    input:
        f"outputs/{features}/{scenario}/curves/bmds.parquet",
        f"outputs/{features}/{scenario}/curves/ccpods.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/pods.parquet",
    shell:
        "Rscript concresponse/select_pod.R {input} {output}"


rule plot_cc_curve_fits:
    input:
        f"outputs/{features}/{scenario}/curves/ccpods.parquet",
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/plots/cc_plots.pdf",
    params:
        meta_nm = "Metadata_Count_Cells"
    shell:
        "Rscript concresponse/plot_meta_curve.R {input} {output} {params.meta_nm}"

rule plot_mtt_curve_fits:
    input:
        f"outputs/{features}/{scenario}/curves/mttpods.parquet",
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/plots/mtt_plots.pdf",
    params:
        meta_nm = "Metadata_mtt_normalized"
    shell:
        "Rscript concresponse/plot_meta_curve.R {input} {output} {params.meta_nm}"

rule plot_ldh_curve_fits:
    input:
        f"outputs/{features}/{scenario}/curves/ldhpods.parquet",
        f"outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/plots/ldh_plots.pdf",
    params:
        meta_nm = "Metadata_ldh_abs_signal"
    shell:
        "Rscript concresponse/plot_meta_curve.R {input} {output} {params.meta_nm}"


rule plot_cp_curve_fits:
    input:
        f"outputs/{features}/{scenario}/curves/pods.parquet",
        f"outputs/{features}/{scenario}/curves/ccpods.parquet",
        f"outputs/{features}/{scenario}/distances/distances.parquet",
    output:
        f"outputs/{features}/{scenario}/curves/plots/cp_plots.pdf",
    shell:
        "Rscript concresponse/plot_cp_curve.R {input} {output}"