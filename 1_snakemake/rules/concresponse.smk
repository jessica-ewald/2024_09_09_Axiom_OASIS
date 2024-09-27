rule filter_cc:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        "outputs/{features}/{scenario}/profiles/{scenario}_filtcc.parquet",
    shell:
        "Rscript concresponse/filter_cc.R {input} {output}"

rule compute_distances:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}.parquet",
    output:
        expand("outputs/{{features}}/{{scenario}}/distances/{method}.parquet", method=config["distances"]),
    params:
        cover_var=config["cover_var"],
        treatment=config["treatment"],
        categories=config["categories"],
        distances=config["distances_R"],
    shell:
        """
        for method in {params.distances}; do
            method_name=$(echo $method | tr -d '[],"') 
            Rscript concresponse/compute_distances.R {input} outputs/{wildcards.features}/{wildcards.scenario}/distances/${{method_name}}.parquet {params.cover_var} {params.treatment} {params.categories} ${{method_name}}
        done
        """

rule compile_distances:
    input:
        lambda wildcards: [f"outputs/{wildcards.features}/{wildcards.scenario}/distances/{method}.parquet" for method in config["distances"]],
    output:
        "outputs/{features}/{scenario}/distances/distances.parquet",
    params:
        distances=config["distances"],
    shell:
        "Rscript concresponse/compile_distances.R {input} {output} {params.distances}"

rule prep_cmd:
    input:
        expand("outputs/{features}/{scenario}/profiles/{scenario}_filtcc.parquet", scenario=WORKFLOW, features=features),
    output:
        expand("outputs/{features}/{scenario}/cmd/{category}_rot.parquet",
               scenario=WORKFLOW, category=categories, features=features),
        expand("outputs/{features}/{scenario}/cmd/{category}_inv.parquet",
               scenario=WORKFLOW, category=categories, features=features)
    params:
        cover_var=config["cover_var"],
        treatment=config["treatment"],
    shell:
        "Rscript concresponse/prep_cmd.R {input} {output} {params.cover_var} {params.treatment}"

rule compute_cmd:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}_filtcc.parquet",
        expand("outputs/{features}/{scenario}/cmd/{category}_rot.parquet",
               scenario=WORKFLOW, category=categories, features=features),
        expand("outputs/{features}/{scenario}/cmd/{category}_inv.parquet",
               scenario=WORKFLOW, category=categories, features=features)
    output:
        "outputs/{features}/{scenario}/cmd/cmd.parquet",
    params:
        compound=config["compound"],
        ctrl=config["control"],
    shell:
        "Rscript concresponse/compute_cmd.R {input} {output} {params.compound} {params.ctrl}"

rule fit_curves:
    input:
        "outputs/{features}/{scenario}/distances.parquet",
    output:
        "outputs/{features}/{scenario}/curves/bmds.parquet",
    params:
        num_sds = config['num_sds']
    shell:
        "Rscript concresponse/fit_curves.R {input} {output} {params.num_sds}"

rule fit_curves_cc:
    input:
        "outputs/{features}/{scenario}/profiles/{scenario}_filtcc.parquet",
    output:
        "outputs/{features}/{scenario}/curves/ccpods.parquet",
    params:
        num_sds = config['num_sds']
    shell:
        "Rscript concresponse/fit_curves_cc.R {input} {output} {params.num_sds}"


rule select_pod:
    input:
        "outputs/{features}/{scenario}/curves/bmds.parquet",
        "outputs/{features}/{scenario}/curves/ccpods.parquet",
    output:
        "outputs/{features}/{scenario}/curves/pods.parquet",
    shell:
        "Rscript concresponse/select_pod.R {input} {output}"


rule plot_cc_curve_fits:
    input:
        "outputs/{features}/{scenario}/curves/ccpods.parquet",
        "outputs/{features}/{scenario}/profiles/{scenario}_filtcc.parquet",
    output:
        "outputs/{features}/{scenario}/curves/plots/cc_plots.pdf",
    shell:
        "Rscript concresponse/plot_cc_curve.R {input} {output}"


rule plot_cp_curve_fits:
    input:
        "outputs/{features}/{scenario}/curves/pods.parquet",
        "outputs/{features}/{scenario}/curves/ccpods.parquet",
        "outputs/{features}/{scenario}/distances.parquet",
    output:
        "outputs/{features}/{scenario}/curves/plots/cp_plots.pdf",
    shell:
        "Rscript concresponse/plot_cp_curve.R {input} {output}"