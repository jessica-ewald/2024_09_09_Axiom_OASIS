import os

from sh import aws


def main() -> None:
    path_prefix = "/Users/jewald/repos/2024_09_09_Axiom_OASIS/1_snakemake/inputs/profiles/dino/cpg0037-oasis/axiom/workspace/scratch"
    aws_prefix = "s3://staging-cellpainting-gallery/cpg0037-oasis/axiom/workspace_dl/profiles/cpcnn_zenodo_7114558"
    local_prefix = "/Users/jewald/Desktop/cpg0037_parquet"

    parent_directories = [
        f"{path_prefix}/prod_25",
        f"{path_prefix}/prod_26",
        f"{path_prefix}/prod_27",
        f"{path_prefix}/prod_30",
    ]
    subdir_to_parent = {}
    for parent_dir in parent_directories:
        for root, dirs, files in os.walk(parent_dir):
            for sub_dir in dirs:
                subdir_to_parent[sub_dir] = os.path.basename(root)

    # Hardcode exceptions
    subdir_to_parent["plate_41002908"] = "prod_27"
    subdir_to_parent["plate_41002960"] = "prod_30"
    subdir_to_parent["plate_41002687"] = "prod_25"
    subdir_to_parent["plate_41002891"] = "prod_26"

    plates = os.listdir(local_prefix)
    for plate in plates:
        print(plate)
        plate_nm = plate.replace(".parquet", "")
        batch = subdir_to_parent[plate_nm]

        aws_path = f"{aws_prefix}/{batch}/{plate}"
        local_path = f"{local_prefix}/{plate}"

        aws("s3", "cp", local_path, aws_path)


if __name__ == "__main__":
    main()
