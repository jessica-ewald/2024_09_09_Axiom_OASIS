"""Download Axiom metadata.

Download all Axiom platemap and biochem metadata.

"""  # noqa: CPY001, INP001

import re

from sh import aws


def main() -> None:
    """Download metadata.

    Read in index file, download data.

    """
    aws_path = "s3://cellpainting-gallery/cpg0037-oasis/axiom/workspace/metadata"
    local_meta = "../1_snakemake/inputs/metadata"
    batches = ["prod_25", "prod_26", "prod_27", "prod_30"]

    # get metadata (both biochem.parquet and metadata.parquet)
    for batch in batches:
        batch_path = f"{aws_path}/{batch}/"
        aws_output = aws("s3", "ls", batch_path)
        plates = re.findall(r"plate_\d{8}", aws_output)

        for plate in plates:
            aws("s3", "cp", f"{batch_path}{plate}/metadata.parquet", f"{local_meta}/metadata/metadata_{plate}.parquet")
            aws("s3", "cp", f"{batch_path}{plate}/biochem.parquet", f"{local_meta}/biochem/biochem_{plate}.parquet")


if __name__ == "__main__":
    main()
