import polars as pl


def compile_dist(input_files: list, output_path: str) -> None:
    dfs = []
    for fp in input_files:
        dat = pl.read_parquet(fp)
        dfs.append(dat.unique())

    dfs = pl.concat(dfs, how="vertical")
    meta_cols = [i for i in dfs.columns if "Metadata" in i and i != "Metadata_Distance"]
    df_wide = dfs.pivot(
        values="Distance",
        index=meta_cols,
        columns="Metadata_Distance",
        aggregate_function=None,
    )

    df_wide.write_parquet(output_path)
