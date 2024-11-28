import polars as pl  # noqa: CPY001, D100, INP001

min_set_size = 5
max_set_size = 1000

# Get OASIS IDs
oasis = pl.read_csv("../1_snakemake/inputs/annotations/seal_input/v5_oasis_03Sept2024_simple.csv")
oasis = oasis.select("OASIS_ID").to_series().unique().to_list()
oasis = pl.DataFrame({
    "OASIS_ID": oasis,
})

# Process motive annotations
arevalo = pl.read_parquet("../1_snakemake/inputs/annotations/cg_motive.parquet")
arevalo = arevalo.with_columns(
    (pl.concat_str(["target", "interaction_type"], separator="_")).alias("cmpd_target_interaction"),
).select(["OASIS_ID", "cmpd_target_interaction"])

int_counts = arevalo.select(pl.col("cmpd_target_interaction").value_counts()).unnest("cmpd_target_interaction")
int_counts = int_counts.filter(pl.col("counts") > min_set_size).filter(pl.col("counts") < max_set_size)
keep_int = int_counts.select("cmpd_target_interaction").to_series().to_list()

arevalo = arevalo.filter(pl.col("cmpd_target_interaction").is_in(keep_int))

# Convert to binary matrix
binary_matrix = (
    arevalo.with_columns(pl.lit(1).alias("value"))
    .pivot_table(
        index="OASIS_ID",
        columns="cmpd_target_interaction",
        values="value",
        aggregate_function="first",
    )
    .fill_null(0)
)
binary_matrix = oasis.join(binary_matrix, on="OASIS_ID", how="left").fill_null(0)
binary_matrix.write_parquet("../1_snakemake/inputs/annotations/motive_binary.parquet")
