import anndata
import polars as pl
import scanpy as sc


def make_umaps(prof_path: str, morph_pod: str, cc_pod: str, ldh_pod: str, mtt_pod: str, plot_path: str) -> None:
    data = pl.read_parquet(prof_path)

    cc = (
        pl.read_parquet(cc_pod)
        .filter(
            pl.col("all.pass") is True,
        )
        .select(["Metadata_Compound", "bmd"])
        .rename({"bmd": "Metadata_cc_POD"})
    )
    ldh = (
        pl.read_parquet(ldh_pod)
        .filter(
            pl.col("all.pass") is True,
        )
        .select(["Metadata_Compound", "bmd"])
        .rename({"bmd": "Metadata_ldh_POD"})
    )
    mtt = (
        pl.read_parquet(mtt_pod)
        .filter(
            pl.col("all.pass") is True,
        )
        .select(["Metadata_Compound", "bmd"])
        .rename({"bmd": "Metadata_mtt_POD"})
    )
    morph = pl.read_parquet(morph_pod).select(["Metadata_Compound", "bmd"]).rename({"bmd": "Metadata_morph_POD"})

    data = data.join(cc, on="Metadata_Compound", how="left")
    data = data.join(ldh, on="Metadata_Compound", how="left")
    data = data.join(mtt, on="Metadata_Compound", how="left")
    data = data.join(morph, on="Metadata_Compound", how="left")

    metadata_cols = [col for col in data.columns if "Metadata" in col]

    data = data.to_pandas()
    data.sort_values(["Metadata_Plate", "Metadata_Well"], inplace=True)
    data.index = [f"{row['Metadata_Plate']}__{row['Metadata_Well']}" for _, row in data.iterrows()]
    data = data.loc[~data.index.duplicated(keep="first")]

    metadata = data[metadata_cols]
    adata = anndata.AnnData(X=data.drop(metadata_cols, axis=1))
    adata.obs = adata.obs.merge(metadata, left_index=True, right_index=True)

    sc.pp.neighbors(adata)
    sc.tl.umap(adata)

    # Plot UMAP coloured by cell count
    sc.pl.embedding(
        adata,
        "X_umap",
        color="Metadata_Count_Cells",
        s=10,
    )
