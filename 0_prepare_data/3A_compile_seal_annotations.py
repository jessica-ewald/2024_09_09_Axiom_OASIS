import polars as pl  # noqa: CPY001, D100, INP001

# For now, we will ignore the more complex in vivo/ADME outcomes
ann_files = [
    #"FINAL_oasis_fdaranks_cleaned.csv",
    #"FINAL_oasis_proxyDILI_cleaned.csv",
    #"FINAL_oasis_TDC_ADME_cleaned.csv",
    "FINAL_oasis_toxcast_cleaned.csv",
    "FINAL_targetannotations_singleproteins_oasiscompounds.csv",
]


def main() -> None:
    """Compile annotations from Srijit.

    Select variables of interest and separate based on binary/continuous.

    """
    rel_path = "../1_snakemake/inputs/annotations/seal_input"
    oasis_id = (
        pl.read_csv(f"{rel_path}/v5_oasis_03Sept2024_simple.csv").select("OASIS_ID").to_series().unique().to_list()
    )

    binary = pl.DataFrame({
        "OASIS_ID": oasis_id,
    })

    continuous = pl.DataFrame({
        "OASIS_ID": oasis_id,
    })

    for af in ann_files:
        dat = pl.read_csv(f"{rel_path}/{af}")

        if "fdaranks" in af:
            dat_binary = dat.select([
                "OASIS_ID",
                "DIRIL",
                "DICTrank",
            ]).rename({
                "DIRIL": "Renal_DIRIL",
                "DICTrank": "Cardio_DICTrank",
            })
            dat_binary = dat_binary.unique()
            binary = binary.join(dat_binary, on="OASIS_ID", how="left")

        if "proxyDILI" in af:
            dat_binary = dat.select([
                "OASIS_ID",
                "BESP",
                "animal hepatotoxicity Ambe et al",
                "animal hepatotoxicity Liu et al",
                "human hepatotoxicity Mulliner et al",
                "preclinical hepatotoxicity Mulliner et al",
            ]).rename({
                "BESP": "Hepato_BESP",
                "animal hepatotoxicity Ambe et al": "Hepato_animal_Ambe",
                "animal hepatotoxicity Liu et al": "Hepato_animal_Liu",
                "human hepatotoxicity Mulliner et al": "Hepato_human_Mulliner",
                "preclinical hepatotoxicity Mulliner et al": "Hepato_preclinical_Mulliner",
            })
            dat_binary = dat_binary.unique()
            binary = binary.join(dat_binary, on="OASIS_ID", how="left")

        if "ADME" in af:
            dat_binary = dat.select([
                "OASIS_ID",
                "Y_PAMPA_NCATS",
                "Y_HIA_Hou",
                "Y_Pgp_Broccatelli",
                "Y_Bioavailability_Ma",
                "Y_BBB_Martins",
                "Y_CYP2C19_Veith",
                "Y_CYP2D6_Veith",
                "Y_CYP3A4_Veith",
                "Y_CYP1A2_Veith",
                "Y_CYP2C9_Veith",
                "Y_CYP2C9_Substrate_CarbonMangels",
                "Y_CYP2D6_Substrate_CarbonMangels",
                "Y_CYP3A4_Substrate_CarbonMangels",
            ])
            adme_cols = dat_binary.columns
            adme_cols = [item.replace("Y_", "ADME_") if item.startswith("Y_") else item for item in adme_cols]

            dat_binary.columns = adme_cols
            dat_binary = dat_binary.unique()
            binary = binary.join(dat_binary, on="OASIS_ID", how="left")

            # continuous values
            dat_continuous = dat.select([
                "OASIS_ID",
                "Y_Caco2_Wang",
                "Y_Lipophilicity_AstraZeneca",
                "Y_Solubility_AqSolDB",
                "Y_HydrationFreeEnergy_FreeSolv",
                "Y_PPBR_AZ",
            ])
            adme_cols = dat_continuous.columns
            adme_cols = [item.replace("Y_", "ADME_") if item.startswith("Y_") else item for item in adme_cols]

            dat_continuous.columns = adme_cols
            dat_continuous = dat_continuous.unique()
            continuous = continuous.join(dat_continuous, on="OASIS_ID", how="left")

        if "toxcast" in af:
            dat_binary = dat.drop([
                "DTXSID",
                "PREFERRED_NAME",
                "DTXCID",
                "CASRN",
                "INCHIKEY",
                "IUPAC_NAME",
                "SMILES",
                "INCHI_STRING",
                "DILILIST_ID",
                "DILIRANK2_ID",
                "DRUGMATRIX_ID",
                "FRAUNHOFER_ID",
                "ICE_ID",
                "TGGATES_ID",
                "TOXREFDB_ID",
                "Invivo_ranking",
                "Purchased_Broad_Enamine",
                "Purchased_Broad_Molport",
                "Purchased_Broad_Medchemxpress",
                "InChIKey14",
            ])

            toxcast_cols = dat_binary.columns
            toxcast_cols = [item if item == "OASIS_ID" else f"Toxcast_{item}" for item in toxcast_cols]
            dat_binary.columns = toxcast_cols
            dat_binary = dat_binary.unique()
            binary = binary.join(dat_binary, on="OASIS_ID", how="left")

        if "targetannotations" in af:
            dat_binary = dat.drop([
                "DTXSID",
                "PREFERRED_NAME",
                "DTXCID",
                "CASRN",
                "INCHIKEY",
                "IUPAC_NAME",
                "SMILES",
                "INCHI_STRING",
                "DILILIST_ID",
                "DILIRANK2_ID",
                "DRUGMATRIX_ID",
                "FRAUNHOFER_ID",
                "ICE_ID",
                "TGGATES_ID",
                "TOXREFDB_ID",
                "Invivo_ranking",
                "Purchased_Broad_Enamine",
                "Purchased_Broad_Molport",
                "Purchased_Broad_Medchemxpress",
                "InChIKey14",
                "Standardized_SMILES_chembl",
                "Standardized_InChI_chembl",
                "Standardized_InChIKey_chembl",
                "ChEMBL_ID",
                "molecule_chembl_id",
            ])

            target_cols = dat_binary.columns
            target_cols = [item if item == "OASIS_ID" else f"Target_{item}" for item in target_cols]
            dat_binary.columns = target_cols
            dat_binary = dat_binary.unique()
            binary = binary.join(dat_binary, on="OASIS_ID", how="left")

    binary.write_parquet("../1_snakemake/inputs/annotations/seal_binary.parquet")
    continuous.write_parquet("../1_snakemake/inputs/annotations/seal_continuous.parquet")


if __name__ == "__main__":
    main()
