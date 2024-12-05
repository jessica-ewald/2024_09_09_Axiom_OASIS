import pandas as pd
from smiles.standardize_smiles import StandardizeMolecule


# read in smiles
smiles = []
for i in range(1, 5):
    chunk = pl.read_csv(f"./output/smiles_chunk_{i}.csv")
    smiles.append(chunk)

smiles = pl.concat(smiles, how="vertical").rename({"SMILES": "smiles"})
smiles = smiles.filter(pl.col("smiles") != "N/A")
smiles.write_csv("./output/compiled_smiles.csv")
smiles = pd.read_csv("./output/compiled_smiles.csv")


# use large chunk size for first pass
chunk_size = 250
for i in range(0, smiles.shape[0], chunk_size):
    chunk = smiles.iloc[i:i + chunk_size]

    try:
        standardizer = StandardizeMolecule(
            input=chunk,
            num_cpu=70,
            augment=True
        )

        batch = standardizer.run()
        batch.to_csv(f"./output/standardized_smiles/standardized_smiles_{i // chunk_size + 1}.csv")
    except Exception as e:
        print(f"An error occurred: {e}")
        chunk.to_csv(f"./output/standardized_smiles/failed_smiles_{i // chunk_size + 1}.csv")


# Read in failed smiles
file_names = os.listdir("./output/standardized_smiles")
file_names = [i for i in file_names if "failed_smiles" in i]
smiles = []
for file_nm in file_names:
    chunk = pl.read_csv(f"./output/standardized_smiles/{file_nm}")
    smiles.append(chunk)

smiles = pl.concat(smiles, how="vertical")
smiles.write_csv("./output/compiled_smiles2.csv")
smiles = pd.read_csv("./output/compiled_smiles2.csv")


# use smaller chunk size for second pass
chunk_size = 25
for i in range(0, smiles.shape[0], chunk_size):
    chunk = smiles.iloc[i:i + chunk_size]

    try:
        standardizer = StandardizeMolecule(
            input=chunk,
            num_cpu=70,
            augment=True
        )

        batch = standardizer.run()
        batch.to_csv(f"./output/standardized_smiles/standardized_smiles2_{i // chunk_size + 1}.csv")
    except Exception as e:
        print(f"An error occurred: {e}")
        chunk.to_csv(f"./output/standardized_smiles/failed_smiles2_{i // chunk_size + 1}.csv")


# Read in failed smiles
file_names = os.listdir("./output/standardized_smiles")
file_names = [i for i in file_names if "failed_smiles2" in i]
smiles = []
for file_nm in file_names:
    chunk = pl.read_csv(f"./output/standardized_smiles/{file_nm}")
    smiles.append(chunk)

smiles = pl.concat(smiles, how="vertical")
smiles.write_csv("./output/compiled_smiles3.csv")
smiles = pd.read_csv("./output/compiled_smiles3.csv")


# Standardize 1-by-1
std_smiles = []
for i in range(0, smiles.shape[0]):
    chunk = smiles.iloc[i:i + 1]

    try:
        standardizer = StandardizeMolecule(
            input=chunk,
            num_cpu=1,
            augment=True
        )

        batch = standardizer.run()
        std_smiles.append(batch)
    except Exception as e:
        print(f"An error occurred: {e}")
        print(f"Failed for index {i}")

std_smiles_df = pd.concat(std_smiles, ignore_index=True)
std_smiles_df.to_csv(f"./output/standardized_smiles/standardized_smiles3.csv")


# Compile all standardized smiles
file_names = os.listdir("./output/standardized_smiles")
file_names = [i for i in file_names if "standardized_smiles" in i]
colnames = ["SMILES_original", "SMILES_standardized", "InChI_standardized", "InChIKey_standardized", "DTXSID", "PREFERRED_NAME"]
smiles = []
for file_nm in file_names:
    chunk = pl.read_csv(f"./output/standardized_smiles/{file_nm}").select(colnames)
    smiles.append(chunk)

smiles = pl.concat(smiles, how="vertical")
smiles.write_csv("./output/compiled_standardized_smiles.csv")


# Merge with refchemdb
refchemdb = pl.read_csv("./data/refchemdb_cmpd_gene.csv").rename({"dsstox_substance_id": "DTXSID"})
refchemdb = refchemdb.join(smiles, on="DTXSID", how="inner")
refchemdb = refchemdb.with_columns(
    pl.col("InChIKey_standardized").str.slice(0,14).alias("INCHI_14")
)
refchemdb.write_csv("./output/refchemdb_inchikey.csv")

# Count annotations per cmpd-target ann
counts = refchemdb.select(["DTXSID", "target", "mode"]).group_by(["target", "mode"]).agg(
    pl.len().alias("count")
)
counts.write_csv("./output/cmpd_gene_count.csv")


# Map to JUMP
jump = pl.read_csv("./data/JUMP_cmpds.csv")
jump = jump.with_columns(
    pl.col("Metadata_InChIKey").str.slice(0,14).alias("INCHI_14")
)
refchemdb_jump = refchemdb.join(jump, on="INCHI_14", how="inner")

# Map to OASIS
oasis = pl.read_csv("../1_snakemake/inputs/annotations/seal_input/v5_oasis_03Sept2024_simple.csv")
oasis = oasis.with_columns(
    pl.col("INCHIKEY").str.slice(0, 14).alias("INCHI_14")
)
refchemdb_oasis = refchemdb.join(oasis, on="INCHI_14", how="inner")