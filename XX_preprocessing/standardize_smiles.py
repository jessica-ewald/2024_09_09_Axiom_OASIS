import pandas as pd
from smiles.standardize_smiles import StandardizeMolecule

smiles = pd.read_csv("./output/compiled_smiles.csv")

for i in range(250, smiles.shape[0], 250):
    chunk = smiles.iloc[i:i + 250]

    try:
        standardizer = StandardizeMolecule(
            input=chunk,
            num_cpu=70,
            augment=True
        )

        batch = standardizer.run()
        batch.to_csv(f"./output/standardized_smiles/standardized_smiles_{i // 250 + 1}.csv")
    except Exception as e:
        print(f"An error occurred: {e}")
        chunk.to_csv(f"./output/standardized_smiles/failed_smiles_{i // 250 + 1}.csv")