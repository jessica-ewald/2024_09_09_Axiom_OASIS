# Tracking experiments

Each of the config files contains different analysis parameters, and is thus a separate experiment into how various preprocessing steps impact assay prediction performance. The authors of 'Cell Painting for Cytotoxicity and Mode-of-Action Analysis' hope that the results presented in their paper are just the beginning, and that improved methods for predicting specific cellular events from images of cells will be developed in the future. We therefore hope that many more config files will be added to this repository over time.

## Reproduce manuscript results

To reproduce the manuscript results, use cellprofiler_filt.json, cpcnn.json, and dino.json to generate results for each cell representation with repo commit 6ea26865b1fedd7aed52c2184bdc5da9b6facbf0.

## Config naming conventions

- Prefixes of cellprofiler, cpcnn, and dino mean the pipeline was run starting with raw.parquet from that cell representation.
- The config parameter 'workflow' specifies the main processing steps used to generate input features for dose-response and classifier analysis.
- Sometimes additional transformations / filters were applied in the downstream analysis. The config name after the "_" reflect this, as does the 'name' config parameter.
- TODO future: provide definitions for all config parameters.
