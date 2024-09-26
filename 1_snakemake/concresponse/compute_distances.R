require(tidyverse)
require(arrow)

############## 0. Read in and process data
args <- commandArgs(trailingOnly = TRUE)

input_file <- args[1]
output_dist <- args[2]
cover_var <- args[3]
treatment <- args[4]
methods <- args[5]

# Process data
all_dat <- read_parquet(input_file) %>% as.data.frame()

dat_cols <- colnames(all_dat)
feat_cols <- dat_cols[!grepl("Metadata_", dat_cols)]
meta_cols <- dat_cols[grepl("Metadata_", dat_cols)]

treatment_labels <- all_dat[, treatment] %>% c()

dat <- all_dat[, feat_cols] %>% as.matrix()

############## 1. gmd
if ("gmd" %in% methods) {
  source("./concresponse/gmd_functions.R")

  gmd_prep <- prep_gmd(dat, cover_var, treatment_labels)

  plates <- unique(all_dat$Metadata_Plate)
  gmd_df <- data.frame()
  for (plate in plates) {
    plate_dat <- all_dat[all_dat$Metadata_Plate == plate, feat_cols]
    plate_dat <- as.matrix(plate_dat)
    plate_meta <- all_dat[all_dat$Metadata_Plate == plate, meta_cols]
    plate_labels <- plate_meta[, "Metadata_Compound"] %>% c()

    gmd <- compute_gmd(plate_dat, gmd_prep$rot_mat, gmd_prep$inv_cov,
                       plate_labels, control)
    plate_meta$gmd <- gmd
    gmd_df <- rbind(gmd_df, plate_meta)

    # Add gmd to dist dataframe
    if (dim(dist_df)[1] == 0) {
      dist_df <- gmd_df
    } else {
      dist_df <- merge(dist_df, gmd_df, by = meta_cols, all = TRUE)
    }
  }

  write_parquet(dist_df, output_dist)
}


############## 2. cmd


############## 3. ap