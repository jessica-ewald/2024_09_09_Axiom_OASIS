require(tidyverse)
require(arrow)

############## 0. Read in and process data
args <- commandArgs(trailingOnly = TRUE)

input_file <- args[1]
output_dist <- args[2]
cover_var <- args[3]
treatment <- args[4]
categories <- args[5]
methods <- args[6]

print(input_file)
print(output_dist)
print(cover_var)
print(treatment)
print(categories)
print(methods)

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

  if (!file.exists(output_dist)) {
    gmd_prep <- prep_gmd(dat, cover_var, treatment_labels)

    plates <- unique(all_dat$Metadata_Plate)
    gmd_df <- data.frame()
    for (plate in plates) {
      plate_dat <- all_dat[all_dat$Metadata_Plate == plate, feat_cols]
      plate_dat <- as.matrix(plate_dat)
      plate_meta <- all_dat[all_dat$Metadata_Plate == plate, meta_cols]
      plate_labels <- plate_meta[, "Metadata_Compound"] %>% c()

      gmd <- compute_gmd(plate_dat, gmd_prep$rot, gmd_prep$inv_cov,
                         plate_labels, "DMSO")
      plate_meta$Metadata_Distance <- "gmd"
      plate_meta$Distance <- gmd
      gmd_df <- rbind(gmd_df, plate_meta)
    }
    write_parquet(gmd_df, output_dist)
  }
}


############## 2. cmd
if ("cmd" %in% methods) {
  source("./concresponse/cmd_functions.R")

  cmd_df <- data.frame()
  categories <- unlist(strsplit(categories, ","))

  # detect feature type
  if (grepl("_", categories[1])) {
    feat_type <- "cellprofiler"
  } else {
    feat_type <- "dino"
  }

  for (category in categories) {
    print(category)
    if (feat_type == "dino") {
      category_cols <- colnames(dat)[grepl(category, colnames(dat))]
    } else {
      category_cols <- "WRITE HERE"
    }

    # Extract category profile
    category_dat <- dat[, category_cols]
    category_res <- compute_matrices(category_dat, cover_var, treatment_labels)

    plates <- unique(all_dat$Metadata_Plate)
    for (plate in plates) {
      plate_dat <- all_dat[all_dat$Metadata_Plate == plate, category_cols]
      plate_dat <- as.matrix(plate_dat)
      plate_meta <- all_dat[all_dat$Metadata_Plate == plate, meta_cols]
      plate_labels <- plate_meta[, "Metadata_Compound"] %>% c()

      cmd <- compute_cmd(plate_dat, category_res$rot_mat,
                         category_res$inv, plate_labels, "DMSO")
      plate_meta$Metadata_Distance <- category
      plate_meta$Distance <- cmd
      cmd_df <- rbind(cmd_df, plate_meta)
    }
  }
  ## NEED STEP TO CAST meta X Metadata_Distance, with Distance as value variable
  write_parquet(cmd_df, output_dist)
}