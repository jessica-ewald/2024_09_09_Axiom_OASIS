require(tidyverse)
require(arrow)

compute_gmd <- function(dat, rot_mat, inv_cov, treatment, control) {
  # get PC scores using loadings and number PCs computed previously
  dat <- dat %*% rot_mat

  # compute the centroid of control samples
  ctrl_mean <- apply(dat[treatment == control, ], 2, mean)

  # subtract the control centroid from each sample
  delta <- sweep(dat, 2, as.matrix(ctrl_mean), "-")

  # compute the Mahalanobis distance
  gmd <- apply(delta, 1, function(x) {
    gmd_util(x, inv_cov)
  })

  return(gmd)
}

gmd_util <- function(x, inv_cov) {
  x_int <- x %*% inv_cov %*% x
  x_int <- sqrt(x_int)
  return(round(x_int, 3))
}

##### 1. Get parameters
args <- commandArgs(trailingOnly = TRUE)

input_file <- args[1]
rot_file <- args[2]
inv_file <- args[3]
output_gmd <- args[4]
compound <- args[5]
control <- args[6]


##### 2. Read in data
all_dat <- read_parquet(input_file) %>% as.data.frame()
dat_cols <- colnames(all_dat)
feat_cols <- dat_cols[!grepl("Metadata_", dat_cols)]
meta_cols <- dat_cols[grepl("Metadata_", dat_cols)]

rot_mat <- read_parquet(rot_file) %>% as.matrix()
inv_cov <- read_parquet(inv_file) %>% as.matrix()


##### 3. Compute gmd
plates <- unique(all_dat$Metadata_Plate)
gmd_df <- data.frame()
for (plate in plates) {
  plate_dat <- all_dat[all_dat$Metadata_Plate == plate, feat_cols]
  plate_dat <- as.matrix(plate_dat)
  plate_meta <- all_dat[all_dat$Metadata_Plate == plate, meta_cols]
  plate_labels <- plate_meta[, compound] %>% c()

  gmd <- compute_gmd(plate_dat, rot_mat, inv_cov, plate_labels, control)
  plate_meta$gmd <- gmd
  gmd_df <- rbind(gmd_df, plate_meta)
}

write_parquet(gmd_df, output_gmd)