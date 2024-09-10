
require(tidyverse)
require(arrow)

compute_cmd <- function(dat, rot_mat, inv_cov, treatment, control) {
  # get PC scores using loadings and number PCs computed previously
  dat <- dat %*% rot_mat

  # compute the centroid of control samples
  ctrl_mean <- apply(dat[treatment == control, ], 2, mean)

  # subtract the control centroid from each sample
  delta <- sweep(dat, 2, as.matrix(ctrl_mean), "-")

  # compute the Mahalanobis distance
  gmd <- apply(delta, 1, function(x) {
    cmd_util(x, inv_cov)
  })

  return(gmd)
}

cmd_util <- function(x, inv_cov) {
  x_int <- x %*% inv_cov %*% x
  x_int <- sqrt(x_int)
  return(round(x_int, 3))
}

comsub <- function(x) {
  # sort the vector
  x <- sort(x)
  # split the first and last element by character
  d_x <- strsplit(x[c(1, length(x))], "")
  # search for the first not common element and so, get the last matching one
  der_com <- match(FALSE, do.call("==", d_x)) - 1
  # return empty vector if no matching, else return the common part
  ifelse(der_com == 0, return(character(0)), return(substr(x[1], 1, der_com)))
}

############## 1. Read in and process data
args <- commandArgs(trailingOnly = TRUE)

input_file <- args[1]
input_rot <- args[2:16]
input_inv <- args[17:31]
output_path <- args[32]
compound <- args[33]
ctrl <- args[34]

# Process data
all_dat <- read_parquet(input_file) %>% as.data.frame()

all_cols <- colnames(all_dat)
meta_cols <- all_cols[grepl("Metadata_", all_cols)]
feat_cols <- all_cols[!grepl("Metadata_", all_cols)]
noimg_cols <- feat_cols[!grepl("Image", feat_cols)]

common_path <- comsub(input_rot)
categories <- gsub(common_path, "", input_rot)
categories <- gsub("_rot.parquet", "", categories)

##############  2. Compute distances for each category
plates <- unique(all_dat$Metadata_Plate)
cmd_df <- data.frame()
for (plate in plates) {
  plate_inds <- which(all_dat$Metadata_Plate == plate)
  plate_dat <- all_dat[plate_inds, noimg_cols]
  plate_meta <- all_dat[plate_inds, meta_cols]
  plate_labels <- plate_meta[, compound] %>% c()

  for (i in seq_along(input_rot)) {
    cat_rot <- read_parquet(input_rot[i]) %>% as.data.frame() %>% as.matrix()
    cat_inv <- read_parquet(input_inv[i]) %>% as.data.frame() %>% as.matrix()

    category <- categories[i]
    cat_vals <- str_split(category, "_")
    compartment <- cat_vals[[1]][1]
    channel <- cat_vals[[1]][2]
    compartment_cols <- noimg_cols[grepl(compartment, noimg_cols)]
    category_cols <- compartment_cols[grepl(channel, compartment_cols)]
    category_dat <- plate_dat[, category_cols] %>% as.matrix()

    cmd <- compute_cmd(category_dat, cat_rot, cat_inv, plate_labels, ctrl)
    plate_meta[, category] <- cmd
  }
  cmd_df <- rbind(cmd_df, plate_meta)
}

write_parquet(as.data.frame(cmd_df), output_path)