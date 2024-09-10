require(tidyverse)
require(arrow)


############## 0. Define useful functions

compute_matrices <- function(dat, cover_var, treatment_labels) {
  pca <- prcomp(dat, center = TRUE, scale = TRUE)
  rotation_matrix <- pca$rotation
  cum_proportion <- cumsum(pca$sdev^2) / sum(pca$sdev^2)

  pc <- length(which(cum_proportion < cover_var)) + 1
  if (pc > dim(dat)[1]) {
    pc <- dim(dat)[1]
  }
  rotation_matrix <- rotation_matrix[, 1:pc]
  model <- lm(pca$x[, 1:pc] ~ 0 + treatment_labels)

  # get covariance matrix
  cov <- as.data.frame(estVar(model))

  # compute inverse
  inv_cov <- solve(cov)

  return(list(rot_mat = rotation_matrix, inv = inv_cov))
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
output_rot <- args[2:16]
output_inv <- args[17:31]
cover_var <- args[32]
treatment <- args[33]

# Process data
dat <- read_parquet(input_file) %>% as.data.frame()
treatment_labels <- dat[, treatment] %>% c()

feat_cols <- colnames(dat)
feat_cols <- feat_cols[!grepl("Metadata_", feat_cols)]
noimg_cols <- feat_cols[!grepl("Image", feat_cols)]

dat <- dat[, feat_cols]

common_path <- comsub(output_rot)
categories <- gsub(common_path, "", output_rot)
categories <- gsub("_rot.parquet", "", categories)

############## 2. Compute rotation and inverse cov matrix for each category

for (category in categories) {
  print(category)

  cat_vals <- str_split(category, "_")
  compartment <- cat_vals[[1]][1]
  channel <- cat_vals[[1]][2]

  compartment_cols <- noimg_cols[grepl(compartment, noimg_cols)]
  category_cols <- compartment_cols[grepl(channel, compartment_cols)]

  category_dat <- dat[, category_cols] %>% as.matrix()

  category_res <- compute_matrices(category_dat, cover_var, treatment_labels)
  cat_rot <- category_res$rot_mat
  cat_inv <- category_res$inv

  rot_path <- paste0(common_path, category, "_rot.parquet")
  inv_path <- paste0(common_path, category, "_inv.parquet")

  write_parquet(as.data.frame(cat_rot), rot_path)
  write_parquet(as.data.frame(cat_inv), inv_path)
}