
require(tidyverse)
require(arrow)

############## 0. Read in and process data
args <- commandArgs(trailingOnly = TRUE)

input_file <- args[1]
output_rot <- args[2]
output_inv <- args[3]
cover_var <- args[4]
treatment <- args[5]

# Process data
dat <- read_parquet(input_file) %>% as.data.frame()
treatment_labels <- dat[, treatment] %>% c()

feat_cols <- colnames(dat)
feat_cols <- feat_cols[!grepl("Metadata_", feat_cols)]
dat <- dat[, feat_cols] %>% as.matrix()


############## 1. Calculate the Eigen features from the well-level data
pca <- prcomp(dat, center = TRUE, scale = TRUE)
rotation_matrix <- pca$rotation
cum_proportion <- cumsum(pca$sdev^2) / sum(pca$sdev^2)


##############  2. Find the inverse of the covariance matrix
pc <- length(which(cum_proportion < cover_var)) + 1
if (pc > dim(dat)[1]) {
  pc <- dim(dat)[1]
}
rotation_matrix <- rotation_matrix[, 1:pc]
model <- lm(pca$x[, 1:pc] ~ 0 + treatment_labels)

# get covariance matrix
cov <- estVar(model) %>% as.data.frame()

# compute inverse
inv_cov <- solve(cov) %>% as.data.frame()


##############  3. Write out results
write_parquet(as.data.frame(rotation_matrix), output_rot)
write_parquet(inv_cov, output_inv)