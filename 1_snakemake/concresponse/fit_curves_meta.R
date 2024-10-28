require(dplyr)
require(arrow)

######## 0. Make sure fastbmdR is installed
if (!requireNamespace("fastbmdR", quietly = TRUE)) {
  
  # Check if devtools is installed, and install it if not
  if (!requireNamespace("devtools", quietly = TRUE)) {
    install.packages("devtools")
  }
  
  # Install fastbmdR from GitHub
  devtools::install_github("jessica-ewald/fastbmdR@v0.0.0.9000")
}
library(fastbmdR)


######## 1. Parse arguments and define parameters
args <- commandArgs(trailingOnly = TRUE)

dat_path <- args[1]
output_path <- args[2]
num_sds <- args[3]
meta_nm <- args[4]

ctrl <- "DMSO"


######## 2. Calculate BMDs from cell counts
dat <- read_parquet(dat_path) %>% as.data.frame()

compounds <- unique(dat$Metadata_Compound)
compounds <- compounds[compounds != ctrl]

cc_dmso <- dat[dat$Metadata_Compound == ctrl, ]
cc_comp <- dat[dat$Metadata_Compound != ctrl, ]

bmd_res <- data.frame()
for (compound in compounds){
  cc_comp_fit <- cc_comp[cc_comp$Metadata_Compound == compound, ]
  cmpd_plates <- cc_comp_fit$Metadata_Plate
  cc_dmso_fit <- cc_dmso[cc_dmso$Metadata_Plate %in% cmpd_plates, ]
  cc_fit <- rbind(cc_dmso_fit, cc_comp_fit)
  cc_fit <- cc_fit[order(cc_fit$Metadata_Concentration), ]

  cc <- matrix(c(cc_fit[, meta_nm]), nrow = 1)
  rownames(cc) <- "cc"

  dose <- c(cc_fit$Metadata_Log10Conc)

  cc_pod <- scoresPOD(cc, dose, log10.dose = TRUE, num.sds = num_sds)
  cc_pod$Metadata_Compound <- compound
  bmd_res <- rbind(bmd_res, cc_pod)
}

write_parquet(as.data.frame(bmd_res), output_path)