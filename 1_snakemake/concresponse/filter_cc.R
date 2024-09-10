require(tidyverse)
require(arrow)


#### 1. Read in and process data
args <- commandArgs(trailingOnly = TRUE)

input_path <- args[1]
output_path <- args[2]

dat <- read_parquet(input_path)
dat$SampleID <- paste0("S", 1:dim(dat)[1])

dmso <- dat[dat$Metadata_Compound == "DMSO", ]
pert <- dat[dat$Metadata_Compound != "DMSO", ]

mad_factor <- 3

#### 2. Filter cell count outliers from each perturbation

# For DMSO, filter by plate
plates <- unique(dmso$Metadata_Plate)
remove_dmso <- c()
for (plate in plates) {
  temp <- dmso[dmso$Metadata_Plate == plate, ]
  med <- median(temp$Metadata_Count_Cells)
  mad <- mad(temp$Metadata_Count_Cells)
  lower_pert <- med - mad_factor * mad
  remove_temp <- temp$SampleID[temp$Metadata_Count_Cells < lower_pert]
  remove_dmso <- c(remove_dmso, remove_temp)
}

# For perturbations, use more relaxed filter to only get rid of extreme outliers
uniq_per <- unique(pert$Metadata_Perturbation)
mad_factor <- 3
remove_pert <- c()
for (per in uniq_per) {
  temp <- pert[pert$Metadata_Perturbation == per, ]
  med <- median(temp$Metadata_Count_Cells)
  mad <- mad(temp$Metadata_Count_Cells)
  lower_pert <- med - mad_factor * mad
  remove_temp <- temp$SampleID[temp$Metadata_Count_Cells < lower_pert]
  remove_pert <- c(remove_pert, remove_temp)
}


# Remove both samples
remove_samples <- c(remove_dmso, remove_pert)
dat <- dat[!(dat$SampleID %in% remove_samples), ]
dat <- dat[, -c(which(colnames(dat) == "SampleID"))]

write_parquet(dat, output_path)
