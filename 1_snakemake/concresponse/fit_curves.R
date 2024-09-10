source("./concresponse/fastbmdR_main.R")
source("./concresponse/fastbmdR_utils.R")
require(tidyverse)
require(arrow)


######## 1. Parse arguments and define parameters
args <- commandArgs(trailingOnly = TRUE)

gmd_path <- args[1]
cmd_path <- args[2]
output_path <- args[3]
num_sds <- args[4]

pos_cons <- c("Berberine chloride", "FCCP")
ctrl <- "DMSO"


######## 2. Calculate BMDs from global mahalanobis distances
gmd_dat <- read_parquet(gmd_path) %>% as.data.frame()
gmd_dat <- gmd_dat[!(gmd_dat$Metadata_SampleType %in% pos_cons), ]

compounds <- unique(gmd_dat$Metadata_Compound)
compounds <- compounds[compounds != ctrl]

gmd_dmso <- gmd_dat[gmd_dat$Metadata_SampleType == ctrl, ]
gmd_comp <- gmd_dat[gmd_dat$Metadata_SampleType != ctrl, ]

bmd_res <- data.frame()
for (compound in compounds){
  gmd_comp_fit <- gmd_comp[gmd_comp$Metadata_Compound == compound, ]
  cmpd_plates <- gmd_comp_fit$Metadata_Plate
  gmd_dmso_fit <- gmd_dmso[gmd_dmso$Metadata_Plate %in% cmpd_plates, ]
  gmd_fit <- rbind(gmd_dmso_fit, gmd_comp_fit)
  gmd_fit <- gmd_fit[order(gmd_fit$Metadata_Concentration), ]

  gmd <- matrix(gmd_fit$gmd, nrow = 1)
  rownames(gmd) <- "gmd"

  dose <- c(gmd_fit$Metadata_Log10Dose)

  gmd_pod <- scoresPOD(gmd, dose, log10.dose = TRUE, num.sds = num_sds,
                       filt.var = "SDres")
  if (!is.null(gmd_pod)) {
    gmd_pod$Metadata_Compound <- compound
    bmd_res <- rbind(bmd_res, gmd_pod)
  }
}


######## 3. Calculate BMDs from categorical mahalanobis distances
cmd_dat <- read_parquet(cmd_path) %>% as.data.frame()
cmd_dat <- cmd_dat[!(cmd_dat$Metadata_SampleType %in% pos_cons), ]

cat_cols <- colnames(cmd_dat)
cat_cols <- cat_cols[!grepl("Metadata_", cat_cols)]

compounds <- unique(cmd_dat$Metadata_Compound)
compounds <- compounds[compounds != ctrl]

cmd_dmso <- cmd_dat[cmd_dat$Metadata_SampleType == ctrl, ]
cmd_comp <- cmd_dat[cmd_dat$Metadata_SampleType != ctrl, ]

for (compound in compounds){
  cmd_comp_fit <- cmd_comp[cmd_comp$Metadata_Compound == compound, ]
  cmpd_plates <- cmd_comp_fit$Metadata_Plate
  cmd_dmso_fit <- cmd_dmso[cmd_dmso$Metadata_Plate %in% cmpd_plates, ]
  cmd_fit <- rbind(cmd_dmso_fit, cmd_comp_fit)
  cmd_fit <- cmd_fit[order(cmd_fit$Metadata_Concentration), ]

  cmd <- cmd_fit[, cat_cols] %>% as.matrix() %>% t()
  rownames(cmd) <- cat_cols

  dose <- c(cmd_fit$Metadata_Log10Dose)

  cmd_pod <- scoresPOD(cmd, dose, log10.dose = TRUE, num.sds = num_sds,
                       filt.var = "SDres")
  if (!is.null(cmd_pod)) {
    cmd_pod$Metadata_Compound <- compound
    bmd_res <- rbind(bmd_res, cmd_pod)
  }
}

write_parquet(as.data.frame(bmd_res), output_path)