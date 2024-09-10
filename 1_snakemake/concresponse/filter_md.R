require(tidyverse)
require(arrow)


#### 1. Read in and process data
args <- commandArgs(trailingOnly = TRUE)

input_gmd <- args[1]
input_cmd <- args[2]
output_gmd <- args[3]
output_cmd <- args[4]

mad_factor <- 3

gmd <- read_parquet(input_gmd) %>% as.data.frame()
cmd <- read_parquet(input_cmd) %>% as.data.frame()

gmd_dmso_all <- gmd[gmd$Metadata_Compound == "DMSO", ]
gmd_pert <- gmd[gmd$Metadata_Compound != "DMSO", ]
cmd_dmso_all <- cmd[cmd$Metadata_Compound == "DMSO", ]
cmd_pert <- cmd[cmd$Metadata_Compound != "DMSO", ]

perturbations <- unique(gmd_pert$Metadata_Perturbation)
categories <- colnames(cmd)
categories <- categories[!grepl("Metadata_", categories)]


#### 2. Filter DMSO (remove samples from gmd, replace with NA for cmd)
plates <- unique(gmd_dmso_all$Metadata_Plate)
gmd_dmso_filt <- data.frame()
cmd_dmso_filt <- data.frame()
for (plate in plates){
  gmd_dmso <- gmd_dmso_all[gmd_dmso_all$Metadata_Plate == plate, ]
  cmd_dmso <- cmd_dmso_all[cmd_dmso_all$Metadata_Plate == plate, ]

  gmd_med_dmso <- median(gmd_dmso$gmd)
  gmd_mad_dmso <- mad(gmd_dmso$gmd)

  gmd_lower_dmso <- gmd_med_dmso - (mad_factor * gmd_mad_dmso)
  gmd_upper_dmso <- gmd_med_dmso + (mad_factor * gmd_mad_dmso)

  gmd_dmso <- gmd_dmso[gmd_dmso$gmd > gmd_lower_dmso, ]
  gmd_dmso <- gmd_dmso[gmd_dmso$gmd < gmd_upper_dmso, ]

  for (category in categories) {
    cmd_med_dmso <- median(cmd_dmso[, category])
    cmd_mad_dmso <- mad(cmd_dmso[, category])

    cmd_lower_dmso <- cmd_med_dmso - (mad_factor * cmd_mad_dmso)
    cmd_upper_dmso <- cmd_med_dmso + (mad_factor * cmd_mad_dmso)

    cmd_dmso[, category][cmd_dmso[, category] < cmd_lower_dmso] <- NA
    cmd_dmso[, category][cmd_dmso[, category] > cmd_upper_dmso] <- NA
  }

  gmd_dmso_filt <- rbind(gmd_dmso_filt, gmd_dmso)
  cmd_dmso_filt <- rbind(cmd_dmso_filt, cmd_dmso)
}


#### 3. Filter each perturbation
gmd_filt <- data.frame()
cmd_filt <- data.frame()
for (per in perturbations) {
  temp_gmd <- gmd_pert[gmd_pert$Metadata_Perturbation == per, ]
  temp_cmd <- cmd_pert[cmd_pert$Metadata_Perturbation == per, ]

  # gmd
  gmd_med <- median(temp_gmd$gmd)
  gmd_mad <- mad(temp_gmd$gmd)
  gmd_lower <- gmd_med - (mad_factor * gmd_mad)
  gmd_upper <- gmd_med + (mad_factor * gmd_mad)
  temp_gmd <- temp_gmd[temp_gmd$gmd > gmd_lower, ]
  temp_gmd <- temp_gmd[temp_gmd$gmd < gmd_upper, ]

  gmd_filt <- rbind(gmd_filt, temp_gmd)

  # cmd
  for (category in categories) {
    cmd_med <- median(temp_cmd[, category])
    cmd_mad <- mad(temp_cmd[, category])

    cmd_lower <- cmd_med - (mad_factor * cmd_mad)
    cmd_upper <- cmd_med + (mad_factor * cmd_mad)

    temp_cmd[, category][temp_cmd[, category] < cmd_lower] <- NA
    temp_cmd[, category][temp_cmd[, category] > cmd_upper] <- NA
  }
  cmd_filt <- rbind(cmd_filt, temp_cmd)
}


#### 4. Write out results
gmd_filt <- rbind(gmd_dmso_filt, gmd_filt)
cmd_filt <- rbind(cmd_dmso_filt, cmd_filt)

write_parquet(gmd_filt, output_gmd)
write_parquet(cmd_filt, output_cmd)