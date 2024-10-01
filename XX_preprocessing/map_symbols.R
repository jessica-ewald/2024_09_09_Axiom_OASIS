# Map symbols to Entrez IDs
# Jessica Ewald
# Oct 1, 2025

library(dplyr)
library(arrow)
library(biomaRt)
library(mygene)

gene_symbols <- read_csv("/Users/jewald/Desktop/gene_symbols.csv", show_col_types = FALSE)$target %>% unique()

# First try map with biomart
ensembl <- useMart("ensembl", dataset = "hsapiens_gene_ensembl")

biomart_results <- getBM(attributes = c("hgnc_symbol", "ensembl_gene_id"),
                 filters = "hgnc_symbol",
                 values = gene_symbols,
                 mart = ensembl) %>% na.omit()

biomart_results <- biomart_results[!duplicated(biomart_results$hgnc_symbol), ]


# Next, try mapping missing symbols with MyGene
missing_symbols <- gene_symbols[!(gene_symbols %in% biomart_results$hgnc_symbol)] %>% unique()

mg <- MyGene()

mygene_results <- queryMany(missing_symbols, scopes = "symbol", fields = "ensembl.gene", species = "human", returnall=TRUE)
mygene_results <- mygene_results$response %>% as.data.frame()

mygene_results$mapped_id <- lapply(mygene_results$ensembl, function(x){
  ids <- x[[1]]
  if (class(ids) == "data.frame"){
    ids <- ids$gene
  }
  
  if(length(ids) > 0){
    res <- ids[1]
  } else {
    res <- NA
  }
  return(res)
  }) %>% unlist()

mygene_results <- mygene_results[,c("query", "mapped_id")] %>% na.omit()
colnames(mygene_results) <- colnames(biomart_results)

# Symbol -> Ensembl
symbol_ensembl <- rbind(biomart_results, mygene_results)
write_csv(symbol_ensembl, "/Users/jewald/Desktop/symbol_ensembl.csv")
