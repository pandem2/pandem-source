library(dplyr)
dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)  # create personal library
.libPaths(Sys.getenv("R_LIBS_USER")) 
if(!require("COVID19")) {
    devtools::install_github(repo = "covid19datahub/COVID19", ref = "7f20c86", upgrade = "never", lib = Sys.getenv("R_LIBS_USER"))
    library(COVID19)
}
message("getting data for covid19datahub :-) ")
df <- covid19(country = c(
  #"AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", 
  "FRA"
 #, "DEU", "GRC", "HUN", "IRL", "ITA",
 #"LVA", "LTU", "LUX", "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR"
 ), level = 3
)

df$key_nuts <- ifelse(is.na(df$key_nuts), df$iso_alpha_2, df$key_nuts)
for(n in unique(df$key_nuts)) {
  ddf <- df %>% 
    filter(key_nuts == n)
  write.csv(ddf, paste(n, ".csv", sep = ""), row.names = FALSE)
}
message("getting covid-19 datahub data done")


