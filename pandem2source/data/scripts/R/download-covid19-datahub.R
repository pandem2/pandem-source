library(dplyr)
dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)  # create personal library
.libPaths(Sys.getenv("R_LIBS_USER")) 
if(!require("COVID19")) {
    devtools::install_github(repo = "covid19datahub/COVID19", ref = "107191f", upgrade = "never", lib = Sys.getenv("R_LIBS_USER"))
    library(COVID19)
}
message("getting data for covid19datahub :-) ")
df <- covid19(country = c(
  "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA", "DEU", "GRC", "HUN", "IRL", "ITA",
 "LVA", "LTU", "LUX", "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR"), level = 3
)
for(d in unique(df$date)) {
  ddf <- df %>% 
    filter(date == d)
  write.csv(ddf, paste(d, ".csv", sep = ""), row.names = FALSE)
}
message("getting covid-19 datahub data done")


