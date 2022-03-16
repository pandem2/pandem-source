library(dplyr)
#dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)  # create personal library
#.libPaths(Sys.getenv("R_LIBS_USER")) 
#if(!require("COVID19")) {
#    devtools::install_github(repo = "covid19datahub/COVID19", ref = "b941b66e59b7b0aec4807eb5b28208abba66de4a", upgrade = "never", lib = Sys.getenv("R_LIBS_USER"))
    library(COVID19)
#}
message("getting data for covid19datahub :-) ")
df <- covid19(country = c(
  "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", 
  "FRA", "DEU", "GRC", "HUN", "IRL", "ITA",
 "LVA", "LTU", "LUX", "MLT", "NLD", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "GBR"
 ), level = 3
)

df$key_nuts <- ifelse(is.na(df$key_nuts), df$iso_alpha_2, df$key_nuts)
df$part <- paste(df$iso_alpha_2, substr(df$date, 1, 7), sep = "_")
for(n in unique(df$part)) {
  ddf <- df %>% 
    filter(part == n)
  write.csv(ddf, paste(n, ".csv", sep = ""), row.names = FALSE)
}
message("getting covid-19 datahub data done")


