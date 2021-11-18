library(dplyr)
dir.create(Sys.getenv("R_LIBS_USER"), recursive = TRUE)  # create personal library
.libPaths(Sys.getenv("R_LIBS_USER")) 
if(!require("COVID19")) {
    devtools::install_github(repo = "covid19datahub/COVID19", ref = "1b5b5d7", upgrade = "never", lib = Sys.getenv("R_LIBS_USER"))
    library(COVID19)
}
message("getting data for covid19datahub :-) ")
df <- covid19(country = c("FRA", "DEU"), level = 3)
for(d in unique(df$date)) {
  ddf <- df %>% 
    select(key_nuts, date, confirmed, iso_alpha_2) %>%
    filter(date == d)
  write.csv(ddf, paste(d, ".csv", sep = ""), row.names = TRUE)
}
message("getting data for done")


