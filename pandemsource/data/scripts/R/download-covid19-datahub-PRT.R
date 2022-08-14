library(dplyr)
library(COVID19)
message("getting data for covid19datahub for Country PRT")
df <- covid19(country = c("PRT"), level = 3)

df$key_nuts <- ifelse(is.na(df$key_nuts), df$iso_alpha_2, df$key_nuts)
df$part <- paste(df$iso_alpha_2, substr(df$date, 1, 7), sep = "_")
for(n in unique(df$part)) {
  ddf <- df %>% 
    filter(part == n)
  write.csv(ddf, paste(n, ".csv", sep = ""), row.names = FALSE)
}
message("getting covid-19 datahub data done for country PRT done")


