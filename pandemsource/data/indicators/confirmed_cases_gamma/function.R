start_date <- "2023-11-25"
if (geo_code == "NL" || geo_code == "DE") {
  library(Pandem2simulator)
  library(dplyr)
  library(lubridate)
  set.seed(1) 
  ratio = 60000 / max(confirmed_cases)
  all_cases <- data.frame(time = as_date(reporting_period), confirmed_cases = confirmed_cases)
  all_cases$cases <- round(all_cases$confirmed_cases * ratio)
  df <- Pandem2simulator::fx_simulator(data = all_cases,startdate = start_date)
  
  gamma = df[df$variant == "gamma",]$cases
  na_gamma = rep(NA, length(reporting_period[reporting_period < start_date]))
  mapping = sapply(1:(length(reporting_period)-length(na_gamma)), function(i) floor(i*length(gamma)/(length(reporting_period)-length(na_gamma))))
  mapped_gamma = c(na_gamma, sapply(1:length(mapping), function(i) round(gamma[[i]]/ratio)))
  mapped_gamma[[length(na_gamma)+1]] = 1
  final_gamma = sapply(1:length(confirmed_cases), function(i) if(is.na(mapped_gamma[[i]])) NA else{ if(confirmed_cases[[i]] < mapped_gamma[[i]]) confirmed_cases[[i]] else mapped_gamma[[i]]})
  alpha = confirmed_cases - ifelse(is.na(final_gamma), 0, final_gamma)
  final_gamma 
} else {
  ratio = ifelse(reporting_period < start_date, NA , atan(-10 + as.numeric(difftime(strptime(reporting_period, "%Y-%m-%d"), start_date, unit ='days'))^0.52)/ pi + 0.5)
  gamma = round(confirmed_cases * ratio)
  alpha <- confirmed_cases - gamma
  gamma
}
