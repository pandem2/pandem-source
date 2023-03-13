start_date <- "2023-10-25"
if (geo_code == "NL" || geo_code == "DE") {
  library(Pandem2simulator)
  library(dplyr)
  library(lubridate)
  set.seed(1) 
  ratio = 60000 / max(confirmed_cases)
  all_cases <- data.frame(time = as_date(reporting_period), confirmed_cases = confirmed_cases)
  all_cases$cases <- round(all_cases$confirmed_cases * ratio)
  df <- Pandem2simulator::fx_simulator(data = all_cases,startdate = start_date)
  df <- df %>% 
    rowwise() %>%
    mutate(round = round(cases / ratio))%>%
    mutate(time = as_date(time))%>% 
    group_by(time)%>% 
    mutate(somme = sum(round))%>% 
    mutate(moins = somme - confirmed_cases)%>%
    select(-c("cases","somme"))
  
  gamma <- df %>% filter(variant =='gamma') %>%  mutate(variant ="Gamma") %>% rowwise() %>% mutate(round = round - moins)
  other <- df %>% filter(variant !='gamma') %>% mutate(variant ="Alpha")
  
  df = union_all(gamma,other) %>% 
    group_by(time)%>% 
    mutate(somme = sum(round))%>% 
    mutate(moins = somme - confirmed_cases)%>%
    rowwise()%>%
    mutate(cases = ifelse(variant == "other",round - moins,round))%>%
    arrange(as_date(time))%>% select(-c("moins","somme","confirmed_cases","round"))
  
  zero_cases <- all_cases %>% filter(as_date(time) < as_date(min(df$time))) %>% mutate(variant = "Alpha") %>% select(-cases)%>% rename(cases = confirmed_cases) %>% select(time,variant,cases)
  
  all = all_cases %>% mutate(variant = "All")%>% select(-cases)%>% rename(cases = confirmed_cases)%>% select(time,variant,cases)
  df = union_all(zero_cases,df)
  df = union_all(df,all)
  (df %>% filter(variant == "Gamma"))$cases
} else {
  ratio = ifelse(reporting_period < start_date, NA , atan(-10 + as.numeric(difftime(strptime(reporting_period, "%Y-%m-%d"), start_date, unit ='days'))^0.52)/ pi + 0.5)
  gamma = round(confirmed_cases * ratio)
  alpha <- confirmed_cases - gamma
  gamma
}
