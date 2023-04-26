library(dplyr)

### Local access to the dataset loading dataframe
"""
setwd("C:/Users/Charline CLAIN/Documents/DataScience/pandem")
df <- read.csv("timeserie-2023-04-18_13-45-19.csv")
"""

###  Variables initialization

reporting_period <- df$date[df$ts_id == 1]
confirmed_cases <- df$value[df$ts_id == 1]
number_of_contact_tracers <- sapply(reporting_period, function(v) 1000)
full_contact_tracing <-  sample(df$value[df$ts_id == 4], length(reporting_period)) # sur mon export -> ts 4
limited_tracing <-   sample(df$value[df$ts_id == 3],length(reporting_period)) # sur mon export -> ts 3
no_contact_tracing <-   sample(df$value[df$ts_id == 2] , length(reporting_period))# sur mon export -> ts 2

day_loop <- function(period, confirmed_cases, df){ #sans recalcul des nouveaux cas
  nb_contact_tracers = 10
  ct_capacity_val = nb_contact_tracers * 100
  df_res <- data.frame(period,ct_capacity_val, confirmed_cases)
  df_res <- arrange(df_res,period)
 
  ### Cas
 
  for (i in 1:nrow(df_res)){df_res$cases_reached_day[i]<-min(df_res[i,3],df_res[i,2])} #calcul des nouveaux cas reached in the day
  for (i in 1:nrow(df_res)){df_res$cases_unreached_day[i]<-(df_res[i,3]-df_res[i,4])}# calcul des nouveaux cas UNreached in the day
  for (i in 1:nrow(df_res)){df_res$capacity_reeval[i]<-(df_res[i,2]-df_res[i,4])}#calcul de la capacite restante
  for (i in 1:nrow(df_res)){df_res$cases_reached_with_delay_if_capa[i]<-min(df_res[i-1,5],df_res[i,6])} # cases reached with delay if capacity
  # ligne 38 : attention valeur fausse pour la premiere ligne car on n'a pas de valeur
  for (i in 1:nrow(df_res)){df_res$cases_unreached_with_delay[i]<-(df_res[i-1,5]-df_res[i,7])}# cas en retard non contactés
  for (i in 1:nrow(df_res)){df_res$capacity_reeval_again[i]<-(df_res[i,6]-df_res[i,7])}#recalcul de la capacite restante
  for (i in 1:nrow(df_res)){df_res$sum_cases_reached[i]<-(df_res[i,4]+df_res[i,7])}#Somme des cas contactés dans la journée
  for (i in 1:nrow(df_res)){df_res$sum_cases_unreached[i]<-(df_res[i,5]+df_res[i,8])}#Somme des cas non contactés dans la journée
 
  ### Contacts
  for (i in 1:nrow(df_res)){df_res$sum_contacts_identified[i]<-(df_res[i,10]*sample(5:6,1))}#Somme des contacts identifies
  for (i in 1:nrow(df_res)){df_res$contacts_reached_day[i]<-min(df_res[i,9],df_res[i,12])} #calcul des nouveaux contacts reached in the day
  for (i in 1:nrow(df_res)){df_res$contacts_unreached_day[i]<-(df_res[i,12]-df_res[i,13])}# calcul des nouveaux contacts UNreached in the day
 
  for (i in 1:nrow(df_res)){df_res$capacity_reeval_contact[i]<-(df_res[i,9]-df_res[i,13])}#calcul de la capacite restante
  for (i in 1:nrow(df_res)){df_res$contacts_reached_with_delay_if_capa[i]<-min(df_res[i-1,14],df_res[i,15])} # contacts reached with delay if capacity
  for (i in 1:nrow(df_res)){df_res$contacts_unreached_with_delay[i]<-(df_res[i-1,14]-df_res[i,16])}# contacts en retard non contactés
 
  for (i in 1:nrow(df_res)){df_res$capacity_reeval_again2[i]<-(df_res[i,15]-df_res[i,16])}#recalcul de la capacite restante
  for (i in 1:nrow(df_res)){df_res$sum_contacts_reached[i]<-(df_res[i,13]+df_res[i,16])}#Somme des contacts contactés dans la journée
  for (i in 1:nrow(df_res)){df_res$sum_contacts_unreached[i]<-(df_res[i,14]+df_res[i,17])}#Somme des contacts non contactés dans la journée
 
  return(df_res$contacts_reached_day)
}

df_res <- day_loop(reporting_period,limited_tracing,df )
