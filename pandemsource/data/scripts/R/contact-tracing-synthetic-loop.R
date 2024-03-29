### Local access to the dataset loading dataframe

#setwd("C:/Users/Charline CLAIN/Documents/DataScience/pandem")
#df <- read.csv("timeserie-2023-04-18_13-45-19.csv")

###  Variables initialization for testing
#period <- df$date[df$ts_id == 1]
#coconfirmed_cases <- df$value[df$ts_id == 1]
#nb_contact_tracers = rep(10, length(period))
#full_ctp <- ifelse(confirmed_cases < 1000, 1, 0)
#partial_ctp <- ifelse(confirmed_cases > 1000 & confirmed_cases < 10000,1, 0)
#no_ctp <- ifelse(confirmed_cases > 10000,1,0)

day_loop <- function(period, confirmed_cases, nb_contact_tracers,full_ctp, partial_ctp, no_ctp){
  ret <- list()
  cases_unreached <- 0
  contacts_unreached <- 0
 
  for (i in 1:length(period)){
    ct_cap = nb_contact_tracers[i] * 20
    if(full_ctp[i]>0){
      new_cases_to_reach=confirmed_cases[i]
    } else if(partial_ctp[i]>0){
      new_cases_to_reach = round(mean(confirmed_cases[i],ct_cap)* runif(1, 0.95, 1.05))
    } else {
      new_cases_to_reach =0
    }
    if(is.na(new_cases_to_reach)) {
      new_cases_to_reach <- 0
    }
    if(is.na(ct_cap)) {
      ct_cap <- 0
    }
    cases_reached_day <- min(round(new_cases_to_reach * runif(1, 0.8, 1)), ct_cap) #calcul des nouveaux cas reached in the day
    cases_unreached_day <- new_cases_to_reach - cases_reached_day #    calcul des nouveaux cas UNreached in the day
    ct_cap <- ct_cap - cases_reached_day #calcul de la capacite restante
   
    cases_reached_with_delay<-min(ct_cap, cases_unreached)
    cases_unreached_with_delay <- cases_unreached - cases_reached_with_delay # cas en retard non contactés
    ct_cap <- ct_cap - cases_reached_with_delay #calcul de la capacite restante
   
   
    cases_reached <- cases_reached_day + cases_reached_with_delay#Somme des cas contactés dans la journée
    cases_unreached <- cases_unreached_day + cases_unreached_with_delay#Somme des cas non contactés dans la journée
   

    sum_contacts_identified<-round(runif(1, cases_reached*3,cases_reached*4))#Somme des contacts identifies
    contacts_reached_day<-min(ct_cap,sum_contacts_identified) #calcul des nouveauxcontacts reached in the day
    contacts_unreached_day<-sum_contacts_identified-contacts_reached_day # calcul des nouveauxcontacts UNreached in the day
   
    ct_cap<-ct_cap-contacts_reached_day #calcul de la capacite restante
    contacts_reached_with_delay_if_capa<-min(ct_cap,contacts_unreached) #contacts reached with delay if capacity
    contacts_unreached_with_delay<-(contacts_unreached-contacts_reached_with_delay_if_capa)# contacts enretard non contactés
   
    ct_cap<-ct_cap - contacts_reached_with_delay_if_capa#recalcul de la capaciterestante
    contacts_reached<-contacts_reached_day+contacts_reached_with_delay_if_capa #Somme des contacts contactésdans la journée
    contacts_unreached<-contacts_unreached_day + contacts_unreached_with_delay #Somme des contacts noncontactés dans la journée
    
    contact_tracing_cases_previously_contacts = round(sum_contacts_identified *(cases_reached_day/max(confirmed_cases[max(1, i-7):i])))
    
    ret[[i]] <- list(date = period[[i]],cases_to_reach = new_cases_to_reach,
                     cases_reached_day=cases_reached_day,cases_unreached_day=cases_unreached_day,
                     cases_reached=cases_reached, cases_unreached=cases_unreached,
                     sum_contacts_identified=sum_contacts_identified,contacts_reached_day=contacts_reached_day,
                     contacts_reached=contacts_reached,contacts_unreached=contacts_unreached, ct_cap=ct_cap,
                     contact_tracing_cases_previously_contacts=contact_tracing_cases_previously_contacts)
  }
  df = dplyr::bind_rows(ret)
  return (df)
}
