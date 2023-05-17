function(period, hospitalised_infected_patients){
    round(hospitalised_infected_patients*0.4 +runif(length(period),0.9,1.1)*(max(hospitalised_infected_patients)/20))
}