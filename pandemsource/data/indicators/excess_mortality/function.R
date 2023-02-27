#dummy data calculation for the excess mortality
#Assuming that normally 1% of the population dies each year and 
weekly_deaths = sapply(1:length(reporting_period), function(i) sum(sapply((i-6):i, function(j) if(j<1)  NA else (if(as.numeric(difftime(reporting_period[[i]], reporting_period[[j]], units = "days")) < 7) deaths_infected[[j]] else 0))))
expected_deads = 0.01 * population *(7/365)

100 * weekly_deaths/expected_deads
