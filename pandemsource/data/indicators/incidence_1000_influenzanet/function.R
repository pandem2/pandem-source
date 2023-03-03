set.seed(10)
normal_weekly = c(44,59,113,161,196,171,235,240,285,259,188,100,67,30,10,5,1,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,1,2,0,3,4,4,2,4,2,17,30)
normal_weekly <- (normal_weekly/max(normal_weekly))*max(confirmed_cases)/2
weeks_in_period <- as.integer(strftime(reporting_period, "%V"))
influenza_cases = normal_weekly[weeks_in_period]
#total_cases = influenza_cases + confirmed_cases
total_cases = influenza_cases + confirmed_cases + round(runif(n = length(confirmed_cases), min = 10, max = 20))
(total_cases / max(influenza_cases) * 50) * (1 + sample((-5:5), size = length(reporting_period), replace = TRUE)/20)
