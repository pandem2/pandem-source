max = 5
random_variance = 0.2
confirmed_cases_weight = 0

set.seed(10)
normal_weekly <- c(68,70,77,86,90,86,100,100,108,105,90,80,68,64,65,64,57,59,58,63,61,58,58,63,59,60,63,59,58,63,62,58,57,61,61,63,59,63,63,58,58,58,59,57,61,58,62,63,62,60,59,60,64)
#normal_weekly = round(normal_weekly / 28 + 150 + sample(-3:3, size = length(normal_weekly), replace = T))
normal_weekly = normal_weekly / max(normal_weekly)
reporting_period = as.Date(strptime("2022-01-01", format = "%Y-%m-%d")) + seq(0, 1000, by = 7)
confirmed_cases = c(rep(0, length(reporting_period[reporting_period < "2022-03-01"])), round((sin(-pi/2 + (1:length(reporting_period))/20)+1) * 1:length(reporting_period)))[1:length(reporting_period)]
confirmed_cases = confirmed_cases_weight * (confirmed_cases / max(confirmed_cases))
weeks_in_period <- as.integer(strftime(reporting_period, "%V"))
influenza_cases = normal_weekly[weeks_in_period]
total_cases = influenza_cases + confirmed_cases
round((total_cases / max(total_cases) * max) * (1 + random_variance*sample((-500:500), size = length(reporting_period), replace = TRUE)/500))*0.7
