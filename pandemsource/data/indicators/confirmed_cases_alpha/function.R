start_date <- "2023-10-15"
ratio = ifelse(reporting_period < start_date, NA , atan(-10 + as.numeric(difftime(strptime(reporting_period, "%Y-%m-%d"), start_date, unit ='days'))^0.52)/ pi + 0.5)
gamma = round(confirmed_cases * ratio)
alpha <- confirmed_cases - gamma
alpha
