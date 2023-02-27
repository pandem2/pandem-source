library(Pandem2simulator)
df0 = data.frame(time = reporting_period, cases = confirmed_cases)
df <- Pandem2simulator::fx_simulator(df0,startdate = "2023-10-15")
gamma <- df[df$variant == "gamma" & !is.na(df$cases) & df$cases > 0,]$cases

l = nrow(df0)
ind_start_date = nrow(df0[df0$time < "2023-10-15",])
seq_zeros = sample(0, size = ind_start_date, replace = TRUE)
cases_fin = c(seq_zeros,gamma)
cases_fin = cases_fin[0:l]
cases_fin <- ifelse(is.na(cases_fin), 0, cases_fin)
print(cases_fin)
cases_fin

