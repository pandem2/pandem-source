library(Pandem2simulator)
ratio = 60000 / max(confirmed_cases)
df0 = data.frame(time = reporting_period, cases = round(confirmed_cases * ratio))
df <- Pandem2simulator::fx_simulator(df0,startdate = "2023-10-15")
gamma <- df[df$variant == "gamma" & !is.na(df$cases) & df$cases > 0,]$cases
gamma <- round(gamma / ratio)

l = nrow(df0)
ind_start_date = nrow(df0[df0$time < "2023-10-15",])
seq_zeros = sample(0, size = ind_start_date, replace = TRUE)
cases_fin = c(seq_zeros,gamma)
cases_fin = cases_fin[0:l]
cases_fin <- ifelse(is.na(cases_fin), 0, cases_fin)
cases_fin <- sapply(1:length(confirmed_cases), function(i) min(confirmed_cases[[i]], cases_fin[[i]]))
cases_fin

