#cumsum(round(confirmed_cases * sqrt(confirmed_cases / max(confirmed_cases))) * 5)
cumsum(round(
round(confirmed_cases / (confirmed_cases / max(confirmed_cases)*0.4
+ runif(n = length(confirmed_cases), min = 0.8, max = 1.2)))
))

