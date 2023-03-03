#cumsum(round(confirmed_cases * sqrt(confirmed_cases / max(confirmed_cases))) * 5)
cumsum(round(
  confirmed_cases + sqrt(confirmed_cases / max(confirmed_cases))*max(confirmed_cases)*0.8* runif(n = length(confirmed_cases), min = 0.98, max = 1.02)
))

