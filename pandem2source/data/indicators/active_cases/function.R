active_days = 10
if(length(confirmed_cases)>0) 
  sapply(1:length(confirmed_cases), function(i) sum(confirmed_cases[max(1, i-active_days):i], na.rm = T))
else 
  confirmed_cases
