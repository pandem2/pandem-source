sapply(seq_along(reporting_period), function(i) {
  num_vec = primary_care_positive_cases 
  den_vec = primary_care_cases 
  num = sum(sapply(max(1, i-7):i, function(j) {
    if(as.numeric(as.Date(reporting_period[[i]]) - as.Date(reporting_period[[j]]), unit = "days") < 7)
        num_vec[[j]]
    else
      0
  }))
  den = sum(sapply(max(1, i-7):i, function(j) {
    if(as.numeric(as.Date(reporting_period[[i]]) - as.Date(reporting_period[[j]]), unit = "days") < 7)
        den_vec[[j]]
    else
      0
  }))
  if(den == 0)
    NA
  else
    num / den
})
