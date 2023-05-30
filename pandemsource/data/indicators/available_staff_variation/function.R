sapply(seq(reporting_period), function(i) {
  a = available_staff[[i]]
  b = available_staff[[1]]
  (a - b) / b
})
