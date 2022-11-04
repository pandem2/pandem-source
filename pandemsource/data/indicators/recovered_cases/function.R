active_days <- 10
if (length(confirmed_cases) > 0) {
    sapply(seq_len(length(confirmed_cases)), function(i) {
    if (i <= 11) {
      return(0)
    } else {
      return(sum(confirmed_cases[1:(i - active_days)], na.rm = T))
    }
  })
} else {
    confirmed_cases
}
