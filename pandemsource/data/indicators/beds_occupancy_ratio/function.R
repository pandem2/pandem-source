fillgaps <- function(x) {
  goodIdx <- !is.na(x)
  goodVals <- c(x[goodIdx][1], x[goodIdx])
  fillIdx <- cumsum(goodIdx)+1
  goodVals[fillIdx]
}


hospitalised_infected_patients / fillgaps(number_of_operable_beds)
