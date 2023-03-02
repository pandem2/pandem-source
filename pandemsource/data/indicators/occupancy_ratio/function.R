fillgaps <- function(x) {
  goodIdx <- !is.na(x)
  goodVals <- c(x[goodIdx][1], x[goodIdx])
  fillIdx <- cumsum(goodIdx)+1
  goodVals[fillIdx]
}

number_of_patients / fillgaps(number_of_beds)
