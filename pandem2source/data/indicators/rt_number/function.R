# source https://staff.math.su.se/hoehle/blog/2020/04/15/effectiveR0.html
confirmed_cases = sapply(confirmed_cases, function(v) {if(is.na(v)) 0 else v})  

#' R_e(t) estimator with generation time GT (default GT=4). Note: The time t
#' is assigned to the last day in the blocks of cases on [t,t+1,t+2, t+3] vs.
#' [t+4,t+5,t+6, t+7], i.e. s=t-8 (c.f. p. 14 of 2020-04-24 version of the RKI paper)
#' @param ts - Vector of integer values containing the time series of incident cases
#' @param GT - PMF of the generation time is a fixed point mass at the value GT.

est_rt_last <- function(ts, GT=4L) {
  # Sanity check
  if (!is.integer(GT) | !(GT>0)) stop("GT has to be postive integer.")
    # Estimate, if s=1 is t-7
  res <- sapply( 1:length(ts), function(t) {
    if((t < 2*GT) || sum(ts[t-2*GT+1:GT]) == 0)
      NA
    else
      sum(ts[t-(0:(GT-1))]) / sum(ts[t-2*GT+1:GT])
  })
  names(res) <- names(ts)[(2*GT):length(ts)]
  return(res)
}

if(length(confirmed_cases)<9) { 
  sapply(confirmed_cases, function(v) NA)
} else {
  # using GT = 7 to limit dayofthe week impact
  est_rt_last(confirmed_cases, GT=7L)
}
