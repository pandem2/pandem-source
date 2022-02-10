require("R0")
library(MASS)
library(R0)
GT_pmf <- structure( c(0, 0.1, 0.1, 0.2, 0.2, 0.2, 0.1, 0.1), names=0:7)
GT_obj <- R0::generation.time("empirical", val=GT_pmf)
est_rt_wt <- function(ts, GT) {
   end <- length(ts) - (length(GT_obj$GT)-1)
   R0::est.R0.TD(ts, GT=GT_obj, begin=1, end=end, nsim=1000)
 }
if ((any(is.na(number_of_cases))) || (length(table(number_of_cases))==1) || (length(number_of_cases)<9)){
        res<-NA
}else{
        res<-est_rt_wt(number_of_cases, GT=GT_obj)
        res<-res$R
    }

res
