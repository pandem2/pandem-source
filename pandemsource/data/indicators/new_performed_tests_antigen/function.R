#new_performed_tests * 0.4
library(dplyr)
library(p2synthr)
i<-which(new_performed_tests!=0, arr.ind = TRUE)[1]
wait = 60
if(i+wait >= length(new_performed_tests)) {
	rep(0,length(new_performed_tests))
}else{
	first <- rep(0,i+wait)
  last <- synth1(new_performed_tests[i + wait + 1:length(new_performed_tests)], group_names=c("antigen", "naats","unknow"), group_prob=c(0.4,0.5, 0.1), setSeed = T, seedValue = 10)$antigen
	c(first, last)
}
