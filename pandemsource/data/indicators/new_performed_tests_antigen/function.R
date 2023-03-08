#new_performed_tests * 0.4
library(dplyr)
library(p2synthr)
i<-which(new_performed_tests!=0, arr.ind = TRUE)[1]
if(i+30<=length(new_performed_tests)){
	rep(0,length(new_performed_tests))
}else{
	new_performed_tests[1:i+30] <- 0
	last <- synth1(new_performed_tests[i+30:length(new_performed_tests)], group_names=c("antigen", "naats","unknow"), group_prob=c(0.4,0.5, 0.1), setSeed = T, seedValue = 10)$antigen
	c(new_performed_tests[1:i+30], last)
}
