#new_performed_tests * 0.5
library(dplyr)
library(p2synthr)
i<-which(new_performed_tests!=0, arr.ind = TRUE)[1]
wait = 60
if(i+wait >= length(new_performed_tests)) {
	synth1(new_performed_tests, group_names=c("naats","unknow"), group_prob=c(0.7,0.3), setSeed = T, seedValue = 10)$naats
}else{
  first <- synth1(new_performed_tests[1:i+wait], group_names=c("naats","unknow"), group_prob=c(0.7,0.3), setSeed = T, seedValue = 10)$naats
	last <- synth1(new_performed_tests[i + wait + 1:length(new_performed_tests)], group_names=c("antigen", "naats","unknow"), group_prob=c(0.4,0.5, 0.1), setSeed = T, seedValue = 10)$naats
	c(first, last)
}
