#new_performed_tests * 0.1
library(dplyr)
library(p2synthr)
synth1(new_performed_tests, group_names=c("antigen","naats","unknow"), group_prob=c(0.4,0.5,0.1), setSeed = T, seedValue = 10)$unknow
