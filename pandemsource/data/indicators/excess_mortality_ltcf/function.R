#excess_mortality_pscore *  5
library(dplyr)
library(p2synthr)
synth1(excess_mortality_pscore, group_names=c("mortality","other"), group_prob=c(0.5,0.5), setSeed = T, seedValue = 10)$mortality
