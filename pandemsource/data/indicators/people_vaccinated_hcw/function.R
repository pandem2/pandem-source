library(dplyr)
library(p2synthr)
synth1(people_vaccinated, group_names=c('hcw','other'), group_prob=c(0.015,0.985), setSeed = T, seedValue = 10)$hcw
