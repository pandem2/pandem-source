#people_vaccinated_hcw * 0.3
library(dplyr)
library(p2synthr)
synth1(people_vaccinated_hcw, group_names=c("admin","doctors","responders","nurses"), group_prob=c(0.3,0.4,0.1,0.2), setSeed = T, seedValue = 10)$admin
