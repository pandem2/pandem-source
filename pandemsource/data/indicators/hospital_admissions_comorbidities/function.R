library(dplyr)
library(p2synthr)
prob = 0.22 * 3
synth1(hospital_admissions, group_names=c("comorb","other"), group_prob=c(prob,1-prob), setSeed = T, seedValue = 10)$comorb
