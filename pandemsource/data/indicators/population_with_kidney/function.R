#round(population * 0.005, 0)
library(dplyr)
library(p2synthr)
synth1(population, group_names=c("diabetes","obesity","kidney","respiratory","underlying", "other"), group_prob=c(0.066,0.08,0.015,0.06,0.05,0.729), setSeed = T, seedValue = 10)$kindney
