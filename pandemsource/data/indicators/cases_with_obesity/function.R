#round(confirmed_cases * 0.02 * 4, 0)
library(dplyr)
library(p2synthr)
synth1(confirmed_cases, group_names=c("diabetes","obesity","kidney","respiratory","other"), group_prob=c(0.066,0.08,0.015,0.06,0.779), setSeed = T, seedValue = 10)$obesity
