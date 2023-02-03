#round(confirmed_cases * 0.5 * 1, 0)
library(dplyr)
library(p2synthr)
synth1(confirmed_cases, group_names=c("male","female"), group_prob=c(0.481,0.519), setSeed = T, seedValue = 10)$male
