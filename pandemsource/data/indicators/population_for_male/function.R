#round(population * 0.5, 0)
library(dplyr)
library(p2synthr)
synth1(population, group_names=c("male","female"), group_prob=c(0.49,0.51), setSeed = T, seedValue = 10)$male
