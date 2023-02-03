#round(people_vaccinated * 0.5, 0)
library(dplyr)
library(p2synthr)
synth1(people_vaccinated, group_names=c("male","female"), group_prob=c(0.481,0.519), setSeed = T, seedValue = 10)$male
