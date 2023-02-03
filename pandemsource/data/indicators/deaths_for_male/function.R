#round(deaths_infected * 0.5 * 1, 0)
library(dplyr)
library(p2synthr)
synth1(deaths_infected, group_names=c("male","female"), group_prob=c(0.5,0.5), setSeed = T, seedValue = 10)$male
