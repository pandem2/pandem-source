#round(hospitalised_infected_patients * 3 / 21, 0)
library(dplyr)
library(p2synthr)
synth1(hospitalised_infected_patients,group_names =c("a","b","c","d","e","f"),group_prob=c(0.15,0.152,0.21,0.28,0.148,0.06), setSeed = T, seedValue = 10)$c
