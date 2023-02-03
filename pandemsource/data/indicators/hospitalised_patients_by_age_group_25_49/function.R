#round(hospitalised_infected_patients * 3 / 21, 0)
library(dplyr)
library(p2synthr)
synth1(deaths_infected,group_names =c("0_14","15_24","25_49","50_64","65-79","80+"),group_prob=c(0.15,0.152,0.21,0.28,0.148,0.06), setSeed = T, seedValue = 10)$25_49
