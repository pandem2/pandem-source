#round(hospitalised_infected_patients_in_icu * 4 / 21, 0)
library(dplyr)
library(p2synthr)
# a 0-14, b 15-24, c 25-49, d 50-64, e 65-79, f 80+
synth1(hospitalised_infected_patients_in_icu,group_names =c("a","b","c","d","e","f"),group_prob=c(0.15,0.152,0.21,0.28,0.148,0.06), setSeed = T, seedValue = 10)$d
