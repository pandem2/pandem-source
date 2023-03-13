library(dplyr)
library(p2synthr)
prob = max(0.22 * ifelse(bed_type == "icu", 3, 2))
synth1(admissions, group_names=c("comorb","other"), group_prob=c(prob,1-prob), setSeed = T, seedValue = 10)$comorb
