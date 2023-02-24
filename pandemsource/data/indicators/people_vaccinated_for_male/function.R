#round(people_vaccinated * 0.5, 0)
library(dplyr)
library(p2synthr)
people_vaccinated_small <- ifelse(people_vaccinated < 1000, people_vaccinated, 0)
people_vaccinated_big <- ifelse(people_vaccinated < 1000, 0, people_vaccinated)

(
  synth1(people_vaccinated_small, group_names=c("male","female"), group_prob=c(0.481,0.519), setSeed = T, seedValue = 10)$male
  +
  synth1(round(people_vaccinated_big/100), group_names=c("male","female"), group_prob=c(0.481,0.519), setSeed = T, seedValue = 10)$male*100
)
