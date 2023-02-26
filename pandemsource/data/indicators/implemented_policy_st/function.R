icu_occupancy_ratio <- c(0, 0, 0, 0.01, 0.03, 0.07, 0.1, 0.3, 0.8, 0.9, 1.2, 0.7, 0.3, 0.2, 0, 0)
peak_index <- match(max(icu_occupancy_ratio), icu_occupancy_ratio)
cut <- peak_index + 2
sapply(1:length(icu_occupancy_ratio), function(i) {if((i < cut && icu_occupancy_ratio[[i]] > 0.04) || (i > cut && icu_occupancy_ratio[[i]] < 0.04)) 1 else 0})
