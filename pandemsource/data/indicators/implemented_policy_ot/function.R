peak_index <- match(max(icu_occupancy_ratio), icu_occupancy_ratio)
cut <- peak_index + 2
sapply(1:length(icu_occupancy_ratio), function(i) {if(i > cut && icu_occupancy_ratio[[i]] <= 0.04) 1 else 0})
