sapply(
    seq_along(confirmed_cases),
    function(i) confirmed_cases[[i]] * (3 / log(i))
)