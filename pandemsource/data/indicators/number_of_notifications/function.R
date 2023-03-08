sapply(

  seq_along(confirmed_cases),

  function(i) if (i == 1){0} else{round(confirmed_cases[[i]] * (3 / log(i)))}

)
