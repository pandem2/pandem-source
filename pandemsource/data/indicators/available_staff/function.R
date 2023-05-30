#reporting_period <- strftime(as.Date(strptime("2020-01-31", "%Y-%m-%d")) + 1:365, "%Y-%m-%d")
#population_hcw <- sapply(reporting_period, function(v) 10000)
#icu_occupancy_ratio = round((1 + sin(-pi/2 + 2*pi * seq(reporting_period) / length(reporting_period)))/2, 3)

if(length(reporting_period) == 0) {
  NULL
} else {
  if(exists("population_hcw"))
    population = population_hcw
  
  if(exists("population_hcw_emergency_responders"))
    population = population_hcw_emergency_responders
  
  if(exists("population_hcw_nurses")){
    if(staff_type[[1]] == "hospital_staff_icu")
      population = population_hcw_nurses * 0.1
    else if(staff_type[[1]] == "hospital_staff_ward")
      population = population_hcw_nurses * 0.2

  }
  
  if(exists("population_phw")) {
    if(staff_type[[1]] == "phw")
      population = population_phw
    else if (staff_type[[1]] == "phw_surveillance") 
      population = population_phw * 0.5
  }
  
  start = reporting_period[[1]]
  staff0 = population[[1]] * 0.5
  stress = 0
  prevstaff = staff0
  if(length(reporting_period) == 1) {
    staff0
  } else {
    staff = rep(staff0, length(reporting_period))
    for(i in 2:length(reporting_period)) {
      prevdays = as.numeric(as.Date(reporting_period[[i]]) - as.Date(reporting_period[[i-1]]), unit = "days")
      weekstart = min(sapply(max(1, i - 7):(i-1), function(j) {
        diffdays = as.numeric(as.Date(reporting_period[[i]]) - as.Date(reporting_period[[j]]), unit = "days")
        if(diffdays < 7)
          j
        else
          i
      }))
      instress = (sum(sapply(weekstart:i, function(j) {
          if(icu_occupancy_ratio[[i]] > 0.4)
            1
          else
            0
        }))/length(weekstart:i)) >= 0.5
      if(instress)
        stress = stress + prevdays
      else
        stress = max(0, stress - prevdays)
      #print(paste(i,  stress))
      nextstaff =  (
        if(stress > 20 && prevstaff > staff0 * 0.7) {
          prevstaff -staff0 * 0.005 * prevdays
        } else if( stress > 20 && prevstaff <= staff0 * 0.7){
          prevstaff
        } else if( stress <= 20 && instress && prevstaff < staff0 * 1.5) {
          prevstaff + staff0 * 0.02 * prevdays
        } else if( stress <= 20 && instress && prevstaff >= staff0 * 1.5) {
          prevstaff 
        } else if( stress <= 20 && !instress && prevstaff < staff0) {
          min(prevstaff  + staff0 * 0.002 * prevdays, staff0)
        } else if( stress <= 20 && !instress && prevstaff > staff0) {
          max(prevstaff - staff0 * 0.005 * prevdays, staff0)
        } else {
          prevstaff 
        }
      )
      prevstaff = nextstaff
      staff[[i]] = nextstaff
    }
    round(staff)
  }
}

