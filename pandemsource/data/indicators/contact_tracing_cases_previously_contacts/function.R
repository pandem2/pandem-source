if(!exists("day_loop"))
  source(file.path(Sys.getenv('PANDEM_HOME'), "files", "scripts", "R", "contact-tracing-synthetic-loop.R"))

day_loop(reporting_period, 
  confirmed_cases, 
  population_contact_tracers,
  implemented_contact_tracing_comprehensive, 
  implemented_contact_tracing_limited, 
  implemented_contact_tracing_no
)$contact_tracing_cases_previously_contacts
