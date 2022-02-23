#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)


if(!require("shiny")) {
  stop("Shiny package is not availabla and it is needed to run the dashboard")
}

 
ui <- function(source_list, logo_path) {
  shiny::navbarPage("PANDEM II: Data Surveillance"
    , shiny::tabPanel("Integration progress", integration_page())
    , shiny::tabPanel("Data sources", data_sources_page(source_list))
    , shiny::tabPanel("Data dictionary", variables_page())
    , shiny::tabPanel("Query", query_page())
    , shiny::tabPanel("About", about_page(logo_path))
  )
}


integration_page <- function() {
  fluidPage(
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("Data Surveillance") 
      )
    ),
    shiny::fluidRow(
      shiny::column(1),
      shiny::column(10, 
        DT::dataTableOutput("sources_df")
      ),
      shiny::column(1)
    ), 
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("Jobs") 
      )
    ),
    shiny::fluidRow(
      shiny::column(1),
      shiny::column(10, 
        DT::dataTableOutput("jobs_df")
      ),
      shiny::column(1)
    ), 
    shiny::fluidRow(
      shiny::column(2, 
        shiny::actionButton("job_delete", "Delete selected Job"),
      ),
      shiny::column(10) 
    ),
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("Integration Issues") 
      )
    ),
    shiny::fluidRow(
      shiny::column(1),
      shiny::column(10, 
        DT::dataTableOutput("issues_df")
      ),
      shiny::column(1)
    )
  )
}

data_sources_page <- function(sources) {
  fluidPage(
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("Data Sources") 
      )
    ),
    shiny::fluidRow(
      shiny::column(1),
      shiny::column(10, 
        shiny::selectInput("source_detail", label = "Source", multiple = FALSE, choices = sources, selected = sources[[1]])
      ),
      shiny::column(1, 
      ),
    ), 
    shiny::fluidRow(shiny::column(12, shiny::h3("Source definition"))),
    shiny::fluidRow(shiny::column(3, shiny::h5("Source")), shiny::column(9,shiny::htmlOutput("source_name"))) ,
    shiny::fluidRow(shiny::column(3, shiny::h5("Description")), shiny::column(9,shiny::htmlOutput("source_description"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Tags")), shiny::column(9,shiny::htmlOutput("source_tags"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Reference User")), shiny::column(9,shiny::htmlOutput("source_user"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Reference Email")), shiny::column(9,shiny::htmlOutput("source_email"))), 

    shiny::fluidRow(shiny::column(12, shiny::h3("Acquisition"))),
    shiny::fluidRow(shiny::column(3, shiny::h5("Channel")), shiny::column(9,shiny::htmlOutput("source_channel"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Channel info")), shiny::column(9,shiny::htmlOutput("source_channel_info"))), 
   
    shiny::fluidRow(shiny::column(3, shiny::h5("Format")), shiny::column(9,shiny::htmlOutput("source_format"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Format info")), shiny::column(9,shiny::htmlOutput("source_format_info"))), 

    shiny::fluidRow(shiny::column(12, shiny::h5("Data Scope"))),
    shiny::fluidRow(shiny::column(3, shiny::h5("Globals")), shiny::column(9,shiny::htmlOutput("source_globals"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Replace data in")), shiny::column(9,shiny::htmlOutput("source_replace"))), 
    
    shiny::fluidRow(shiny::column(12, shiny::h5("Mapping"))),
    shiny::fluidRow(shiny::column(12,
        DT::dataTableOutput("columns_df")
    ))
  )
}

variables_page <- function() {
  fluidPage(
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("Variables") 
      )
    ),
    shiny::fluidRow(
      shiny::column(1),
      shiny::column(10, 
        DT::dataTableOutput("variables_df")
      ),
      shiny::column(1)
    ) 
  )
}



query_page <- function() {
  fluidPage(
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("Query") 
      )
    ),
    shiny::fluidRow(
      shiny::column(1),
      shiny::column(10, 
      ),
      shiny::column(1)
    ) 
  )
}



about_page <- function(logo_path) {
  b64 <- base64enc::dataURI(file=logo_path, mime="image/jpeg")
  fluidPage(
    shiny::fluidRow(
      shiny::column(12, 
        shiny::h4("by the pandem consortium") 
      )
    ),
    shiny::fluidRow(
      shiny::column(12, 
        shiny::img(src=b64, height="80%", width="80%", align="center")
      )
    ) 
  )
}


server <- function(input, output, session, ...) {
  `%>%` <- magrittr::`%>%`
 
  rv_reload_sources <- shiny::reactiveVal(0)
  rv_sources <- shiny::reactive({
    shiny::invalidateLater(10000)
    jsonlite::fromJSON("http://localhost:8000/sources")
  })

  rv_source_details <- shiny::reactive({
    jsonlite::fromJSON(paste("http://localhost:8000/source_details?source=", 
      URLencode(
        input$source_detail,
        reserved=TRUE
      ), sep = ""
    ))
  })

  
  rv_issues <- shiny::reactive({
    job_row <- input$jobs_df_rows_selected
    shiny::validate(
      shiny::need(length(job_row) > 0, 'Please select a job to see its issues')
    )
    job_id = rv_jobs()$jobs$id[[job_row]]
    jsonlite::fromJSON(paste("http://localhost:8000/issues?job_id=", job_id ,sep = ""))
  })

  rv_variable_list <- shiny::reactive({
    jsonlite::fromJSON("http://localhost:8000/variable_list")
  })

  rv_jobs <- shiny::reactive({
    source_row <- input$sources_df_rows_selected
    shiny::validate(
      shiny::need(length(source_row) > 0, 'Please select a source to see its jobs')
    )
    source_name = rv_sources()$sources$name[[source_row]]
    jsonlite::fromJSON(paste("http://localhost:8000/jobs?source=",URLencode(source_name, reserved=TRUE), sep = ""))
  })
  
  output$sources_df <- DT::renderDataTable({
      observed <- rv_reload_sources()
      sources = shiny::isolate(rv_sources())
      data <- tibble::as_tibble(data.frame(
        Name = sources$sources$name, 
        Progress = sources$sources$progress,
        Step = sources$sources$`step`, 
        Status=sources$sources$`status`,
        `Last Import Started` = substr(sources$sources$`last_import_start`, 1, 19), 
        `Last Import Ended` = substr(sources$sources$`last_import_end`, 1, 19), 
        `Next Check` = substr(sources$sources$`next_check`, 1, 19), 
        `Files` = sources$sources$`files`,  
        `Mb` = round(sources$sources$`size`/1024, 1),  
        `Issues` = sources$sources$`issues`,  
         check.names = FALSE
      )) %>% dplyr::filter(!is.na(.data$Step)) 

      DT::datatable(
        data, 
        escape = FALSE,
        options = list(
          searching = FALSE,
          pageLength = 50 
          #dom = 't'
        ),
        rownames = TRUE,
        selection = 'single'
     ) %>%
      DT::formatStyle('Progress',
        background = DT::styleColorBar(c(0,1), 'lightblue'),
        backgroundSize = '98% 88%',
        backgroundRepeat = 'no-repeat',
        backgroundPosition = 'left') %>%
      DT::formatPercentage(c("Progress"), 2)
    })

    shiny::observe({
      sources = rv_sources()
      data <- tibble::as_tibble(data.frame(
        Name = sources$sources$name, 
        Progress = sources$sources$progress,
        Step = sources$sources$`step`, 
        Status=sources$sources$`status`,
        `Last Import Started` = substr(sources$sources$`last_import_start`, 1, 19), 
        `Last Import Ended` = substr(sources$sources$`last_import_end`, 1, 19), 
        `Next Check` = substr(sources$sources$`next_check`, 1, 19), 
        `Files` = sources$sources$`files`,  
        `Mb` = round(sources$sources$`size`/1024, 1),  
        `Issues` = sources$sources$`issues`,  
         check.names = FALSE
      )) %>% dplyr::filter(!is.na(.data$Step)) 
      
      proxy <- DT::dataTableProxy('sources_df')
      DT::replaceData(proxy, data, clearSelection = "none")
    }) 
    
    output$source_name <- shiny::renderText({
      names(rv_source_details()$definitions)[[1]]
    })

    source_description <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$scope$source_description
    })

    source_tags <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      paste(details$scope$tags, collapse = ", ")
    })

    output$source_user  <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$scope$reference_user
    })

    output$source_email  <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$scope$reporting_email
    })

    output$source_channel <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$acquisition$channel$name
    })

    output$source_channel_info <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      items <- names(details$acquisition$channel)
      items <- items[items != "name"]
      values <- details$acquisition$channel[items]
      paste("<UL><LI>", paste(items, values, collapse = "</LI><LI>", sep = ": "), "</LI></UL>", sep = "")
    })
   
    output$source_format <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$acquisition$format$name
    })

    output$source_format_info <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      items <- names(details$acquisition$format)
      items <- items[items != "name"]
      values <- details$acquisition$format[items]
      paste("<UL><LI>", paste(items, values, collapse = "</LI><LI>", sep = ": "), "</LI></UL>", sep = "")
    })

    output$source_globals <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      items <- details$scope$globals$variable
      values <- details$scope$globals$value
      paste("<UL><LI>", paste(items, values, collapse = "</LI><LI>", sep = ": "), "</LI></UL>", sep = "")
    })

    output$source_replace <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      items <- details$scope$update_scope$variable
      values <- details$scope$upsate_scope$value
      paste("<UL><LI>", paste(items, values, collapse = "</LI><LI>", sep = ": "), "</LI></UL>", sep = "")
    })
    
    output$columns_df <- DT::renderDataTable({
      details <- rv_source_details()$definitions[[1]]
      items <- details$scope$update_scope$variable
      data <- tibble::as_tibble(data.frame(
        Column = details$columns$name, 
        Variable = details$columns$variable,
        check.names = FALSE
      ))  

      DT::datatable(
        data, 
        escape = FALSE,
        options = list(
          searching = FALSE,
          pageLength = 200
          #dom = 't'
        ),
        rownames = FALSE,
        selection = 'none'
     ) 
    })
   
    shiny::observe({
      observed <- input$sources_df_rows_selected
      output$jobs_df <- DT::renderDataTable({
        jobs = shiny::isolate(rv_jobs())$jobs
        data <- tibble::as_tibble(data.frame(
          Id = jobs$id, 
          Source = jobs$source, 
          Progress = jobs$progress, 
          Step = jobs$step, 
          Status = jobs$status,
          Started = substr(jobs$start, 1, 19), 
          Ended = substr(jobs$end, 1, 19), 
          Files = jobs$files,  
          Mb = round(jobs$size/1024, 1),  
          Issues = jobs$issues,  
          check.names = FALSE
        )) 

        DT::datatable(
          data, 
          escape = FALSE,
          options = list(
            searching = FALSE,
            pageLength = 50 
            #dom = 't'
          ),
          rownames = TRUE,
          selection = 'single'
       ) %>%
        DT::formatStyle('Progress',
          background = DT::styleColorBar(c(0,1), 'lightblue'),
          backgroundSize = '98% 88%',
          backgroundRepeat = 'no-repeat',
          backgroundPosition = 'left') %>%
        DT::formatPercentage(c("Progress"), 2)
      })
    })

    shiny::observe({
      observed <- input$jobs_df_rows_selected
      output$issues_df <- DT::renderDataTable({
        issues <- shiny::isolate(rv_issues())$issues
        data <- tibble::as_tibble(data.frame(
             Id = issues$id,
             `Job Id` = issues$job_id,
             Source = issues$tag,
             Table = issues$source,
             `Severity` = issues$issue_severity,
             `Issue Type` = issues$issue_type,
             Step = issues$step,
             `Line Number` = issues$line,
             File = issues$file,
             Message = issues$message,
             `Raised On` = substr(issues$raised_on, 1, 19),
             check.names = FALSE
        ))  

        DT::datatable(
          data, 
          escape = FALSE,
          options = list(
            searching = TRUE,
            pageLength = 200
            #dom = 't'
          ),
          rownames = FALSE,
          selection = 'none'
       ) 
      })
    })

    output$variables_df <- DT::renderDataTable({
      variables <- rv_variable_list()$variables
      data <- tibble::as_tibble(data.frame(
        `Family` = variables$data_family,
        `Type` = variables$type,
        `Variable` = gsub("_", " ", variables$variable),
        `Base Variable` = gsub("_", " ", variables$base_variable),
        `Definition` = variables$description,
         Unit = variables$unit,
        `Partition` = 
           paste("<UL>", sapply(variables$partition, function(r) paste(sapply(r, function(i) paste("<LI>", i, "</LI>", collapse = "", sep = "")), collapse = "")), "</UL>", sep = ""),
        `Modifiers` = 
           sapply(variables$modifiers, function(df) if(nrow(df) == 0) "" else paste("<UL><LI>", paste(df$variable, ": ", df$value, sep = "", collapse = "</LI><LI>"), sep = "", "</LI></UL>")),
        `Links` = sapply(variables$linked_attributes, function(v) paste(v, collapse = ', ')),
        `Formula` = variables$formula,
        check.names = FALSE
      ))  

      DT::datatable(
        data, 
        escape = FALSE,
        options = list(
          searching = TRUE,
          pageLength = 200
          #dom = 't'
        ),
        rownames = FALSE,
        selection = 'none'
     ) 
    })

    shiny::observeEvent(input$job_delete, {
      shiny::showModal(shiny::modalDialog(
        title = "Confirm deletion",
        "Are you sure you want to permanently delete the job",
        footer = shiny::tagList(shiny::actionButton("job_do_delete", "Yes"), shiny::modalButton("Cancel"))
      ))
    })
    
    shiny::observeEvent(input$job_do_delete, {
      job_row <- input$jobs_df_rows_selected
      job_id = rv_jobs()$jobs$id[[job_row]]
      httr::DELETE(paste('http://localhost:8000/jobs?job_id=', job_id, sep = ""))
      #rv_reload_sources(shiny::isolate(rv_reload_sources()) + 1)
      source_row <- input$sources_df_rows_selected
      proxy <- DT::dataTableProxy('sources_df')
      DT::selectRows(proxy, NULL)
      DT::selectRows(proxy, source_row)
      shiny::removeModal()
    })



}
  
source_list <- NULL
while(is.null(source_list)) {
  source_list = tryCatch(jsonlite::fromJSON("http://localhost:8000/source_details"), error = function(e) NULL)
  if(is.null(source_list)) {
    message("going to sleep for 2 seconds until pandem2source API is up")
    Sys.sleep(2)
  }
}

# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  options = options(shiny.fullstacktrace = TRUE)
}
if (length(args)>0) {
  options = options(shiny.fullstacktrace = TRUE, shiny.port = as.integer(args[1]))
}
if (length(args)>1) {
  logo_path = args[2]
} else {
  logo_path = ""
}
shiny::shinyApp(ui = ui(source_list, logo_path), server = server, options = options )

