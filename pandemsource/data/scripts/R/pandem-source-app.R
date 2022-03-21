#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)


if(!require("shiny")) {
  stop("Shiny package is not availabla and it is needed to run the dashboard")
}

 
ui <- function(source_list, timeseries, logo_path) {
  shiny::navbarPage("PANDEM II: Data Surveillance"
    , shiny::tabPanel("Integration progress", integration_page())
    , shiny::tabPanel("Data sources", data_sources_page(source_list))
    , shiny::tabPanel("Data dictionary", variables_page())
    , shiny::tabPanel("Time series", query_page(timeseries))
    , shiny::tabPanel("About", about_page(logo_path))
  )
}


integration_page <- function() {
  shiny::fluidPage(
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
  shiny::fluidPage(
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
    shiny::fluidRow(shiny::column(3, shiny::h5("Data Quality")), shiny::column(9,shiny::htmlOutput("source_quality"))), 
    shiny::fluidRow(shiny::column(3, shiny::h5("Frequency")), shiny::column(9,shiny::htmlOutput("source_frequency"))), 

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
  shiny::fluidPage(
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



query_page <- function(timeseries) {
  cols = colnames(timeseries)
  if(length(cols) > 0) {
    namedorder = setNames(cols, sapply(1:length(cols), function(i) {
      if(cols[[i]] == "source__source_name") -8 
      else if(cols[[i]] == "indicator__family") -10 
      else if(cols[[i]] == "indicator") -7 
      else if(cols[[i]] == "geo_code") -4
      else if(cols[[i]] == "ref__geo_parent") -5 
      else if(cols[[i]] == "ref__geo_level") -6 
      else if(cols[[i]] == "source__reference_user") -2 
      else if(cols[[i]] == "pathogen_code") -1
      else i
    }))
    cols <- unname(namedorder[as.character(sort(as.integer(names(namedorder))))])
  }
  code_cols = cols[!grepl(".*_label$", cols) & !(cols %in% c("source", "source__table", "source__source_description", "indicator__description", "indicator__unit"))] 
  filters <- lapply(code_cols, function(col) {
      label <- sapply(strsplit(col, "__"), function(t) tail(t, 1))[[1]]
      label <- sapply(strsplit(label, "_"), function(t) paste(sapply(t, function(w) paste(toupper(substr(w, 1, 1)), substr(w, 2, nchar(w)), sep = "")), collapse = " "))[[1]]
      if(grepl('.* Code$', label))
        label = substr(label, 1, nchar(label)-5)
      control_id <- paste("ts_filter",col, sep = "_")

      shiny::conditionalPanel(
        condition=paste("Object.keys(document.getElementById('",control_id,"').selectize.options).length > 1 || Object.keys(document.getElementById('",control_id,"').selectize.options)[0] != 'NA' ", sep = ""),
        shiny::selectInput(control_id, label = label, multiple = TRUE, choices = unique(timeseries[col]), selected = NULL)
      )
    })

  shiny::fluidPage(
    shiny::fluidRow(
      shiny::column(2, 
        c(
          list(shiny::h4("Filters"))
          ,filters
          )
      ),
      shiny::column(6, 
        shiny::fluidRow(shiny::column(12, shiny::h4("Time Series"))),
        shiny::fluidRow(shiny::column(12, shiny::h5(shiny::htmlOutput("timeseries_count")))),
        shiny::fluidRow(
          shiny::column(6,shiny::radioButtons("timeseries_scale",label="Y-scale", choices = list(`Standard`="std", `Normalized`="normalized", `Log`="log"), selected = "std", inline = TRUE)),
          shiny::column(6,shiny::radioButtons("timeseries_period",label="Time", choices = list(`1 Year`="12", `6 months`="6", `3 months`="3", `1 month`="1", `All`="all"), selected = "12", inline = TRUE))
        ),
        shiny::fluidRow(
          shiny::column(2,shiny::actionButton("plot_series", "Visualize")),
          shiny::column(2,shiny::actionButton("load_series", "Synchronize")),
          shiny::column(2,shiny::actionButton("clear_filters", "Clear")),
          shiny::column(6)
        ),
        shiny::fluidRow(
          shiny::column(12,
            shiny::fluidRow(shiny::column(12, plotly::plotlyOutput("timeseries_chart")))
          )
        )
      ),
      shiny::column(4, 
        shiny::h5(shiny::htmlOutput("timeseries_title"))
      )
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
  
  rv_timeseries <- shiny::reactive({
    # Adding a dependency to the reload series button
    x = input$load_series
    #getting the data
    ts <- jsonlite::fromJSON(url("http://localhost:8000/timeseries"))
    df <- tibble::as_tibble(data.frame(ts$timeseries))
    df
  })
 
  # filters reactivity
  shiny::observe({
    df <- rv_timeseries()
    cols = colnames(df)
    filters = cols[!grepl(".*_label$", cols) & !(cols %in% c("source", "source_description", "description", "unit"))]
    #updating filters
    for(col in filters) {
      dff <- df
      this_select <- paste("ts_filter",col, sep = "_") 
      this_selection <- shiny::isolate(input[[this_select]])
      this_selection <- ifelse(this_selection == "NA", NA, this_selection)
      for(ocol in filters) {
        if(ocol != col) { 
          other_select = paste("ts_filter",ocol, sep = "_") 
          oselection = input[[other_select]]
          if(!is.null(oselection)) {
            oselection <- ifelse(oselection == "NA", NA, oselection)
            if(is.null(this_selection))
              dff <- dff[dff[[ocol]] %in% oselection,]
            else
              dff <- dff[dff[[ocol]] %in% oselection | dff[[col]] %in% this_selection,]
          }
        }
      }
      label_col <- paste(col, "label", sep = "_")
      values <- dff[[col]]
      if(label_col %in% colnames(dff)) {
        labels <- dff[[label_col]]
      }
      else
        labels <- values
      values <- setNames(values, labels)
      values <- values[sort(unique(names(values)), na.last = TRUE)]
      no_navalues <- values[!is.na(values)]
      no_navalues <- setNames(
        no_navalues,
        sapply(strsplit(names(no_navalues), "_|-| "), function(t) paste(sapply(t, function(w) paste(toupper(substr(w, 1, 1)), tolower(substr(w, 2, nchar(w))), sep = "")), collapse = " "))
      )
      if(NA %in% values) {
        # Setting NA as not available and put it at the end 
        values <- setNames(c(no_navalues, NA), c(names(no_navalues), "Not Available"))
      } else {
        values <- no_navalues 
      }
      # Updating the select with the respective values
      shiny::updateSelectInput(session, this_select,
        choices = values,
        selected = shiny::isolate(input[[this_select]])
      )
    }
  })

  # Count of filtered series 
  output$timeseries_count <- shiny::renderText({
    df <- rv_timeseries()
    cols = colnames(df)
    filters = cols[!grepl(".*_label$", cols) & !(cols %in% c("source", "source_description", "description", "unit"))]
    #updating series count
    for(col in filters) {
        select = paste("ts_filter",col, sep = "_") 
        selection = input[[select]]
        if(!is.null(selection)) {
          selection <- ifelse(selection == "NA", NA, selection)
          df <- df[df[[col]] %in% selection,]
        }
    }
    paste(nrow(df), "time series found")
  })
  
  shiny::observeEvent(input$clear_filters, {
    df <- shiny::isolate(rv_timeseries())
    cols = colnames(df)
    filters = cols[!grepl(".*_label$", cols) & !(cols %in% c("source", "source_description", "description", "unit"))]
    for(col in filters) { 
      select = paste("ts_filter",col, sep = "_")
      selection = shiny::isolate(input[[select]])
      if(!is.null(selection)) {
        shiny::updateSelectInput(session, select, choices = NULL, selected = list())
      }
    }
  })

  # Plot action
  shiny::observeEvent(input$plot_series, {
    df <- rv_timeseries()
    cols = colnames(df)
    filters = cols[!grepl(".*_label$", cols) & !(cols %in% c("source", "source_description", "description", "unit"))]
    # filtering series
    for(col in filters) {
        select = paste("ts_filter",col, sep = "_") 
        selection = input[[select]]
        if(!is.null(selection)) {
          selection <- ifelse(selection == "NA", NA, selection)
          df <- df[df[[col]] %in% selection,]
        }
    }
    # we have a dataframe with the sources to get
    # getting the series combination
    keys = cols[(!grepl(".*_label$", cols) & !grepl("^source__.*", cols) & !grepl("^indicator__.*", cols) & !grepl("^ref__.*", cols))]
    if(nrow(df) > 0) {
      rep = new.env()
      progress_start("Obtaining time series data", rep) 
      series_data = list()
      results <- lapply(1:nrow(df), function(i) {
         progress_set(value = 0.8*i/nrow(df), message = "Getting time series data", rep)
         post_data <- jsonlite::toJSON(as.list(sapply(keys, function(f) df[[f]][[i]])), auto_unbox = TRUE)
         resp <- httr::POST('http://localhost:8000/timeserie', body = post_data, encode="json")
         if(resp$status_code == 200) {
            data <- httr::content(resp)$timeserie
            json <- jsonlite::toJSON(data, auto_unbox=T, null = "null")
            res_df <- as.data.frame(jsonlite::fromJSON(json, simplifyDataFrame=T))
            res_df$source <- df[["source__source_name"]][[i]]
            res_df
         } else {
           stop("Error while obatining data series")
         }
      })
      progress_set(value = 0.9, message = "Joining results", rep)
      ts_df <- jsonlite::rbind_pages(results)
      progress_set(value = 0.9, message = "Plotting time series", rep)

      title <- ""#paste(sapply(strsplit(unique(ts_df$indicator), "_|-| "), function(t) paste(sapply(t, function(w) paste(toupper(substr(w, 1, 1)), tolower(substr(w, 2, nchar(w))), sep = "")), collapse = " ")), collase = ", ")
      singles = list()
      details = list()
      ts_df$legend <- sapply(ts_df$key, function(v) "")
      # creating labels and title
      for(col in c(keys)) {
        label_col <- paste(col, "label", sep = "_")
        values <- df[[col]]
        if(label_col %in% colnames(df)) {
          labels <- df[[label_col]]
        }
        else
          labels <- values
        values <- setNames(values, labels)
        values <- values[sort(unique(names(values)), na.last = TRUE)]
        labels <- setNames(
          sapply(strsplit(names(values), "_|-| "), function(t) paste(sapply(t, function(w) paste(toupper(substr(w, 1, 1)), tolower(substr(w, 2, nchar(w))), sep = "")), collapse = " ")),
          values
        )
        df_labels <- sapply(1:nrow(ts_df), function(i) {
          json <- ts_df$key[[i]]
          keys <-  jsonlite::fromJSON(json)
          if(col == "indicator")
            label = ts_df$indicator[[i]]
          else if(col %in% names(keys)) {
            label = labels[[as.character(keys[[col]])]]
          }
          else 
            label = NA
          label
        })
        if(col == "indicator")
          title <- paste(unique(labels), collapse = ", ")
        if(length(values)>0 && !all(is.na(values)))
          details[[col]] = sort(unique(labels))
        if(length(unique(labels))==1 && !is.na(values)) {
          #if(unique(labels) != "All" && col != "period_type")
          #  title = paste(title, ", ", unique(labels), sep = "") 
          singles[[col]] = unique(labels) 
        } else if(length(unique(labels))>1) {
          ts_df$legend = sapply(1:nrow(ts_df), function(i) {
            leg = ts_df[["legend"]][[i]]
            if(nchar(leg)>0)
              leg = paste(leg, ", ")
            leg = paste(leg, df_labels[[i]])
            leg
          })
        }
      }
      output$timeseries_title <- shiny::renderText({
        elements = c(
          list(
            Sources = unique(df[["source__source_name"]]),
            `Data Quality` = unique(df[["source__data_quality"]]),
            `Sources Description` = unique(df[["source__source_description"]]),
            `Reported By` = unique(df[["source__reference_user"]]),
            `Indicator` = unique(df[["indicator"]]),
            `Indicator Family` = unique(df[["indicator__family"]]),
            `Indicator Description` = unique(df[["indicator__description"]]),
            `Indicator Unit` = unique(df[["indicator__unit"]])
          ),
          details
        )
        paste(
          "<UL>",
          paste(
            "<LI><B>",
            names(elements),
            "</B>:",
            "<UL>",
            sapply(elements, function(e){
               paste("<LI>",e,"</LI>", collapse = "")
            }),
            "</UL></LI>",
            sep ="", 
            collapse = ""
          ),
          "</UL>",
          sep = ""
        )
      })
      
      output$timeseries_chart <- plotly::renderPlotly({
         # Validate if minimal requirements for rendering are met 
         progress_set(value = 0.15, message = "Generating line chart", rep)
         ts_df$date <- as.Date(ts_df$date)
         chart <- plot_timeseries(ts_df, title = title, scale = input$timeseries_scale, period = input$timeseries_period)
         
         # Setting size based on container size
         height <- session$clientData$output_line_chart_height
         width <- session$clientData$output_line_chart_width
	       
         # returning empty chart if no data is found on chart
         chart_not_empty(chart)
         

         # transforming chart on plotly
         gg <- plotly::ggplotly(chart, height = height, width = width, tooltip = c("label")) %>% plotly::config(displayModeBar = FALSE) 
      }) 
      progress_close(env = rep)
    }
  })

  # Listing source progress
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

  output$source_description <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$scope$source_description
  })

  output$source_frequency <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$scope$frequency
  })
  output$source_quality <- shiny::renderText({
      details <- rv_source_details()$definitions[[1]]
      details$scope$data_quality
  })

  output$source_tags <- shiny::renderText({
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

# Plot the time series chart for shiny app
plot_timeseries <- function(df, title, scale = "std", period = "12") {
  #Importing pipe operator
  `%>%` <- magrittr::`%>%`
  # getting regions and countries 
  #turning off the scientific pen
  old <- options()
  on.exit(options(old))
  options(scipen=999)

  # deleting old dates depending on period
  if(period != "all") {
    df <- df[as.numeric(difftime(max(df$date), df$date, units = "days")) < as.numeric(period)*30,]
  }
  # adding hover texts
  df$Details <- sapply(1:nrow(df), function(i) {
    paste(
      "",
      df$value[[i]],
      paste("date:",  df$date[[i]]),
      df$source[[i]],
      paste(strsplit(df$legend[[i]], ", ")[[1]], collapse = "\n"),
      sep = "\n"
    )

  })
  df$y <- if(scale == "std") {
    df$value
  } else if(scale == "log") {
    log(df$value)
  } else if(nrow(df)>0) {
    maxs = list()
    for(i in 1:nrow(df)) {
      key = paste(df$indicator[[i]], df$source[[i]], df$key[[i]])
      if(!exists(key, where = maxs))
        maxs[[key]] = max(0, df$value[[i]], na.rm = TRUE)
      else
        maxs[[key]] = max(maxs[[key]], df$value[[i]], na.rm = TRUE)
    }
    sapply(1:nrow(df), function(i) {
      key = paste(df$indicator[[i]], df$source[[i]], df$key[[i]])
      if(!is.na(maxs[[key]]) && maxs[[key]] != 0) 
        df$value[[i]]/maxs[[key]] 
      else 0
    })
  } else 
    numeric(0)
  # Calculating breaks of y axis
  y_breaks <- unique(floor(pretty(seq(0, (max(df$value, na.rm=T) + 1) * 1.1))))
  # plotting
  fig_line <- ggplot2::ggplot(df, ggplot2::aes(x = .data$date, y = .data$y, color = .data$legend, label = .data$Details)) +
    ggplot2::geom_line() + #ggplot2::aes(colour=.data$key)) +
    # Title
    ggplot2::labs(
     title= title,
     color="Legend"
    ) +
    ggplot2::xlab("Date") + 
    ggplot2::ylab(if(scale == "std") "Indicator" else if(scale == "log") "Log" else "[0-1] Max Normalized") +
    #ggplot2::scale_y_continuous(breaks = y_breaks, limits = c(0, max(y_breaks)), expand=c(0 ,0))+
    ggplot2::theme_classic(base_family = get_font_family()) +
    ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5, size = 12, face = "bold",lineheight = 0.9),
      axis.text = ggplot2::element_text(colour = "black", size = 8),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1, vjust = 0.5, 
                                          margin = ggplot2::margin(-15, 0, 0, 0)),
      axis.title.x = ggplot2::element_text(margin = ggplot2::margin(30, 0, 0, 0), size = 10),
      axis.title.y = ggplot2::element_text(margin = ggplot2::margin(-25, 0, 0, 0), size = 10),
      legend.position= if(length(unique(df$legend)) < 50) "bottom" else "none"
    )
  
  fig_line

}

# Helper function to get the font family
get_font_family <- function(){
  if(.Platform$OS.type == "windows" && is.null(grDevices::windowsFonts("Helvetica")[[1]]))
    grDevices::windowsFonts(Helvetica = grDevices::windowsFont("Helvetica"))
  "Helvetica"
}


# Functions for managing calculation progress
progress_start <- function(start_message = "processing", env = cd) {
  progress_close(env = env)
  env$progress <- shiny::Progress$new()
  env$progress$set(message = start_message, value = 0)
}
progress_close <- function(env = cd) {
  if(exists("progress", where = env)) {
    env$progress$close()
    rm("progress", envir = env)
  }
}

progress_set <- function(value, message  = "processing", env = cd) {
  if(!is.null(env$progress$set))
    env$progress$set(value = value, detail = message)
}
  
# validate that chart is not empty
chart_not_empty <- function(chart) {
  shiny::validate(
     shiny::need(!("waiver" %in% class(chart$data)), chart$labels$title)
  )
} 

# Shiny app initialization
source_list <- NULL
while(is.null(source_list)) {
  source_list = tryCatch(jsonlite::fromJSON("http://localhost:8000/source_details"), error = function(e) NULL)
  if(is.null(source_list)) {
    message("going to sleep for 2 seconds until pandem2source API is up")
    Sys.sleep(2)
  }
}

ts <- jsonlite::fromJSON(url("http://localhost:8000/timeseries"))
ts_df <- tibble::as_tibble(data.frame(ts$timeseries))

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
shiny::shinyApp(ui = ui(source_list = source_list, timeseries = ts_df, logo_path = logo_path), server = server, options = options )

