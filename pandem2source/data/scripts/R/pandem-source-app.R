if(!require("shiny")) {
  install.packages("shiny")
}

# 
ui <- function(source_list) {
  shiny::navbarPage("PANDEM II: Data Surveillance"
    , shiny::tabPanel("Integration progress", integration_page())
    , shiny::tabPanel("Data sources", data_sources_page(source_list))
    , shiny::tabPanel("Data dictionary", variables_page())
    , shiny::tabPanel("Query", query_page())
    , shiny::tabPanel("About", about_page())
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



about_page <- function() {
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


server <- function(input, output, session, ...) {
  `%>%` <- magrittr::`%>%`
  
  rv_sources <- shiny::reactive({
    jsonlite::fromJSON("http://localhost:8000/sources")
  })
  rv_source_details <- shiny::reactive({jsonlite::fromJSON(paste("http://localhost:8000/source_details?source=", URLencode(input$source_detail, reserved=TRUE), sep = ""))})
  
  output$sources_df <- DT::renderDataTable({
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

      DT::datatable(
        data, 
        escape = FALSE,
        options = list(
          searching = FALSE,
          pageLength = nrow(data)
          #dom = 't'
        ),
        rownames = FALSE,
        selection = 'none'
     ) %>%
      DT::formatStyle('Progress',
        background = DT::styleColorBar(c(0,1), 'lightblue'),
        backgroundSize = '98% 88%',
        backgroundRepeat = 'no-repeat',
        backgroundPosition = 'center') %>%
      DT::formatPercentage(c("Progress"), 2)
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
          pageLength = nrow(data)
          #dom = 't'
        ),
        rownames = FALSE,
        selection = 'none'
     ) 
    })

}
  
source_list <- jsonlite::fromJSON("http://localhost:8000/source_details")$sources
shiny::shinyApp(ui = ui(source_list), server = server, options = options(shiny.fullstacktrace = TRUE))

