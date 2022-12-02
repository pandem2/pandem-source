library(Pandem2simulator)
library(ggplot2)
library(dplyr)
library(tidyr)

library(XML)
library(httr)
library(jsonlite)



get_df_age_group <- function() {
    json_payload <- '{
        "select": ["confirmed_cases", "age_group", "geo_code", "reporting_week"],
        "filter": {
            "source": "ecdc-covid19-age-group"
        }
    }'

    return(request_dataset(json_payload))
}

get_df_variants <- function() {
    json_payload <- '{
        "select": ["confirmed_cases", "variant", "geo_code", "reporting_week"],
        "filter": {
            "source": "ecdc-covid19-variants"
        }
    }'

    return(request_dataset(json_payload))
}

request_dataset <- function(json_payload) {
    req <- httr::POST(
        "http://127.0.0.1:8000/dataset",
        body = json_payload, encode = "json"
    )
    if (req$status_code == 200) {
        data <- httr::content(req)$dataset
        json <- jsonlite::toJSON(data, auto_unbox = T, null = "null")
        res_df <- as.data.frame(jsonlite::fromJSON(json, simplifyDataFrame = T))
        return(res_df)
    } else {
        stop("Error while obtaining data series")
    }
}

check_dataframe <- function(df) {
    if (dim(df)[1] == 0 || dim(df)[2] == 0) {
        stop("Dataframe is empty. Please check that this source has been completely acquired before retrying.")
    }
}

#' Normalizes dataframe columns for UCL-algorithm
normalize_dataframe <- function(df) {
    if ("reporting_period" %in% colnames(df)) {
        names(df)[names(df) == "reporting_period"] <- "time"
        fun1 <- function(time) strftime(time, format = "%G-%V")
        df$year_week <- mapply(fun1, df$time)
    }
    if ("geo_code" %in% colnames(df)) {
        names(df)[names(df) == "geo_code"] <- "country_code"
    }
    if ("variant" %in% colnames(df)) {
        names(df)[names(df) == "confirmed_cases"] <- "new_cases"
        df$number_sequenced <- NA
    }
    return(df)
}


#' Keeps only useful columns for UCL-algorithm and eliminates rows
#' that are not in the given interval
format_dataframe <- function(df, start_date = "", end_date = "") {
    keeps <- c("country_code", "year_week", "time", "new_cases")
    if ("variant" %in% colnames(df)) {
        keeps <- append(keeps, "variant")
    } else {
        keeps <- append(keeps, "age_group")
    }
    df <- df[keeps]
    if (start_date != "" & end_date != "") {
        df <- df[is_in_date_range(df$time, start_date, end_date), ]
    }
    return(df)
}

is_in_date_range <- function(date2check_str, start_date_str, end_date_str) {
    date2check <- as.Date(date2check_str, "%Y-%m-%d")
    start <- as.Date(start_date_str, "%Y-%m-%d")
    end <- as.Date(end_date_str, "%Y-%m-%d")
    return(start <= date2check & date2check <= end)
}


normalize_mutation_dates <- function(df) {
  df = data_mutation[nchar(df$collection_date) == 10,]
  df$time <- df$collection_date
  df$year_week <- strftime(df$collection_date, "%G-%V")
  df
}

#normalize_dates <- function(data_mutation) {
#	data_mutation$collection_date <- mapply(normalize_date, data_mutation$collection_date)
#	names(data_mutation)[names(data_mutation) == "collection_date"] <- "time"
#	return(data_mutation)
#}

#normalize_date <- function(date) {
#    if (nchar(date) > 4) {
#        res <- date
#    } else {
#        res <- paste(date, "-01-01", sep = "")
#    }
#    return(as.Date(res, format = "%Y-%m-%d", origin = "1970-01-01"))
#}

write_country_csv <- function(df) {
    countries <- unique(df$country_code)
    for (country in countries) {
        df2write <- subset(
            df, df$country_code == country, c(
                "country_code",
                "year_week",
                "time",
                "variant",
                "position",
                "mutation",
                "new_cases"
            )
        )
        write.csv(df2write,
            file = paste(country, "_case_variants.csv", sep = "")
        )
    }
}

message("Getting age_group & variants dataframe... (1/6)")
variants_df <- get_df_variants()


message("Checking integrity of variants... (2/6)")
check_dataframe(variants_df)

message("Normalizing dataframes... (3/6)")
variants_df <- normalize_dataframe(variants_df)


message("Formatting dataframes... (4/6)")
filter_start_date <- min(variants_df$time, na.rm = TRUE)
filter_end_date <- max(variants_df$time, na.rm = TRUE)

variants_df_formatted <- format_dataframe(
    variants_df,
    start_date = filter_start_date, end_date = filter_end_date
)


# Filtering by country
message("Filtering by country...")
variants_df_formatted_country <- filter(variants_df_formatted, country_code %in% c("FR", "AT", "BE", "IT"))

message("Downloading Genomic Datasets from NCBI... (5/6)")
download_genomic_data(
    ecdcdata = variants_df_formatted_country,
    var_names_variant = "variant",
    path_dataformat = "/usr/bin/dataformat",
    path_dataset = "/usr/bin/datasets",
    path_nextclade = "/usr/bin/nextclade"
)
genomic_data <- read.csv("genomic_data.csv")
file.remove("genomic_data.csv")

message("Adding genomic data on the cases dataset... (6/7)")
genomic_data$time <- genomic_data$collection_date
data_mutation <- create_table_mutation(genomic_data)
data_mutation <- normalize_mutation_dates(data_mutation)

case_variants_genomic <- add_genomic_data(
   metadata = variants_df_formatted_country, 
   genomic_data = data_mutation, 
   var_names_merge=variant, 
   var_names_count=new_cases, 
   var_names_time=time
)

message("Selecting time correlated mutations on last 30 days... (7/7)")
case_variants_genomic_selected <- select_mutation(
  data = case_variants_genomic, 
  dateStart= as.Date(filter_end_date, format = "%Y-%m-%d") - 30, 
  dateEnd = as.Date(filter_end_date, format = "%Y-%m-%d"), 
  number=5,
  var_names_count=new_cases, 
  var_names_time=time
)

write_country_csv(case_variants_genomic_selected)

#cleaning data
file.remove(list.files("data/sars-cov-2", full.names = TRUE))
file.remove("data/sars-cov-2")
file.remove("data")
file.remove(list.files("Nextclade_results", full.names = TRUE))
file.remove("Nextclade_results")
file.remove("genomic_filter.fna")

