
library(XML)
library(httr)
library(dplyr)
library(jsonlite)
library(Pandem2simulator)
library(ggplot2)

eu_countries <- c(
    "germany" = "DE",
    "austria" = "AT",
    "belgium" = "BE",
    "bulgaria" = "BG",
    "cyprus" = "CY",
    "croatia" = "HR",
    "denmark" = "DK",
    "spain" = "SP",
    "estonia" = "EE",
    "finland" = "FI",
    "france" = "FR",
    "greece" = "GR",
    "hungary" = "HU",
    "ireland" = "IE",
    "italy" = "IT",
    "latvia" = "LV",
    "lithuania" = "LT",
    "luxembourg" = "LU",
    "malta" = "MT",
    "netherlands" = "NL",
    "poland" = "PL",
    "portugal" = "PT",
    "romania" = "RO",
    "slovakia" = "SK",
    "slovenia" = "SI",
    "sweden" = "SE",
    "czechia" = "CZ"
)

get_df_hospital_occupancy <- function() {
    json_payload <- '{
        "select": ["hospitalised_infected_patients_in_icu"],
        "filter": {
            "source": "ecdc-covid19-weekly-hospital-occupancy"
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
        message(req$status_code)
        stop("Error while obtaining data series")
    }
}

#' Normalizes dataframe columns for UCL-algorithm
normalize_dataframe <- function(df) {
    if ("reporting_period" %in% colnames(df)) {
        names(df)[names(df) == "reporting_period"] <- "time"
        fun1 <- function(time) strftime(time, format = "%Y-%V")
        df$year_week <- mapply(fun1, df$time)
    }
    if ("geo_code" %in% colnames(df)) {
        names(df)[names(df) == "geo_code"] <- "country_code"
    }
    if ("country_name" %in% colnames(df)) {
        names(df)[names(df) == "country_name"] <- "country_code"
        fun2 <- function(country_name) eu_countries(country_name)
        df$country_code <- mapply(fun2, df$country_code)
    }
    if ("variant" %in% colnames(df)) {
        names(df)[names(df) == "confirmed_cases"] <- "new_cases"
        df$number_sequenced <- NA
    }
    if ("hospitalised_infected_patients_in_icu" %in% colnames(df)) {
        names(df)[names(df) == "hospitalised_infected_patients_in_icu"] <- "new_cases"
    }
    return(df)
}

#' Keeps only useful columns for UCL-algorithm and eliminates rows
#' that are not in the given interval
format_dataframe <- function(df, start_date = "", end_date = "") {
    keeps <- c("country_code", "year_week", "time", "new_cases")
    if ("variant" %in% colnames(df)) {
        keeps <- append(keeps, "variant")
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

remove_exclusive_dates <- function(df1, df2) {
    # Mark as YES if couples date/country aren't in both dataframes else NO
    df1$check <- ifelse(is.na(match(
        paste0(df1$time, df1$country_code),
        paste0(df2$time, df2$country_code)
        )), "NO", "YES")
    df2$check <- ifelse(is.na(match(
        paste0(df2$time, df2$country_code),
        paste0(df1$time, df1$country_code)
        )), "NO", "YES")

    # Only keep row where check equals YES
    df1_without_exclusive <- df1[(df1$check == "YES"), ]
    df2_without_exclusive <- df2[(df2$check == "YES"), ]
    return(list(df1_without_exclusive, df2_without_exclusive))
}

write_country_csv <- function(df) {
    countries <- unique(df$country_code)
    for (country in countries) {
        df2write <- subset(
            df, df$country_code == country, c(
                "country_code", "year_week", "time", "variant", "new_cases"))
        write.csv(df2write,
            file = paste(country, "_hosp_occupancy_variants.csv", sep = "")
        )
    }
}

message("Getting hospital_occupancy dataframe... (1/7)")
hosp_occupancy_df <- get_df_hospital_occupancy()
message("Getting variants dataframe... (2/7)")
variants_df <- get_df_variants()

message("Normalizing dataframes... (3/7)")
hosp_occupancy_df_normalized <- normalize_dataframe(hosp_occupancy_df)
variants_df_normalized <- normalize_dataframe(variants_df)

message("Formatting dataframes... (4/7)")
filter_start_date <- min(variants_df_normalized$time, na.rm = TRUE)
filter_end_date <- max(variants_df_normalized$time, na.rm = TRUE)
hosp_occupancy_df_formatted <- format_dataframe(
    hosp_occupancy_df_normalized,
    start_date = filter_start_date, end_date = filter_end_date
)
variants_df_formatted <- format_dataframe(
    variants_df_normalized,
    start_date = filter_start_date, end_date = filter_end_date
)

message("Removing exclusive dates in both dataset... (5/7)")
dataframes <- remove_exclusive_dates(
    variants_df_formatted, hosp_occupancy_df_formatted)
variants_df_formatted <- dataframes[[1]]
hosp_occupancy_df_formatted <- dataframes[[2]]

message("Obtaining simulated case & hosp occupancy dataframe... (6/7)")
case_hosp_occupancy_aggregated <- simulator(
    trainset = variants_df_formatted,
    testset = hosp_occupancy_df_formatted, 
    var_names_geolocalisation = country_code,
    var_names_outcome = variant,
    var_names_count = new_cases,
    var_names_time = time,
    factor = 500
)
str(case_hosp_occupancy_aggregated)

message("Writing resulting datasets... (7/7)")
write_country_csv(case_hosp_occupancy_aggregated)
