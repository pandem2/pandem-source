local({r <- getOption("repos")
       r["CRAN"] <- "http://cran.r-project.org" 
       options(repos=r)
})

if (!require("BiocManager", quietly = TRUE))
    install.packages("BiocManager")
BiocManager::install("Biostrings")

list.of.packages <- c("reticulate", "seqinr", "Pandem2simulator", "readr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

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
        "select": ["number_detections_variant", "variant", "geo_code", "reporting_week"],
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
        fun1 <- function(time) strftime(time, format = "%Y-%V")
        df$year_week <- mapply(fun1, df$time)
    }
    if ("geo_code" %in% colnames(df)) {
        names(df)[names(df) == "geo_code"] <- "country_code"
    }
    if ("number_detections_variant" %in% colnames(df)) {
        names(df)[names(df) == "number_detections_variant"] <- "new_cases"
        df$number_sequenced <- NA
    }
    if ("age_group" %in% colnames(df)) {
        df$age_group <- mapply(normalize_age_group, df$age_group)
        names(df)[names(df) == "confirmed_cases"] <- "new_cases"
    }
    return(df)
}

normalize_age_group <- function(age_group) {
    nb <- regmatches(age_group, gregexpr("[0-9]+", age_group, perl = TRUE))
    nb_size <- length(nb[[1]])
    if (nb_size == 1) {
        if (strtoi(nb[[1]], base = 10) <= 40) {
            return(paste("<", nb[[1]], "yr", sep = ""))
        } else {
            return(paste(nb[[1]], "+yr", sep = ""))
        }
    } else if (nb_size == 2) {
        return(paste(nb[[1]][1], "-", nb[[1]][2], "yr", sep = ""))
    } else {
        return(NA)
    }
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

normalize_dates <- function(data_mutation) {
	data_mutation$collection_date <- mapply(normalize_date, data_mutation$collection_date)
	names(data_mutation)[names(data_mutation) == "collection_date"] <- "time"
	return(data_mutation)
}

normalize_date <- function(date) {
    if (nchar(date) > 4) {
        res <- date
    } else {
        res <- paste(date, "-01-01", sep = "")
    }
    return(as.Date(res, format = "%Y-%m-%d"))
}

write_country_csv <- function(df) {
    countries <- unique(df$country_code)
    for (country in countries) {
        df2write <- subset(
            df, df$country_code == country, c(
                "country_code",
                "year_week",
                "time",
                "age_group",
                "variant",
                "new_cases"
            )
        )
        write.csv(df2write,
            file = paste(country, "_case_variants.csv", sep = "")
        )
    }
}

message("Getting age_group & variants dataframe... (1/10)")
age_group_df <- get_df_age_group()
variants_df <- get_df_variants()


message("Checking integrity of age_group... (2/10)")
check_dataframe(age_group_df)
message("Checking integrity of variants... (3/10)")
check_dataframe(variants_df)

message("Normalizing dataframes... (4/10)")
age_group_df <- normalize_dataframe(age_group_df)
variants_df <- normalize_dataframe(variants_df)


message("Formatting dataframes... (5/10)")
filter_start_date <- min(variants_df$time, na.rm = TRUE)
filter_end_date <- max(variants_df$time, na.rm = TRUE)

variants_df_formatted <- format_dataframe(
    variants_df,
    start_date = filter_start_date, end_date = filter_end_date
)
age_group_df_formatted <- format_dataframe(
    age_group_df,
    start_date = filter_start_date, end_date = filter_end_date
)

age_group_df_formatted_country <- age_group_df
variants_df_formatted_country <- variants_df_formatted

# Filtering by country
message("Filtering by country...")
age_group_df_formatted_country <- filter(age_group_df_formatted_country, country_code %in% c("FR", "AT", "BE", "IT"))
variants_df_formatted_country <- filter(variants_df_formatted_country, country_code %in% c("FR", "AT", "BE", "IT"))


message("Removing exclusive dates in both dataset... (6/10)")
dataframes <- remove_exclusive_dates(
    variants_df_formatted_country, age_group_df_formatted_country)
variants_df_formatted_country <- dataframes[[1]]
age_group_df_formatted_country <- dataframes[[2]]


message("Downloading Genomic Datasets from NCBI... (7/10)")
download_genomic_data(
    ecdcdata = variants_aggregated_formatted_BE,
    variant_colname = "variant",
    path_dataformat = "/usr/bin/dataformat",
    path_dataset = "/usr/bin/datasets",
    path_nextclade = "/usr/bin/nextclade"
)
genomic_data <- read.csv("genomic_data.csv")


message("Obtaining simulated cases & variants dataframe... (8/10)")
case_variants_aggregated <- simulator(
    trainset = variants_df_formatted_country,
    testset = age_group_df_formatted_country, geolocalisation = "country_code",
    outcome = "variant",
    count = "new_cases",
    time = "time",
    factor = 500
)
write.csv(case_variants_aggregated, file = "case_variants_aggregated")

message("Merging genomic data and the result of UCL simulator... (9/10)")
data_mutation <- find_mutation(genomic_data)

write.csv(genomic_data, file = "genomic_data")
write.csv(data_mutation, file = "data_mutation")

data_mutation <- normalize_dates(data_mutation)

case_variants_genomic <- add_genomic_data(
    metadata = case_variants_aggregated,
    genomic_data = data_mutation,
    col_merge = "variant",
    count = "new_cases",
    time = "time",
    mutation = T
)


message("Writing resulting datasets... (10/10)")
write_country_csv(case_variants_genomic)
