library(httr)
library(XML)
library(dplyr)
library(jsonlite)
library(Pandem2simulator)
library(ggplot2)

filter_country_code <- "HU"

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
    #df1_without_exclusive <- select(df1[(df1$check == "YES"), ], -"check")
    #df2_without_exclusive <- select(df2[(df2$check == "YES"), ], -"check")
    df1_without_exclusive <- df1[(df1$check == "YES"), ]
    df2_without_exclusive <- df2[(df2$check == "YES"), ]
    write.csv(df1, file = "df1.csv")
    write.csv(df2, file = "df2.csv")
    return(list(df1_without_exclusive, df2_without_exclusive))
}

write_country_csv <- function(df) {
    countries <- unique(df$country_code)
    for (country in countries) {
        df2write <- subset(
            df, df$country_code == "LU", c(
                "country_code", "year_week", "time", "age_group", "variant"))
        write.csv(df2write,
            file = paste(country, "_case_variants.csv", sep = "")
        )
    }
}

# 1. Normalize dataframes

age_group_df <- normalize_dataframe(get_df_age_group())
variants_df <- normalize_dataframe(get_df_variants())

# 2. Format dataframes

filter_start_date <- variants_df$time[1]
filter_end_date <- variants_df$time[length(variants_df$time)]
print(filter_start_date)
print(filter_end_date)


variants_df_formatted <- format_dataframe(
    variants_df,
    start_date = filter_start_date, end_date = filter_end_date
)
age_group_df_formatted <- format_dataframe(
    age_group_df,
    start_date = filter_start_date, end_date = filter_end_date
)

# (OPTIONAL: 3. Filter by given country dataframes)

variants_df_formatted_country <- variants_df_formatted %>%
    filter(country_code == filter_country_code)
age_group_df_formatted_country <- age_group_df_formatted %>%
    filter(country_code == filter_country_code)

# 4. Remove couples date/country that aren't in both dataframes

dataframes <- remove_exclusive_dates(
    variants_df_formatted_country, age_group_df_formatted_country)
variants_df_formatted_country <- dataframes[[1]]
age_group_df_formatted_country <- dataframes[[2]]

# 5. Obtain simulated case & variants dataframe

case_variants_aggregated <- simulator(
    trainset = variants_df_formatted_country,
    testset = age_group_df_formatted_country, geolocalisation = "country_code",
    outcome = "variant",
    count = "new_cases",
    time = "time",
    factor = 500
)
print(case_variants_aggregated)

# 6. Write a resulting CSV file per country

write_country_csv(case_variants_aggregated)