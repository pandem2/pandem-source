`%>%` <- magrittr::`%>%`

df1 <- data.frame(reporting_time, article_count) %>% dplyr::mutate(reporting_date = substr(reporting_time, 1, 10)) 
df2 <- df1 %>% dplyr::group_by(reporting_date ) %>% dplyr::summarize(count = sum(article_count))

df2$alert <- epitweetr::ears_t_reweighted(ts = df2$count, alpha = 0.025, alpha_outlier = 0.05, k_decay = 4, no_historic = 7, same_weekday_baseline = T)$alarm0
dplyr::inner_join(df1, df2, by = "reporting_date")$alert
