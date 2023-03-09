
#' Merge cohort-specific summary files to overall table
calculate_cohort_summary = function(files, n_cohorts_include = 365) {
  cohort_data = purrr::map(files, readRDS) %>% 
    purrr::lift_dl(dplyr::bind_rows)() %>%
    dplyr::rename(  ## FIXME: better names in simulation
      cohort_age = detention_age,
      n_breakthrough = n_breakthrough,
      n_exposed_breakthrough = n_exposed_breakthrough,
      n_infectious_breakthrough = n_infectious_breakthrough,
      n_recovered_breakthrough = n_recovered_breakthrough) %>%
    dplyr::mutate(
      n_breakthrough_cases = n_infectious_breakthrough + n_exposed_breakthrough
    )
                       
  cohort_birth = cohort_data %>%
    dplyr::group_by(cohort) %>%
    dplyr::summarize(cohort_birth = min(time), cohort_death = max(time),
                     cohort_lifetime = cohort_death - cohort_birth + 1) %>%
    dplyr::ungroup()
  cohort_age_points = expand.grid(
    cohort_age = dplyr::pull(cohort_data, cohort_age) %>% unique(),
    cohort = dplyr::pull(cohort_data, cohort) %>% unique(),
    stringsAsFactors = FALSE)

  cohort_data = cohort_data %>% 
    dplyr::left_join(y = cohort_birth, by = 'cohort') %>%
    dplyr::filter(cohort_birth <= n_cohorts_include) %>%
    dplyr::right_join(y = cohort_age_points, by = c('cohort_age', 'cohort')) 

  cohort_summary = cohort_data %>% dplyr::mutate(
    dplyr::across(tidyselect::starts_with("n_"), ~ dplyr::if_else(is.na(.x), 0L, .x)))  

  return(cohort_summary)
}

#' Table summarizing what happen at day zero and day (release)
calculate_cohort_endpoints = function(cohort_summary, release_day = 14) {
  endpoints = dplyr::left_join(
      x = cohort_summary %>%
        dplyr::filter(cohort_age == 0) %>%
        dplyr::group_by(cohort) %>%
        dplyr::summarize(
          n_total = n_total,
          n_isolated_start = n_isolated,
          n_cohort_start = n_total - n_isolated,
          n_breakthrough_cases_start = n_breakthrough_cases,
          p_breakthrough_case_start = n_breakthrough_cases / n_cohort_start),
      y = cohort_summary %>%
        dplyr::filter(cohort_age == release_day) %>% 
        dplyr::group_by(cohort) %>%
        dplyr::summarize(
          n_isolated_end = n_isolated,
          n_cohort_end = n_total - n_isolated,
          n_breakthrough_cases_end = n_breakthrough_cases,
          p_breakthrough_case_end = n_breakthrough_cases / n_cohort_end),
      by = 'cohort')
  return(endpoints)
}

calculate_breakthrough_cases = function(cohort_summary) {
  cohort_endpoints = calculate_cohort_endpoints(cohort_summary)

  breakthrough_summary = cohort_endpoints %>% 
    dplyr::mutate(n_total_cohorts = dplyr::n()) %>%
    dplyr::group_by(n_breakthrough_cases_end) %>% 
    dplyr::summarize(
      n_cohorts = dplyr::n(),
      n_total_cohorts = unique(n_total_cohorts)) %>%
    dplyr::mutate(breakthrough_percentage = 100 * (n_cohorts / n_total_cohorts)) %>%
    dplyr::arrange(n_breakthrough_cases_end) %>% 
    dplyr::mutate(cumulative_breakthrough_percentage = cumsum(breakthrough_percentage)) %>%
    dplyr::arrange(n_breakthrough_cases_end)
  return(breakthrough_summary)
}

calculate_breakthrough_proportion = function(cohort_summary) {
  cohort_count = cohort_summary %>% 
    dplyr::mutate(n_breakthrough = n_infectious_breakthrough + n_exposed_breakthrough) %>%
    dplyr::mutate(n_breakthrough = dplyr::if_else(is.na(n_breakthrough), 0L, n_breakthrough)) %>%
    dplyr::group_by(cohort_age, n_breakthrough) %>%
    dplyr::summarize(cohort_count = dplyr::n()) %>%
    dplyr::ungroup()
  return(cohort_count)
}
  
intake_simulation_summary = function(simulation_name, simulation_id, replicate_id) {
  require(simulator)
  require(magrittr)

  summary_dir = workflow::artifact_dir(simulation_name, simulation_id, replicate_id)
  fs::dir_create(path = summary_dir, recurse = TRUE)
  
  simulation_dir = workflow::build_dir(simulation_name, simulation_id, replicate_id)
  cohort_files = fs::dir_ls(simulation_dir, regexp  = 'cohort-summary--')

  ## FIXME: these could be analyzed elsewhere
  #timepoint_files = fs::dir_ls(simulation_dir, regexp = 'timepoint-data--')
  #cohorts_files = fs::dir_ls(simulation_dir, regexp = 'cohorts--')
  
  paths = character()

  cohort_summary = calculate_cohort_summary(cohort_files)
  save_path = fs::path(summary_dir, "intake-cohort-summary.rds")
  saveRDS(cohort_summary, save_path)
  paths = c(paths, save_path)

  breakthrough_summary = calculate_breakthrough_cases(cohort_summary)
  save_path = fs::path(summary_dir, "intake-breakthrough-summary.rds")
  saveRDS(breakthrough_summary, save_path)
  paths = c(paths, save_path)
 
  breakthrough_proportion = calculate_breakthrough_proportion(cohort_summary)
  save_path = fs::path(summary_dir, "intake-breakthrough-proportion.rds")
  saveRDS(breakthrough_proportion, save_path)
  paths = c(paths, save_path)

  
  n_cutoff = 4
  breakthrough_proportion_agg = dplyr::bind_rows(
    dplyr::filter(breakthrough_proportion, n_breakthrough < n_cutoff) %>%
      dplyr::group_by(cohort_age, n_breakthrough) %>%
      dplyr::summarize(cohort_count = sum(cohort_count)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(n_breakthrough = as.character(n_breakthrough)),
    dplyr::filter(breakthrough_proportion, n_breakthrough >= n_cutoff) %>%
      dplyr::group_by(cohort_age) %>%
      dplyr::summarize(cohort_count = sum(cohort_count)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(n_breakthrough = paste0(n_cutoff, "+"))
  )
  save_path = fs::path(summary_dir, "intake-breakthrough-proportion-agg.rds")
  saveRDS(breakthrough_proportion_agg, save_path)
  paths = c(paths, save_path)

  return(paths)
}

intake_breakthrough_summary = function(simulation_name, day = 14) {
  require(simulator)
  require(magrittr)
  
  theta = readRDS(file = workflow::build_dir(simulation_name, "parameter-grid.rds")) 
  summary = theta %>%
    dplyr::select(simulation_name, simulation_id, replicate_id) %>%
    purrr::pmap( ~ workflow::artifact_dir(..1, ..2, ..3, "intake-cohort-summary.rds") %>%
      readRDS() %>%
      dplyr::filter(time == day) %>%
      dplyr::mutate(
        simulation_name = ..1, simulation_id = ..2, replicate_id = ..3)
    ) %>% 
    purrr::lift_dl(dplyr::bind_rows)() %>%
    dplyr::left_join(theta, by = c('simulation_name', 'simulation_id', 'replicate_id'))

  set_estimates = dplyr::left_join(
    x = summary %>% 
      dplyr::group_by(simulation_name, simulation_id, replicate_id) %>%
      dplyr::summarize(
        quantile = seq(from = 0, to = 1, by = 0.1), 
        quantile_tag = paste0('q', quantile * 100),
        estimate = quantile(n_breakthrough_cases, p = quantile)) %>%
      tidyr::pivot_wider(
        id_cols = c(simulation_name, simulation_id, replicate_id, quantile_tag),
        names_from = quantile_tag, values_from = estimate),
    y = summary %>% 
      dplyr::group_by(simulation_name, simulation_id, replicate_id) %>%
      dplyr::summarize(
        min = min(n_breakthrough_cases),
        mean = mean(n_breakthrough_cases),
        max = max(n_breakthrough_cases)),
    by = c('simulation_name', 'simulation_id', 'replicate_id')) %>%
  dplyr::left_join(theta, by = c('simulation_name', 'simulation_id', 'replicate_id'))
  
#  set_estimates = dplyr::left_join(
#    x = summary %>% 
#      dplyr::group_by(intake_prevalence) %>%
#      dplyr::summarize(
#        quantile = seq(from = 0, to = 1, by = 0.1), 
#        quantile_tag = paste0('q', quantile * 100),
#        estimate = quantile(n_breakthrough_cases, p = quantile)) %>%
#      tidyr::pivot_wider(
#        id_cols = c(intake_prevalence, quantile_tag),
#        names_from = quantile_tag, values_from = estimate),
#    y = summary %>% 
#      dplyr::group_by(intake_prevalence) %>%
#      dplyr::summarize(
#        min = min(n_breakthrough_cases),
#        mean = mean(n_breakthrough_cases),
#        max = max(n_breakthrough_cases)),
#    by = c('intake_prevalence'))
  
  return(set_estimates)
}





