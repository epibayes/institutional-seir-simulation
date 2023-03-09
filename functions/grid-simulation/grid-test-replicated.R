#' Stub for how location changes are calculated
#'
#' Currently not calculating anything...
calculate_location = function(time, location, isolation, movement_probabilities) location

#' Column-wise rules for infection status
calculate_status = function(time, infection_time, latent_duration, infectious_duration) {
  N = length(infection_time)
  infected = !is.na(infection_time)
  infection_age = time - infection_time
  stopifnot(all(is.na(infection_age) | infection_age >= 0))
  status = rep(NA_character_, N) %>%
    purrr::map_if(!infected, ~ 'S') %>%
    purrr::map_if(infected & (infection_age <= latent_duration), ~'E') %>%
    purrr::map_if(infected & (infection_age  > latent_duration) & 
      (infection_age <= latent_duration + infectious_duration), ~'I') %>%
    purrr::map_if(infected & (infection_age > latent_duration + infectious_duration), ~ 'R') %>%
    purrr::flatten_chr()
  stopifnot(all(!is.na(status)))
  status = state$new(x = status, labels = c('S', 'E', 'I', 'R'))
  return(status)
}

#' Column-wise rules for isolation status
calculate_isolation = function(time, start_time, duration, status, test_pr) {
  N = length(start_time)
  isolated = !is.na(start_time)
  isolation_age = time - start_time
  stopifnot(all(is.na(isolation_age) | isolation_age >= 0))
  iso_status = rep(NA, N) %>% 
    purrr::map_if(!isolated, ~ FALSE) %>%
    purrr::map_if(isolated & isolation_age <= duration, ~ TRUE) %>%
    purrr::map_if(isolated & isolation_age > duration & status %in% c('E', 'I'), 
      ~ rbinom(n = 1, size = 1, prob = test_pr) == 1) %>%
    purrr::map_if(isolated & isolation_age > duration & status %in% c('S', 'R'), ~ FALSE) %>%
    purrr::flatten_lgl()
  stopifnot(all(!is.na(iso_status)))
  return(iso_status)
}

#' Column-wise rules for movement
calculate_movement = function(time, location, isolation_status, movement_probabilities) {

  return(location)
}

#' Retrieve a parameter from the finalize list
get_parameter = function(l, nm) {
  o = rlang::env_git(l$running, nm, default = NULL, inherit = TRUE)
  return(o)
}

#' Population summary
population_summary = function(p, status = 'detained') {
  summary = p$summarize(
      time = time, 
      detention_age = detention_age, 
      cohort = cohort,
      location = location, 
      isolation = dplyr::if_else(isolation, 'isolated', 'general-population'),
      infection_status = infection_status$state,
      first_infection = first_infection,
      .return_type = 'tibble')
  if (nrow(summary) == 0) {
    return(summary)
  } 
  summary = summary %>% 
    dplyr::group_by(time, detention_age, cohort, location, isolation, infection_status) %>%
    dplyr::summarize(
      n_intake_people = sum(detention_age == 0),
      n_new_cases = sum(first_infection),
      n_people = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(status = status) 
  return(summary)
}

