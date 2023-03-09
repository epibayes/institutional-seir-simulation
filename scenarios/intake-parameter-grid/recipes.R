
#' Constructing each new cohort
daily_cohort_recipe = recipe(
  # Population initialization
  detention_age = rep(0, n_intake_today),
  first_infection = rep(FALSE, n_intake_today),
  detention_start = rep(time, n_intake_today),
  detention_duration = rexp(n_intake_today, rate = kappa),
  rtu_needed = simulator::two_state_generator(
    labels = c('rtu', 'general-population'), p = rep(rtu_prevalence, n_intake_today)),
  latent_period_duration = rexp(n = n_intake_today, rate = sigma),
  disease_period_duration = rexp(n = n_intake_today, rate = gamma),
  infection_time = rep(NA_real_, n_intake_today) %>% 
    purrr::map_at(
        sample.int(n_intake_today, n_intake_infections), 
      ~ time - sample.int(mean_disease_duration, size = 1)) %>%
    purrr::map_dbl(as.numeric),
  infection_status = calculate_status(time, infection_time, latent_period_duration, disease_period_duration),
  infection_silent = simulator::mix_generator(n = n_intake_today, 
    mix = c(silent = silent_rate, symptomatic = 1 - silent_rate)),
  isolation_start_time = rep(NA_real_, n_intake_today),
  isolation = rep(FALSE, n_intake_today),
  cohort = rep(group, n_intake_today),
  location = rep(group, n_intake_today) %>% 
    purrr::map_if(rtu_needed$state == 'rtu', ~ 'rtu-intake') %>%
    purrr::map_chr(~ .),
  p_infection = rep(NA_real_, n_intake_today)
)

#' Once per simulation
sim_init = recipe(
  n_intake_today = numeric(),
  n_intake_infections = numeric(),
  n_staff_infections = numeric(),
  time = time,
  .continue = TRUE,

  population = simulator::make_population(
    time = time, 
    group = paste0('cohort--', simulator::pad(time, 3)),
    n_intake_today = 1, n_intake_infections = 0,
    rtu_prevalence = rtu_prevalence,
    mean_disease_duration = mean_disease_duration,
    sigma = sigma, gamma = gamma, silent_rate = silent_rate,
    kappa = kappa, steps = daily_cohort_recipe),

  population$mutate(
    detention_age = time - detention_start,
    first_infection = rep(FALSE, length(detention_start)),
    test_positive = rep(FALSE, length(detention_start)),
    infection_status = calculate_status(time, infection_time, 
      latent_period_duration, disease_period_duration),
    test_positive = symptomatic_assessment_result$state == 'positive'),
  population$mutate(
    symptomatic_assessment_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state == 'I' & infection_silent$state == 'symptomatic') *
        test_sensitivity__observation),
    rapid_test_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state == 'I') * test_sensitivity__rapid),
    cxr_test_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state %in% c('I', 'R')) * (
        (infection_silent$state == 'silent') * test_sensitivity__cxr_asymp +
        (infection_silent$state == 'symptomatic') * test_sensitivity__cxr_symp)),
    cxr_tb_test_result = simulator::two_state_generator(
      labels = c('positive', 'negative'), p = rep(tb_rate, infection_status$n_units)),
    test_positive = 
      symptomatic_assessment_result$state == 'positive' |
      rapid_test_result$state == 'positive' |
      cxr_test_result$state == 'positive' |
      cxr_tb_test_result$state == 'positive'),
  released_population = population$bleb(integer()),
  today_cohort = population$bleb(integer()),
  epidemic_summary = NULL
)

#' Each time step of the simulation
sim_step = recipe(
  time = time + 1,
  {population$time = time},
  n_intake_today = rpois(n = 1, lambda = n_intake_rate),
  n_intake_infections = rpois(n = 1, lambda = intake_seed_rate),
  n_staff_infections = rpois(n = 1, lambda = staff_seed_rate), 

  # Daily state update and symptomatic assessment
  population$mutate(
    detention_age = time - detention_start,
    first_infection = rep(FALSE, length(test_positive)),
    test_positive = rep(FALSE, length(test_positive)),
    infection_status = calculate_status(time, infection_time, 
      latent_period_duration, disease_period_duration),
    symptomatic_assessment_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state == 'I' & infection_silent$state == 'symptomatic') *
        test_sensitivity__observation),
    test_positive = symptomatic_assessment_result$state == 'positive'),

  # Split released population, also at day 30 all released.
  released_population = population$which(
    purrr::map_dbl(detention_duration, ~ min(.x, 30)) <= detention_age
  ) %>% population$bleb(),

  # Day X intake test
  pcr_test_population = population$which(detention_duration == cohort_retest) %>% 
    population$bleb(),
  pcr_test_population$mutate(
      pcr_test_result = simulator::two_state_generator(
        labels = c('positive', 'negative'),
        p = (infection_status$state == 'I') * test_sensitivity__pcr),
      test_positive = test_positive | pcr_test_result$state == 'positive'),
  population$absorb(pcr_test_population),
  
  # New cohort
  today_cohort = simulator::make_population(
    time = time, 
    group = paste0('cohort--', simulator::pad(time, 3)),
    n_intake_today = n_intake_today, 
    n_intake_infections = n_intake_infections,
    rtu_prevalence = rtu_prevalence,
    mean_disease_duration = mean_disease_duration,
    sigma = sigma, 
    gamma = gamma, silent_rate = 
    silent_rate,
    kappa = kappa,
    steps = daily_cohort_recipe),
  
  # Intake testing
  today_cohort$mutate(
    symptomatic_assessment_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state == 'I' & infection_silent$state == 'symptomatic') *
        test_sensitivity__observation),
    rapid_test_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state == 'I') * test_sensitivity__rapid),
    cxr_test_result = simulator::two_state_generator(
      labels = c('positive', 'negative'),
      p = (infection_status$state %in% c('I', 'R')) * (
        (infection_silent$state == 'silent') * test_sensitivity__cxr_asymp +
        (infection_silent$state == 'symptomatic') * test_sensitivity__cxr_symp)),
    cxr_tb_test_result = simulator::two_state_generator(
      labels = c('positive', 'negative'), p = rep(tb_rate, infection_status$n_units)),
    test_positive = 
      symptomatic_assessment_result$state == 'positive' |
      rapid_test_result$state == 'positive' |
      cxr_test_result$state == 'positive' |
      cxr_tb_test_result$state == 'positive'
  ),

  # Intake joins
  population$absorb(today_cohort),

  # Daily isolation movement
  population$mutate(
    isolation_start_time = isolation_start_time %>% 
      purrr::map_if(test_positive, ~ time) %>% 
      purrr::flatten_dbl(),
    isolation = calculate_isolation(time, isolation_start_time, isolation_retest, 
      infection_status$state, test_sensitivity__pcr)
  ),

  # Calculate daily epidemic status
  local_risk = population$summarize(
      infectious_breakthrough = infection_status$state == 'I' & !isolation,
      isolation = isolation, 
      location = location,
      .return_type = 'list') %>%
    purrr::lift_dl(tibble::tibble)(),
  epidemic_summary = local_risk %>%
    dplyr::group_by(location) %>%
    dplyr::summarize(n_infectious = sum(infectious_breakthrough), 
                  n_local = dplyr::n(), 
                  n_isolated = sum(isolation),
                  infection_rate = beta * (n_infectious / (n_local - n_isolated)),
                  infection_probability = 1 - exp(-infection_rate)) %>%
    dplyr::ungroup(),
  local_risk = local_risk %>% 
    dplyr::select(location) %>%
    dplyr::left_join(y = epidemic_summary, by = 'location') %>%
    dplyr::pull(infection_probability),
 
  # Daily movement
  population$mutate(
    location = calculate_location(time, location, isolation, movement_probabilities)
  ),

  # Daily infection
  population$mutate(
    p_infection = dplyr::if_else(infection_status$state == 'S', local_risk, 0),
    first_infection = runif(n = length(p_infection)) < p_infection,
    infection_time = infection_time %>% 
      purrr::map_if(first_infection, ~ time) %>%
      purrr::flatten_dbl()
  ),

  # Location summary
  location_summary = population$summarize(
      location = location,
      .return_type = 'list') %>%
    purrr::lift_dl(tibble::tibble)(),
  
  .continue = time <= n_steps || time <= max_n_steps
)

