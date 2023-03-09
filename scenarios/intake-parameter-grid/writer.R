
#' Object responsible for saving simulation data
writer_setup = recipe(
  .time = 0,
  .simulation = simulation_id,
  .replicate = replicate_id,
  .output_path = output_path,
  if (is.null(.output_path) || .output_path == "") {
    rlang::abort("Output path not properly specified.", class = 'filesystem-error', 
                 output_path = output_path)
  },
  output_path %>% fs::dir_create(recurse = TRUE),
  log_path = log_path,
  log_path %>% fs::dir_create(recurse = TRUE),
  log_file = fs::path(log_path, "logger.txt"),
  cat("log file: ", log_file, "\n"),
  logger::log_appender(logger::appender_file(log_file))
)

writer_summaries = recipe(
  .time = time,
  .simulation = simulation_id,
  .replicate = replicate_id,
  .output_path = output_path,
  logger::log_info("step: {time}, n_detainees: {n_detainees}", 
    time = time, n_detainees = nrow(population)),
  timepoint_data = tibble::tibble(
    time = time,
    simulation_id = simulation_id,
    replicate_id = replicate_id,
    n_intake_today = n_intake_today,
    n_intake_infections = n_intake_infections,
    n_staff_infections = n_staff_infections
  ),
  internal_population_summary = population_summary(population, 'detained'),
  released_population_summary = population_summary(released_population, 'released'),
  internal_population = population,
  released_population = released_population,
  today_cohort = today_cohort,
  epidemic_summary = epidemic_summary
)
