
library(targets)
options(clustermq.scheduler = 'slurm', clustermq.template = './clustermq.tmpl')
targets::tar_option_set(
  memory = "transient",
  garbage_collection = TRUE, 
  storage = "worker",
  retrieval = "worker",
  error = "continue",
  workspace_on_error = TRUE,
  resources = targets::tar_resources(
    clustermq = targets::tar_resources_clustermq(
      template = list(
        memory = 8000,
        log_file = "./slurm-clustermq-worker-%a.log",
        time = "04:00:00"
      )
    )
  )
)

library(workflow)
library(simulator)
library(magrittr)
library(ggplot2)

log_file = workflow::build_file("logs", "data-intake.log") %>%
  logger::appender_file() %>% logger::log_appender()
logger::log_info("Start CCCF data intake.")

pipeline_helpers = workflow::project_file("data-processing", "pipeline-helpers.R")
pipeline_steps = workflow::project_file("data-processing", "pipeline-steps.R")
scenario_functions = workflow::project_file("functions", "grid-simulation", "grid-test-replicated.R")
source(pipeline_helpers)
source(pipeline_steps)
source(scenario_functions)

list(
  tar_target(
    auth_token,
    {rdrop2::drop_auth(); ".httr-oauth"},
    format = "file",
    deployment = "main"
  ),
  tar_target(
    dropbox_project_root,
    fs::path('/sph-epibayes/cccf-project'),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    source_root,
    fs::path(dropbox_project_root, 'cccf-background-data/original'),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    applied_test_sensitivity, 0.6, 
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    remote_root,
    fs::path_dir(source_root),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    original_data_paths,
    find_original_data_paths(auth_token, source_root),
    format = "file",
    deployment = "main"
  ),
  tar_target(
    mrsa_data,
    extract_mrsa_data(original_data_paths),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    unit_match_table,
    extract_recoding_data(original_data_paths, mrsa_data, capacity_data, occupancy_data),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    capacity_data,
    extract_capacity_data(original_data_paths),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    detainee_count_data,
    extract_detainee_count_data(original_data_paths),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    unit_occupancy_data,
    extract_occupancy_data(original_data_paths),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    standard_movement_data,
    standardize_movement_data(mrsa_data, unit_match_table),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    standard_capacity_data,
    standardize_capacity_data(capacity_data, unit_match_table),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    standard_occupancy_data,
    standardize_occupancy_data(unit_occupancy_data, unit_match_table),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    movement_transition_data, 
    standardize_movement_transition_data(standard_movement_data),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    standard_count_data,
    standardize_count_data(detainee_count_data, applied_test_sensitivity),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    event_data,
    standardize_event_data(movement_transition_data),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    movement_observed,
    summarize_movement_observed(movement_transition_data, unit_match_table),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    building_mmo,
    summarize_building_level_moves(movement_observed),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(unit_mmo, 
    summarize_unit_level_moves(movement_observed, unit_match_table),
    format = 'qs'
  ),
  tar_target(same_unit_ecdf, 
    unit_mmo %>% 
      dplyr::filter(last_stable_unit == unit, row_sums > 0, percentage != 0, percentage != 100) %>% 
      dplyr::pull(percentage) %>% ecdf(),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(building_count, 
    count_building_level_moves(building_mmo),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(building_percentage,
    percent_building_level_moves(building_mmo),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    unit_transition_plot,
    unit_level_transitions_plot(unit_mmo),
    format = 'qs'
  ),
  tar_target(
    detention_table, {
      detention_table = standard_movement_data %>%
          dplyr::select(record_id, unit, time_in, time_out, time_discharge) %>%
          dplyr::group_by(record_id) %>%
          dplyr::summarize(
            time_detention = min(time_in, time_out, time_discharge, na.rm = TRUE),
            time_release = max(time_in, time_out, time_discharge, na.rm = TRUE))
      detention_table
    }, 
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    duration_of_detention_data,
    summarize_duration_distribution(standard_movement_data),
    format = 'qs'
  ),
  tar_target(
    duration_of_detention_quantiles,
    summarize_duration_quantiles(duration_of_detention_data),
    format = 'qs'
  ),
  tar_target(
    exploratory_jail_data_template, 
    fs::path("templates", "jail-data-summaries.Rmd"),
    format = 'file',
    deployment = "main"
  ),

  #' Artifacts uploaded to dropbox follder and saved locally.
  tar_target(
    detainee_count_file,
    save_upload_artifact(standard_count_data, "standard-count-data.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = 'file',
    deployment = 'main'
  ),
  tar_target(
    unit_match_rds_file,
    save_upload_artifact(unit_match_table, "unit-match-table.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = "file",
    deployment = 'main'
  ),
  tar_target(
    capacity_file,
    save_upload_artifact(standard_capacity_data, "standard-capacity-data.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = 'file',
    deployment = 'main'
  ),
  tar_target(
    standard_occupancy_file,
    save_upload_artifact(standard_occupancy_data, "standard-occupancy-data.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = 'file',
    deployment = 'main'
  ),
  tar_target(
    standard_movement_file,
    save_upload_artifact(standard_movement_data, "standard-movement-data.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = 'file',
    deployment = 'main'
  ),
  tar_target(
    duration_of_detention_file,
    save_upload_artifact(duration_of_detention_data, "duration-of-detention-data.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = 'file',
    deployment = 'main'
  ),
  tar_target(
    duration_of_detention_quantiles_file,
    save_upload_artifact(duration_of_detention_quantiles, "duration-of-detention-quantiles.rds",
      dir = workflow::artifact_dir("summary"),
      remote_dir = fs::path(remote_root, "processed")),
    format = 'file',
    deployment = 'main'
  ),
#  tar_target(
#    exploratory_jail_data_summary, {
#      path = workflow::generate_document(
#        template = exploratory_jail_data_template,
#        standard_capacity_data = standard_capacity_data,
#        standard_occupancy_data = standard_occupancy_data,
#        standard_count_data = standard_count_data,
#        unit_mmo = unit_mmo,
#        duration_of_detention_quantiles = duration_of_detention_quantiles,
#        applied_test_sensitivity = applied_test_sensitivity,
#        building_count = building_count,
#        building_percentage = building_percentage,
#        unit_transition_plot = unit_transition_plot,
#        rel_output_path = "data-summary")
#      file_name = fs::path_file(path)
#      rdrop2::drop_upload(file = path, path = fs::path(remote_root, "processed", "summary", file_name));
#      path
#    }, 
#    format = 'file'
#  ),
  tar_target(
    event_data_summary, {
      quantiles = summarize_event_quantiles(event_data)
      proportions = summarize_event_proportions(event_data)
      list(quantiles = quantiles, proportions = proportions)
    },
    format = 'qs'
  ),
  tar_target(
    event_data_quantile_plot, {
      event_data_summary$quantiles %>% 
        ggplot() +
        geom_line(aes(x = duration, y = q, colour = move_type)) + 
        coord_cartesian(xlim = c(0,50)) +
        theme_minimal()
    },
    format = 'qs'
  ),
  tar_target(
    get_cmdstan, {
      cmdstan_dir = workflow::cache_dir("cmdstanr") %>% 
        fs::dir_ls() %>%
        dplyr::last()
      cmdstanr::check_cmdstan_toolchain()
      cmdstanr::install_cmdstan(
        dir = workflow::cache_dir("cmdstanr"),
        cores = 6)
      #fs::dir_ls(path = workflow::cache_dir("cmdstanr"), recurse = TRUE, all = TRUE)
      cmdstanr::set_cmdstan_path(cmdstan_dir)
      Sys.setenv(CMDSTAN = cmdstan_dir)
      cmdstan_dir
    },
    format = 'file'
  ),
  tar_target(
    model_A_event_data, {
      event_data_df = event_data %>% 
        dplyr::filter(move_type != 'irrelevant') %>%
        dplyr::transmute(
          row = 1:dplyr::n(),
          time_to_event = as.numeric(duration),
          units = 'days',
          event_type = move_type,
          initial_building = last_stable_building
        ) %>% 
        dplyr::left_join(
          y = event_coding_table(),
          by = 'event_type')
      event_data_df
    },
    format = 'qs'
  ),
  tar_target(
    model_A_event_data_cmdstan, {
      event_data_df = model_A_event_data %>% 
        dplyr::filter(!is.na(time_to_event), !is.na(event_type))
      list(
        N = nrow(event_data_df),
        K = unique(event_data_df[['event_type']]) %>% length,
        P = nrow(event_coding_table()),
        time_to_event = event_data_df[['time_to_event']],
        event_type = event_data_df[['event_type_int']])
    }, 
    format = 'qs'
  ),
  tar_target(
    model_A_name,
    "simple-time-to-event",
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    model_A_file,
    paste0(model_A_name, '.stan'),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    model_A_path,
    workflow::project_file("models", model_A_file),
    format = 'file',
    deployment = "main"
  ),
  tar_target(
    model_A_locations, {
      model_name = model_A_name
      model_file = model_A_file
      model_path = model_A_path
      model_build_dir = workflow::build_dir("models", model_name)
      fs::file_copy(model_path, model_build_dir, overwrite = TRUE)
      target_model_path = workflow::build_file("models", model_name, model_file)
      model_output_dir = fs::path(model_build_dir, "output")
      fs::dir_create(model_output_dir)
      stdout_path = fs::path(model_build_dir, "stdout.txt")
      fs::file_create(stdout_path)
      stderr_path = fs::path(model_build_dir, "stderr.txt")
      fs::file_create(stderr_path)
      ppc_dir = fs::path(model_build_dir, "output", "ppc")
      fs::dir_create(ppc_dir, recurse = TRUE)
      c(
        model_name = model_name,
        model_file_name = model_file,
        model_file = target_model_path,
        cmdstanr_dir = model_build_dir,
        output_dir = model_output_dir,
        ppc_dir = ppc_dir,
        stdout = stdout_path,
        stderr = stderr_path
      )
    },
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    model_A_compile_file,
    model_A_locations['model_file'],
    format = 'file',
    deployment = "main"
  ),
  tar_target(
    model_A_object, {
      cmdstanr::set_cmdstan_path(get_cmdstan)
      cmdstanr::cmdstan_model(model_A_compile_file)
    },
    format = 'qs'
  ),
  tar_target(chains, 1:4, format = 'qs'),
  tar_target(
    model_A_mcmc_run, {
      loc = model_A_locations
      cmdstanr::set_cmdstan_path(get_cmdstan)
      model = model_A_object$sample(
        data = model_A_event_data_cmdstan,
        refresh = 1, 
        save_latent_dynamics = TRUE,
        output_dir = loc['output_dir'],
        output_basename = loc['model_name'],
        chains = 1,
        chain_ids = chains,
        iter_warmup = 250,
        iter_sampling = 150,
        save_warmup = TRUE
      )
      model_object_file = fs::path(loc['output_dir'], 'model-object.rds')
      saveRDS(model, file = model_object_file)
      output_files = fs::dir_ls(
        path = loc['output_dir'],
        regexp = paste0(loc['model_name'], '-', chains, '.*\\.csv'))
      fs::path_real(output_files) %>% unique()
    },
    pattern = map(chains),
    format = 'file'
  ),
  tar_target(
    model_A_mcmc_draws, {
      output_files = model_A_mcmc_run %>% 
        purrr::discard(stringr::str_detect, pattern = '.*\\.rds') 
      obj = cmdstanr::read_cmdstan_csv(files = output_files)
    },
    format = 'qs'
  ),
  tar_target(
    model_A_mcmc_draws_df, {
      obj = model_A_mcmc_draws$post_warmup_draws %>%
        posterior::as_draws_df() %>% 
        posterior::subset_draws(variable = 'lambda')
    },
    format = 'qs'
  ),
  tar_target(
    model_A_mcmc_draws_ls,
    purrr::pmap(model_A_mcmc_draws_df, list),
    format = 'qs'
  ),
  tar_target(
    model_A_ppc_1, {
      draws = model_A_mcmc_draws_ls[[1]]
      data = model_A_event_data
      N = data %>%
        dplyr::filter(event_type == 'intake') %>%
        nrow()
      record_idx = 1:N
      fake_data = purrr::map(record_idx, ~ simulate_events(parameters = draws)) %>%
        purrr::map(purrr::map, purrr::lift_dl(tibble::tibble_row)) %>%
        purrr::map(purrr::lift_dl(dplyr::bind_rows)) %>%
        purrr::lift_dl(dplyr::bind_rows)() %>%
        dplyr::mutate(
          .iteration = draws$.iteration,
          .chain = draws$.chain,
          .draw = draws$.draw)
      return(list(fake_data))
    },
    pattern = map(model_A_mcmc_draws_ls),
    format = 'qs'
  ),
  tar_target(
    model_A_ppc_1_summary, {
      ppc_data = targets::tar_read(model_A_ppc_1) %>%
        purrr::map( ~ .x %>% 
          dplyr::group_by(event_type) %>%
          dplyr::summarize(
            ppc_count = dplyr::n(),
            .iteration = unique(.iteration),
            .chain = unique(.chain),
            .draw = unique(.draw)
          ) %>%
          dplyr::ungroup() %>%
          dplyr::mutate(ppc_percentage = (ppc_count / sum(ppc_count)) * 100) %>%
          dplyr::left_join(y = event_data_summary$proportions, by = c(event_type = 'move_type')) %>%
          dplyr::mutate(
            count_error = ppc_count - count,
            percentage_points_error = ppc_percentage - percentage
          )
        )  %>% 
        purrr::lift_dl(dplyr::bind_rows)()
      ppc_data
    },
    format = 'qs'
  ),
  tar_target(
    model_A_ppc_1_plot, {
      pl_percentage_points_error = ggplot(data = model_A_ppc_1_summary,
        aes(x = percentage_points_error)
      ) + geom_histogram() + 
          facet_wrap( ~ event_type)
      pl_count_error = ggplot(data = model_A_ppc_1_summary,
        aes(x = count_error)
      ) + geom_histogram() + 
          facet_wrap( ~ event_type)
      list(
        percentage_points_error = pl_percentage_points_error,
        count_error = pl_count_error)
    },
    format = 'qs'
  ),
  tar_target(
    model_B_plausible_building_transitions, {
      event_data %>% 
        dplyr::select(building, last_stable_building) %>% 
        purrr::flatten_chr() %>% 
        unique() %>% 
        purrr::discard(~ .x == 'RCDC') %>% 
        tidyr::expand_grid(initial_building = ., current_building = .) %>%
        dplyr::arrange(initial_building, current_building)
    },
    format = 'qs'
  ),
  tar_target(
    model_B_transitions_df, {
      transitions_switch_building = model_B_plausible_building_transitions %>%
        dplyr::filter(initial_building != current_building) %>%
        dplyr::mutate(
          event_type = dplyr::case_when(
            initial_building != "Intake" 
              ~ paste('switch_building', initial_building, current_building, sep = '-'),
            initial_building == "Intake" 
              ~ paste('intake', initial_building, current_building, sep = '-')
          ),
          is_intake = dplyr::case_when(
            initial_building == 'Intake' ~ 1,
            TRUE ~ 0),
          is_rejoin = -1,
          is_stay = -1,
          is_exit = -1,
          is_switch_unit = 0
        )
      transitions_rejoin =  model_B_plausible_building_transitions %>%
        dplyr::filter(initial_building == current_building) %>%
        dplyr::mutate(
          event_type = paste('rejoin_unit', initial_building, current_building, sep = '-'),
          is_intake = 0,
          is_rejoin = 1,
          is_stay = -1,
          is_exit = -1,
          is_switch_unit = -1
        )
      transitions_stay = tibble::tibble_row(
        event_type = 'stay',
        is_intake = 0,
        is_rejoin = -1,
        is_stay = 1,
        is_exit = -1,
        is_switch_unit = -1
      )
      transitions_switch_unit = tibble::tibble_row(
        event_type = 'switch_unit',
        is_intake = 0,
        is_rejoin = -1,
        is_stay = -1,
        is_exit = -1,
        is_switch_unit = 1
      )
      transitions_exit = tibble::tibble_row(
        event_type = 'exit',
        is_intake = 0,
        is_rejoin = -1,
        is_stay = -1,
        is_exit = 1,
        is_switch_unit = -1
      )
      transitions = dplyr::bind_rows(transitions_switch_building, 
        transitions_rejoin, transitions_stay, transitions_switch_unit,
        transitions_exit)
      transitions
    },
    format = 'qs'
  ),
  tar_target(
    model_B_event_data, {
      event_data_df = event_data %>% 
        dplyr::filter(move_type != 'irrelevant') %>%
        dplyr::transmute(
          record_id = record_id, 
          row = 1:dplyr::n(),
          time_to_event = as.numeric(duration),
          units = 'days',
          initial_building = last_stable_building,
          current_building = building,
          event_type = dplyr::case_when(
            move_type %in% c('intake', 'switch_building', 'rejoin_unit') ~ 
              paste(move_type, initial_building, current_building, sep ='-'),
            move_type == 'exit' ~ 'exit',
            move_type == 'switch_unit' ~ 'switch_unit',
            move_type == 'rejoin_unit' ~ 'rejoin_unit',
            move_type == 'stay' ~ 'stay',
            TRUE ~ NA_character_
          )
        ) %>% 
        dplyr::left_join(
          y = model_B_transitions_df %>% 
            dplyr::select(-initial_building, -current_building),
          by = c('event_type'))
      event_data_df    
    },
    format = 'qs'
  ),
  tar_target(
    model_B_bad_event_data, {
      bad_records = model_B_event_data %>% 
        dplyr::filter(is.na(is_intake)) %>% 
        dplyr::pull(record_id)
      model_B_event_data %>% dplyr::filter(record_id %in% bad_records)
    },
    format = 'qs'
  ),
  tar_target(
    model_B_event_data_cmdstan, {
      event_data_df = model_B_event_data %>% 
        dplyr::filter(!is.na(time_to_event), !is.na(event_type))
      list(
        N = nrow(event_data_df),
        K = unique(event_data_df[['event_type']]) %>% length,
        P = nrow(event_coding_table()),
        time_to_event = event_data_df[['time_to_event']],
        event_type = event_data_df[['event_type_int']])
    }, 
    format = 'qs'
  ),
  tar_target(
    model_B_name,
    "generic-time-to-event",
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    model_B_file,
    paste0(model_B_name, '.stan'),
    format = 'qs',
    deployment = "main"
  ),
  tar_target(
    model_B_path,
    workflow::project_file("models", model_B_file),
    format = 'file',
    deployment = "main"
  ),
  tar_target(
    model_B_locations, {
      model_name = model_B_name
      model_file = model_B_file
      model_path = model_B_path
      model_build_dir = workflow::build_dir("models", model_name)
      fs::file_copy(model_path, model_build_dir, overwrite = TRUE)
      target_model_path = workflow::build_file("models", model_name, model_file)
      model_output_dir = fs::path(model_build_dir, "output")
      fs::dir_create(model_output_dir)
      stdout_path = fs::path(model_build_dir, "stdout.txt")
      fs::file_create(stdout_path)
      stderr_path = fs::path(model_build_dir, "stderr.txt")
      fs::file_create(stderr_path)
      ppc_dir = fs::path(model_build_dir, "output", "ppc")
      fs::dir_create(ppc_dir, recurse = TRUE)
      c(
        model_name = model_name,
        model_file_name = model_file,
        model_file = target_model_path,
        cmdstanr_dir = model_build_dir,
        output_dir = model_output_dir,
        ppc_dir = ppc_dir,
        stdout = stdout_path,
        stderr = stderr_path
      )
    },
    format = 'qs',
    deployment = "main"
  ),
#  tar_target(
#    model_B_compile_file,
#    model_B_locations['model_file'],
#    format = 'file',
#    deployment = "main"
#  ),
#  tar_target(
#    model_B_object, {
#      cmdstanr::set_cmdstan_path(get_cmdstan)
#      cmdstanr::cmdstan_model(model_B_compile_file)
#    },
#    format = 'qs'
#  ),
#  tar_target(
#    model_B_mcmc_run, {
#      loc = model_B_locations
#      cmdstanr::set_cmdstan_path(get_cmdstan)
#      model = model_B_object$sample(
#        data = model_B_event_data_cmdstan,
#        refresh = 1, 
#        save_latent_dynamics = TRUE,
#        output_dir = loc['output_dir'],
#        output_basename = loc['model_name'],
#        chains = 1,
#        chain_ids = chains,
#        iter_warmup = 250,
#        iter_sampling = 150,
#        save_warmup = TRUE
#      )
#      model_object_file = fs::path(loc['output_dir'], 'model-object.rds')
#      saveRDS(model, file = model_object_file)
#      output_files = fs::dir_ls(
#        path = loc['output_dir'],
#        regexp = paste0(loc['model_name'], '-', chains, '.*\\.csv'))
#      fs::path_real(output_files) %>% unique()
#    },
#    pattern = map(chains),
#    format = 'file'
#  ),
#  tar_target(
#    model_B_mcmc_draws, {
#      output_files = model_B_mcmc_run %>% 
#        purrr::discard(stringr::str_detect, pattern = '.*\\.rds') 
#      obj = cmdstanr::read_cmdstan_csv(files = output_files)
#    },
#    format = 'qs'
#  ),
#  tar_target(
#    model_B_mcmc_draws_df, {
#      obj = model_B_mcmc_draws$post_warmup_draws %>%
#        posterior::as_draws_df() %>% 
#        posterior::subset_draws(variable = 'lambda')
#    },
#    format = 'qs'
#  ),
#  tar_target(
#    model_B_mcmc_draws_ls,
#    purrr::pmap(model_B_mcmc_draws_df, list),
#    format = 'qs'
#  ),


  ## Scenario simulation
  tar_target(
    scenarios_recipes,
    c( 
      workflow::project_file("scenarios", "intake-parameter-grid", "recipes.R"),
      workflow::project_file("scenarios", "intake-parameter-grid", "writer.R"),
      workflow::project_file("data-processing", "pipeline-helpers.R"), 
      workflow::project_file("data-processing", "pipeline-steps.R"),
      workflow::project_file("functions", "grid-simulation", "grid-test-replicated.R")
    ),
    format = 'file',
    deployment = "main"
  ),
  tar_target(
    scenarios_parameter_grid, {
      scenarios_parameter_grid = simulator:::expand_parameters(
        list(
          simulation_name = 'intake-parameter-grid',
          n_replicates = 25,
          n_steps = 30, 
          max_n_steps = 60,
          n_intake_rate = 100,
          intake_prevalence = c(0.01, 0.1),
          n_staff = 3000,
          staff_prevalence = 0.05,
          rtu_prevalence = 1.00,
          r0 = simulator::parameter_range(c(1.1, 5.0), n = 3),
          silent_rate = 0.2,
          mean_latent_duration = 5,
          mean_disease_duration = 14,
          mean_length_of_stay = 30,
          tb_rate = 0.001,
          isolation_retest = 14,
          cohort_retest = 14,
          test_sensitivity = list(
            observation = 0.0,
            rapid = 0.0,
            pcr = 0.0,
            cxr_asymp = 0,
            cxr_symp = 0
          )
        ),
        list(
          simulation_name = 'intake-parameter-grid',
          n_replicates = 25,
          n_steps = 30, 
          max_n_steps = 60,
          n_intake_rate = 100,
          intake_prevalence = c(0.01, 0.1),
          n_staff = 3000,
          staff_prevalence = 0.05,
          rtu_prevalence = 1.00,
          r0 = simulator::parameter_range(c(1.1, 5.0), n = 3),
          silent_rate = 0.2,
          mean_latent_duration = 5,
          mean_disease_duration = 14,
          mean_length_of_stay = 30,
          tb_rate = 0.001,
          isolation_retest = 14,
          cohort_retest = 14,
          test_sensitivity = list(
            observation = 0.0,
            rapid = 0.0,
            pcr = 0.0,
            cxr_asymp = 0.10,
            cxr_symp = 0.65
          )
        ),        
        list(
          simulation_name = 'intake-parameter-grid',
          n_replicates = 25,
          n_steps = 30, 
          max_n_steps = 60,
          n_intake_rate = 100,
          intake_prevalence = c(0.01, 0.1),
          n_staff = 3000,
          staff_prevalence = 0.05,
          rtu_prevalence = 0.00,
          r0 = simulator::parameter_range(c(1.1, 5.0), n = 3),
          silent_rate = 0.2,
          mean_latent_duration = 5,
          mean_disease_duration = 14,
          mean_length_of_stay = 30,
          tb_rate = 0.001,
          isolation_retest = 14,
          cohort_retest = 14,
          test_sensitivity = list(
            observation = 0.0,
            rapid = 0.0,
            pcr = 0.0,
            cxr_asymp = 0,
            cxr_symp = 0
          )
        ),
        list(
          simulation_name = 'intake-parameter-grid',
          n_replicates = 25,
          n_steps = 30, 
          max_n_steps = 60,
          n_intake_rate = 100,
          intake_prevalence = c(0.01, 0.1),
          n_staff = 3000,
          staff_prevalence = 0.05,
          rtu_prevalence = 0.00,
          r0 = simulator::parameter_range(c(1.1, 5.0), n = 3),
          silent_rate = 0.2,
          mean_latent_duration = 5,
          mean_disease_duration = 14,
          mean_length_of_stay = 30,
          tb_rate = 0.001,
          isolation_retest = 14,
          cohort_retest = 14,
          test_sensitivity = list(
            observation = 0.0,
            rapid = 0.0,
            pcr = 0.0,
            cxr_asymp = 0.10,
            cxr_symp = 0.65
          )
        ),
        list(
          simulation_name = 'intake-parameter-grid',
          n_replicates = 25,
          n_steps = 30, 
          max_n_steps = 60,
          n_intake_rate = 100,
          intake_prevalence = c(0.01, 0.1),
          n_staff = 3000,
          staff_prevalence = 0.05,
          rtu_prevalence = 0.0,
          r0 = simulator::parameter_range(c(1.1, 5.0), n = 3),
          silent_rate = 0.2,
          mean_latent_duration = 5,
          mean_disease_duration = 14,
          mean_length_of_stay = 30,
          tb_rate = 0.001,
          isolation_retest = 14,
          cohort_retest = c(3, 5, 7, 14),
          test_sensitivity = list(
            observation = 0.1,
            rapid = 0.40,
            pcr = c(0.0, 0.65, 0.85),
            cxr_asymp = 0.10,
            cxr_symp = 0.65
          )
        ), 
        list(
          simulation_name = 'intake-parameter-grid',
          n_replicates = 25,
          n_steps = 30, 
          max_n_steps = 60,
          n_intake_rate = 100,
          intake_prevalence = c(0.01, 0.1),
          n_staff = 3000,
          staff_prevalence = 0.05,
          rtu_prevalence = 1.0,
          r0 = simulator::parameter_range(c(1.1, 5.0), n = 3),
          silent_rate = 0.2,
          mean_latent_duration = 5,
          mean_disease_duration = 14,
          mean_length_of_stay = 30,
          tb_rate = 0.001,
          isolation_retest = 14,
          cohort_retest = c(3, 5, 7, 14),
          test_sensitivity = list(
            observation = 0.1,
            rapid = 0.40,
            pcr = c(0.0, 0.65, 0.85),
            cxr_asymp = 0.10,
            cxr_symp = 0.65
          )
        ), 
        recipes = list(
          output = simulator::recipe(
            output_path = fs::path(output_path),
            log_path = fs::path(output_path, "logger")
          ),
          shared = simulator::recipe(
            beta = r0 / (mean_disease_duration - mean_latent_duration),
            sigma = 1 / mean_latent_duration,
            gamma = 1 / (mean_disease_duration - mean_latent_duration),
            kappa = 1 / (mean_length_of_stay),
            intake_seed_rate = intake_prevalence * n_intake_rate,
            staff_seed_rate = staff_prevalence * n_staff,
            calculate_status = calculate_status,
            calculate_isolation = calculate_isolation,
            calculate_location = calculate_location
          ),
          initialization = simulator::recipe(
            time = 0
          ),
          running = simulator::recipe(
            time = 0,
            n_steps = n_steps
          )
        ),
        n_batches = 700
      )
      saveRDS(scenarios_parameter_grid,
        file = fs::path(workflow::build_dir("simulations", "intake-parameter-grid"), "parameter-grid.rds")
      )
      scenarios_parameter_grid
    },
    format = 'qs'
  ),
  tar_target(
    scenarios_split_parameter_grid, {
      scenarios_parameter_grid %>%
        dplyr::group_by(job_batch) %>%
        dplyr::group_split() %>%
        rlang::set_names(purrr::map_chr(., ~ unique(.x$job_batch)))
    }
  ),
  tar_target(
    scenarios_split_parameter_files, {
      paths = scenarios_split_parameter_grid %>% purrr::imap(~ {
        root = workflow::build_dir("simulations", "intake-parameter-grid", "batches")
        path = fs::path(root, paste0("parameter-grid-", .y, ".rds"))
        saveRDS(.x, file = path)
        path
      }) %>% purrr::flatten_chr()
      paths
    },
    format = 'file',
    pattern = map(scenarios_split_parameter_grid)
  ),
  tar_target(
    scenarios_grid_simulations, {
      library(simulator)
      library(magrittr)
      library(ggplot2)
      purrr::map(scenarios_recipes, source)
      parameters = scenarios_split_parameter_grid[[1]]
      population_data = list()
      for (i in 1:nrow(parameters)) {
        thetas = parameters[i,] %>% as.list() %>% simulator::parameters()
        timing = system.time({
          sim = simulator::simulation(thetas, sim_init, sim_step,
            writer = simulator::make_writer(writer_setup, writer_summaries))
        })
        population_data[[i]] = sim$files %>%
          select_population_files() %>%
          purrr::map(readRDS) %>%
          purrr::map(population_summary) %>%
          dplyr::bind_rows() %>%
          dplyr::mutate(job_tag = parameters[i, 'job_tag']$job_tag)
        attr(population_data[[i]], 'simulation_id') = parameters[i,'simulation_id']
        attr(population_data[[i]], 'replicate_id') = parameters[i,'replicate_id']
        attr(population_data[[i]], 'parameters') = as.list(parameters[i,])
      }
      population_data 
    }, 
    format = 'qs',
    pattern = map(scenarios_split_parameter_grid)
  ),
  tar_target(
    scenarios_batched_counts, {
      parameters = purrr::map(scenarios_grid_simulations, ~ attr(.x, 'parameters')) %>%
        purrr::map( ~ .x[c('simulation_id', 'replicate_id', 'r0', 'cohort_retest', 'test_sensitivity__observation', 'test_sensitivity__pcr', 'test_sensitivity__cxr_symp', 'test_sensitivity__rapid', 'intake_prevalence')])
      cohort_proportions = scenarios_grid_simulations %>% purrr::map( ~ .x %>% 
        dplyr::filter(cohort %in% sort(unique(cohort))[1:30]))
      data = purrr::map2(parameters, cohort_proportions, ~ dplyr::bind_cols(.x, .y)) %>%
        dplyr::bind_rows()
      data = data %>%
        dplyr::mutate(is_cohorted = location != 'rtu-intake') %>%
        dplyr::group_by(simulation_id, replicate_id, is_cohorted) %>% 
        dplyr::summarize(
          total_new_cases = sum(n_new_cases), 
          total_intake_people = sum(n_intake_people), 
          proportion = total_new_cases / total_intake_people) %>%
        dplyr::ungroup() %>%
        dplyr::left_join(
          y = dplyr::bind_rows(parameters),
          by = c('simulation_id', 'replicate_id')
        )
      data
    },
    format = 'qs',
    pattern = map(scenarios_grid_simulations)
  ),
  tar_target(
    scenarios_counts, 
    dplyr::bind_rows(scenarios_batched_counts),
    format = 'qs'
  )
)
# FIXME: make plots
#cohort_plots = purrr::map( ~ ggplot(data = .x) + geom_point(aes(x = proportion, y = cohort)))

#  tar_target(
#    scenarios_location_summary_plots, {
#      data = scenarios_population_state
#      parameters = scenarios_split_parameter_grid[[1]]
#      pl = list()
#      for (i in 1:length(data)) {
#        pl[[i]] = data[[i]] %>%
#          dplyr::mutate(population = status, count = n_cumulative_new_cases) %>%
#          dplyr::group_split(location) %>%
#          purrr::map(plot_simulated_state_time_series, id_string = parameters[i,'job_tag'])
#      }
#      pl
#    },
#    format = 'qs',
#    pattern = map(scenarios_population_state, scenarios_split_parameter_grid)
#  ),
## FIXME: make these static, somehow?
#  tar_target(
#    scenarios_cumulative_case_counts_plots, {
#      data_by_replicate = scenarios_population_state %>%
#        dplyr::group_by(
#          parameters$simulation_id, 
#          parameters$replicate_id,
#          parameters$intake_prevalence, 
#          parameters$r0,
#          parameters$isolation_retest,
#          parameters$test_sensitivity__observation,
#          parameters$test_sensitivity__rapid,
#          parameters$test_sensitivity__pcr,
#          parameters$test_sensitivity__cxr_asymp,
#          parameters$test_sensitivity__cxr_symp,
#          time
#        ) %>%
#        dplyr::summarize(total_cases = sum(n_
#          
#          
#      pl_bw = ggplot() +
#          geom_boxplot(
#            data = data_by_replicate,
#            aes(x = factor(`parameters$isolation_retest`), y = total_cases, 
#                color = factor(`parameters$test_sensitivity__pcr`),
#                group = paste(`parameters$simulation_id`)),
#            color = 'black'
#          ) +
#          geom_jitter(
#            data = data_by_replicate,,
#            aes(x = factor(`parameters$isolation_retest`), y = total_cases, 
#                color = factor(`parameters$test_sensitivity__pcr`),
#                group = paste(`parameters$replicate_id`)),
#            height = 0, 
#            width = 0.02
#          ) +
#          theme_minimal() + 
#          scale_x_discrete("PCR Timing (days post-intake)") +
#          scale_y_continuous("Total Cases (all cohorts in replicate)") +
#          scale_color_discrete("PCR test sensitivity", h = c(20, 250), c = 80) +
#          facet_grid(no_testing ~ `parameters$intake_prevalence`) +
#          ggtitle("Intake Cohort Isolation (except RTU)")
#      pl_cdf = ggplot() +
#          geom_line(
#            data = scenarios_var_pcr_replicate_final_case_counts %>%
#              dplyr::group_by(parameters$simulation_name,
#                              parameters$simulation_id, 
#                              parameters$replicate_id,
#                              parameters$intake_prevalence, 
#                              parameters$r0) %>%
#              dplyr::summarize(total_cases = sum(total_cases)) %>%
#              dplyr::ungroup() %>%
#              dplyr::group_by(`parameters$simulation_name`,
#                              `parameters$simulation_id`,
#                              `parameters$replicate_id`,
#                              `parameters$intake_prevalence`, 
#                              `parameters$r0`) %>%
#              dplyr::summarize(
#                quantiles = seq(from = 0, to = 1, length.out = 150),
#                ecdf = quantiles %>% purrr::map_dbl( ~ quantile(total_cases, probs = .x))
#              ) %>%
#              dplyr::ungroup(),
#            aes(x = quantiles * 100, y = total_cases, color = factor(parameters$r0))) +
#          theme_minimal() + 
#          scale_x_discrete("Proportion of Replicates (%)") +
#          scale_y_continuous("Maximum of Total Cases (Percentile)") +
#          scale_color_discrete(expression(paste(R[0])), h = c(20, 250), c = 80) +
#          facet_wrap( ~ parameters$intake_prevalence) +
#          ggtitle("Perfect Cohort Isolation")
#      list(boxplot = pl_bw)
#    }, 
#    format = 'qs'
#  ),
#  tar_target(
#    scenarios_var_reproductive_replicate_final_case_counts_plots_pdf, {
#      target_dir = workflow::artifact_dir("scenarios", "var-reproductive", "draft")
#      file = fs::path(target_dir, "replicate-outcomes-boxplots.pdf")
#      pl = scenarios_var_reproductive_replicate_final_case_counts_plots
#      pdf(file = file)
#      try(print(pl$boxplot))
#      dev.off()
#      file
#    },
#    format = 'file'
#  )

