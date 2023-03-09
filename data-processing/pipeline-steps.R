
#' Discover paths to original CCJ data files on dropbox
#'
#' When dropbox is not available you can download these manually.
#'
#' @param auth token file
#' @param root directory to look in (on dropbox)
#'
#' @return *local* paths to the fetched files
find_original_data_paths = function(auth_token, source_root) {
  source_paths = source_root %>%
    rdrop2::drop_dir() %>% 
    dplyr::pull(path_lower)
  source_files = fs::path_file(source_paths)
  local_paths = purrr::map_chr(source_files, 
    ~ workflow::data_file("original", .x))
  fetched = source_paths %>% 
    purrr::map_lgl(rdrop2::drop_download, 
      local_path = workflow::data_dir("original"), 
      overwrite = TRUE)
  return(local_paths)
}

#' Pick MRSA study movement/location data out of the spreadsheet
#'
#' @param original_data_paths vector of paths to spreadhseet files, we
#' grab only one.
#'
#' @return data frame of all relevant columns with simplified names
extract_mrsa_data = function(original_data_paths) {
  match = stringr::str_detect(original_data_paths, "date_shifted_ccj.csv") 
  path = original_data_paths[match]
  movement = path %>%
    readr::read_csv() %>%
    dplyr::transmute(
      study_id = STUDY_ID, episode_id = EpisodeNumber, building = LOC_BUILDING,
      unit_mrsa_data = LOC_NURSE_UNIT, time_in = `TIME IN`, time_out = `TIME OUT`,
      time_discharge = `DISCHARGE DATE`)
  return(movement)
}

#' Unit-level capacity data
#'
#' @param original_data_paths vector of paths to spreadhseet files, we
#' grab only one.
#'
#' @return data frame of all relevant columns with simplified names
extract_capacity_data = function(original_data_paths) {
  match = stringr::str_detect(original_data_paths, "division tier capacity.xls")
  path = original_data_paths[match]
  capacity = path %>%
    readxl::read_xls() %>%
    dplyr::transmute(
      building = Division, facility = Facility, unit_capacity_data = Tier, 
      capacity = Capacity, type = `Holding Type`)
  return(capacity)
}

#' Total intake and COVID-19 testing data.
#'
#' @param original_data_paths vector of paths to spreadhseet files, we
#' grab only one.
#'
#' @return data frame of all relevant columns with simplified names
extract_detainee_count_data = function(original_data_paths) { 
  match = stringr::str_detect(original_data_paths, "modelingdata.xlsx")
  path = original_data_paths[match]
  detainee_counts = path %>% 
    readxl::read_xlsx(skip = 1, sheet = "CHS Aggregate") %>%
    dplyr::transmute(
      date = DATE + lubridate::hours(7), 
      lag_1_intake = `TOTAL INTAKES FROM PREVIOUS DAY`,
      lag_1_test_count = `TOTAL INTAKE COVID19 TESTS FROM PREVIOUS DAY`,
      lag_1_covid_count = `TOTAL INTAKE POSITIVE PATIENTS`,
      cermak_count = `CERMAK POPULATION (EXCLUDING 15HP)`,
      isolated_count = `TOTAL COVID19+ PATIENTS IN ISOLATION`)
  return(detainee_counts)
}

#' Unit-level occupancy data
#'
#' @param original_data_paths vector of paths to spreadhseet files, we
#' grab only one.
#'
#' @return data frame of all relevant columns with simplified names
extract_occupancy_data = function(original_data_paths) {
  match = stringr::str_detect(original_data_paths, "modelingdata.xlsx")
  path = original_data_paths[match]
  occupancy_data = path %>%
    readxl::read_xlsx(skip = 1, sheet = "Daily 530AM Occupancy By Unit") %>%
    tidyr::pivot_longer(
      cols = tidyselect::matches(match = '^[^D]'),
      names_to = 'unit', values_to = 'count') %>%
    dplyr::transmute(
      date = DATE + lubridate::hours(5) + lubridate::minutes(30),
      unit_occupancy_data = unit, count = count)
  return(occupancy_data)
}

#' Two-system tier/unit re-coding table
#'
#' @param original_data_paths vector of paths to spreadhseet files, we
#' grab only one.
#' @param mrsa_data extracted from original paths
#' @param capacity_data extracted from original paths
#' @param occupancy_data extracted from original paths
#'
#' @return table of all units with all naming schemes matched up as well as
#' possible, mostly manually...
extract_recoding_data = function(
  original_data_paths, 
  mrsa_data = extract_mrsa_data(original_data_paths),
  capacity_data = extract_capacity_data(original_data_paths),
  occupancy_data = extract_occupancy_data(original_data_paths)
) {
  match = stringr::str_detect(original_data_paths, "modelingdata.xlsx")
  path = original_data_paths[match]
  unit_codes = path %>%
    readxl::read_xlsx(sheet = "Daily 530AM Occupancy By Unit", 
      range = "B1:IF2", col_names = FALSE, col_types = 'text') %>%
    purrr::map(~ purrr::lift_dl(tibble::tibble_row)(
      unit = .x[2], unit_other = .x[1])) %>%
    purrr::lift_dl(dplyr::bind_rows)() %>%
    dplyr::mutate(
      unit_occupancy_data = unit,
      unit_other = stringr::str_replace(unit_other, 'DIV([0-9])-', 'DIV0\\1-'),
      unit_other = stringr::str_replace(unit_other, 'DIV3AX-', 'DIV03AX-')
    ) %>% dplyr::bind_rows(
      tibble::tibble_row(unit = "RCDC", unit_other = "RCDC", unit_occupancy_data = "RCDC"),
      tibble::tibble_row(unit = "M/HLD", unit_other = "RCDC-M/HLD", unit_occupancy_data = "RCDC-M/HLD"),
      tibble::tibble_row(unit = "RCDC-DISCH", unit_other = "RCDC-DISCHARGE", unit_occupancy_data = "RCDC-DISCHARGE"),
      tibble::tibble_row(unit = "RCDC-EM", unit_other = "RCDC-EM", unit_occupancy_data = "RCDC-EM"),
      tibble::tibble_row(unit = "RCDC-TRANSFER", unit_other = "RCDC-TRANSFER", unit_occupancy_data = "RCDC-TRANSFER"),
      tibble::tibble_row(unit = "Intake", unit_other = "Intake", unit_occupancy_data = "Intake"),
      tibble::tibble_row(unit = "16-16BCB31AA", unit_other = "DIV16-31AA", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "16-16BCB11AA", unit_other = "DIV16-11AA", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "16-16BCB21AA", unit_other = "DIV16-21AA", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "16-16BCB21BB", unit_other = "DIV16-21BB", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "16-16BCB41AA", unit_other = "DIV16-41AA", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "16-16BCB41BB", unit_other = "DIV16-41BB", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "16-16BCPR",    unit_other = "DIV16-BCPR", unit_occupancy_data = "Boot Camp"),
      tibble::tibble_row(unit = "02-02D41LU",   unit_other = "DIV02-D41LU", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "02-02D41NU",   unit_other = "DIV02-D41NU", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "03-031B",      unit_other = "DIV03-031B", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "16-16BCBR",   unit_other = "DIV16-BCBR", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15EM",      unit_other = "DIV15-EM", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15EMAW",    unit_other = "DIV15-EMAW", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15HP",      unit_other = "DIV15-HP", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15JF",      unit_other = "DIV15-JF", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15KA",      unit_other = "DIV15-KA", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15KK",      unit_other = "DIV15-KK", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15LV",      unit_other = "DIV15-LV", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15MRCR",    unit_other = "DIV15-MRCR", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15OT",      unit_other = "DIV15-OT", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15RI",      unit_other = "DIV15-RI", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15SPIDOC",  unit_other = "DIV15-SPIDOC", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "15-15US",      unit_other = "DIV15-US", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-011A",      unit_other = "DIV01-1A", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-011ABO",    unit_other = "DIV01-1ABO", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-011E",      unit_other = "DIV01-1E", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-012C",      unit_other = "DIV01-2C", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-013C",      unit_other = "DIV01-3C", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-013D",      unit_other = "DIV01-3D", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-013E",      unit_other = "DIV01-3E", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-014E",      unit_other = "DIV01-4E", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-014F",      unit_other = "DIV01-4F", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-014H",      unit_other = "DIV01-4H", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-01DR",      unit_other = "DIV01-DR", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "01-01-UNKNOWN", unit_other = "DIV01-UNKNOWN", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "09-09-UNKNOWN", unit_other = "DIV09-UNKNOWN", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "02-02-UNKNOWN", unit_other = "DIV02-UNKNOWN", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "06-06-UNKNOWN", unit_other = "DIV06-UNKNOWN", unit_occupancy_data = NA_character_),
      tibble::tibble_row(unit = "09-09DR",        unit_other = "DIV09-DR", unit_occupancy_data = NA_character_)
    )
  
  unit_recodes = list(
    from_cerner = unit_codes[['unit']] %>%
      rlang::set_names(unit_codes[['unit_other']]),
    to_cerner = unit_codes[['unit_other']] %>%
      rlang::set_names(unit_codes[['unit']])
  )

  unit_mrsa = mrsa_data %>% 
    dplyr::select(building, unit_mrsa_data) %>%
    unique() %>%
    dplyr::mutate(
      unit = unit_mrsa_data,
      unit = dplyr::if_else(building != "16", unit,
          stringr::str_replace(unit, '16BCB([^-])', '16-16BCB\\1')),
      unit = dplyr::if_else(building != "16", unit,
          stringr::str_replace(unit, '16BCPR', '16-16BCPR')),
      unit = dplyr::if_else(building != "03AX", unit,
          stringr::str_replace(unit, '03AX([^-])', '03AX-03AX\\1')),
      unit = dplyr::if_else(building != "2", unit,
          stringr::str_replace(unit, '02D([^-])', '02-02D\\1')),
      unit = dplyr::if_else(building != "4", unit,
          stringr::str_replace(unit, '04([1-9])([A-Z])', '04-04\\1\\2')),
      unit = dplyr::if_else(building != "3", unit,
          stringr::str_replace(unit, '03([1-9])([A-Z])', '03-03\\1\\2')),
      unit = dplyr::if_else(building != "6", unit,
          stringr::str_replace(unit, '06([1-9])([A-Z])', '06-06\\1\\2')),
      unit = dplyr::if_else(building != "8", unit,
          stringr::str_replace(unit, '08([1-9])([A-Z])', '08-08\\1\\2')),
      unit = dplyr::if_else(building != "9", unit,
          stringr::str_replace(unit, '09([1-9][1-9])([A-Z])', '09-09\\1\\2')),
      unit = dplyr::if_else(building != "9", unit,
          stringr::str_replace(unit, '091LI', '09-091LI')),
      unit = dplyr::if_else(building != "10", unit,
          stringr::str_replace(unit, '10([1-9])([A-Z])', '10-10\\1\\2')),
      unit = dplyr::if_else(building != "11", unit,
          stringr::str_replace(unit, '11([1-9])([A-Z][A-Z])', '11-11\\1\\2')),
      unit = dplyr::if_else(building != "8", unit,
          stringr::str_replace(unit, 'CM(..)', '08-CM\\1')),
      unit = dplyr::if_else(building != "RCDC", unit,
          stringr::str_replace(unit, '990[1-2]', 'Intake')),
      unit = dplyr::if_else(building != "Intake", unit,
          stringr::str_replace(unit, '990[1-2]', 'Intake')),
      unit = dplyr::if_else(building != 1, unit,
          stringr::str_replace(unit, '01([0-4D].*)', '01-01\\1')),
      unit = dplyr::if_else(building != 15, unit,
          stringr::str_replace(unit, '^15([A-Z][A-Z].*)', '15-15\\1')),
      unit = dplyr::if_else(building != 1, unit,
          stringr::str_replace(unit, 'DIV 01', '01-01-UNKNOWN')),
      unit = dplyr::if_else(building != 9, unit,
          stringr::str_replace(unit, 'DIV 09', '09-09-UNKNOWN')),
      unit = dplyr::if_else(building != 2, unit,
          stringr::str_replace(unit, 'DIV 02', '02-02-UNKNOWN')),
      unit = dplyr::if_else(building != 6, unit,
          stringr::str_replace(unit, 'DIV 06', '06-06-UNKNOWN')),
      unit = dplyr::if_else(building != 9, unit,
          stringr::str_replace(unit, '09DR', '09-09DR')),
      match_recode_2 = unit %>%  
        purrr::map_int(~ stringdist::amatch(.x, unit_codes$unit, maxDist = 10)) %>%
        purrr::map_chr(~ unit_codes$unit[.x]),
      distance_recode_2 = unit %>% stringdist::stringdist(match_recode_2),
      match_recode_2 = dplyr::case_when(
        distance_recode_2 <= 1 ~ match_recode_2,
        TRUE ~ NA_character_),
      unit = match_recode_2
    ) %>%
    dplyr::select(-match_recode_2, -distance_recode_2)

  unit_capacity = capacity_data %>%
    dplyr::mutate(
      unit_other = unit_capacity_data,
      unit_other = stringr::str_replace(unit_other, 'DIV([0-9])-', 'DIV0\\1-'),
      unit_other = stringr::str_replace(unit_other, 'DIV3AX-', 'DIV03AX-')
    ) %>%
    dplyr::select(unit_other, unit_capacity_data)

  all_unit_codes = unit_codes %>% 
    dplyr::left_join(unit_mrsa, 'unit') %>%
    dplyr::left_join(unit_capacity, 'unit_other') %>%
    dplyr::mutate(
      building = dplyr::case_when(
        is.na(building) & stringr::str_detect(unit, '03AX') ~ "03AX",
        is.na(building) & stringr::str_detect(unit_other, '^DIV[01][0-9][-A]') ~ stringr::str_sub(unit_other, 4, 5),
        is.na(building) & stringr::str_detect(unit_other, 'Boot Camp') ~ "16",
        TRUE ~ building),
      building = as.character(building),
      building = dplyr::case_when(nchar(building) == 1 ~ paste0("0", building), 
                                  TRUE ~ building)
    )
  return(all_unit_codes)
}

#' Standardize detainee count data frame
#'
#' @param count_data extract of count data from spreadsheet
#'
#' @return standardized data frame
standardize_count_data = function(count_data, applied_test_sensitivity) {
  data = count_data %>% 
    dplyr::mutate(
      intake = dplyr::lag(lag_1_intake), 
      test_count = dplyr::lag(lag_1_test_count), 
      covid_count = dplyr::lag(lag_1_covid_count),
      cumulative_covid_count = covid_count %>%
        purrr::map_if(~ is.na(.x), ~ 0) %>%
        purrr::flatten_dbl() %>%
        cumsum(),
      proportion_tested = test_count / intake * 100,
      test_positivity = covid_count / test_count,
      pop_positivity_hat = round(intake * test_positivity) / applied_test_sensitivity
    )
  return(data)
}

#' Standardize unit occupancy data
#'
#' @param occupancy_data extract of occupancy data from spreadsheet
#' @param unit_table table for matching units across conventions
#'
#' @return standardized data frame
standardize_occupancy_data = function(occupancy_data, unit_table) { 
  data = occupancy_data %>%
    dplyr::left_join(
      y = unit_table %>% dplyr::select(unit_occupancy_data, unit, unit_other),
      by = 'unit_occupancy_data')
  return(data)
}

#' Standardize unit capacity data
#'
#' @param capacity_data extract of capacity data from spreadsheet
#' @param unit_table table for matching units across conventions
#'
#' @return standardized data frame
standardize_capacity_data = function(capacity_data, unit_table) { 
  data = capacity_data %>%
    dplyr::left_join(
      y = unit_table %>% dplyr::select(unit, unit_other, unit_capacity_data),
      by = 'unit_capacity_data')
  return(data)
}

#' Clean up unit-level coding in movement data
#'
#' @param mrsa_data data on detainee location
#' @param unit_table data on unit naming across conventions
#'
#' @return standardized data froame of movement data
standardize_movement_data = function(mrsa_data, unit_table) {
  movement = mrsa_data %>% 
    dplyr::mutate(
      record_id = paste(study_id, episode_id, sep = '-')) %>%
    dplyr::group_by(record_id) %>% 
    dplyr::mutate(
      time_in = dplyr::case_when(
        time_in < lubridate::ymd_hms("2016-01-01 00:00:00") ~ lubridate::ymd_hms(NA_character_),
        time_in > lubridate::ymd_hms("2020-12-31 00:00:00") ~ lubridate::ymd_hms(NA_character_),
        TRUE ~ time_in),
      time_out = dplyr::case_when(
        time_out < lubridate::ymd_hms("2016-01-01 00:00:00") ~ lubridate::ymd_hms(NA_character_),
        time_out > lubridate::ymd_hms("2020-12-31 00:00:00") ~ lubridate::ymd_hms(NA_character_),
        TRUE ~ time_out),
      best_time = dplyr::case_when(
        !is.na(time_in) ~ time_in,
        !is.na(time_out) ~ time_out,
        TRUE ~ time_in),
      best_time = dplyr::case_when(
        !is.na(best_time) ~ best_time,
        !is.na(dplyr::lead(best_time)) & !is.na(dplyr::lag(best_time)) ~ 
          c(dplyr::lead(best_time),dplyr::lag(best_time)) %>% mean)
    ) %>% 
    dplyr::ungroup() %>%
    dplyr::select(-building) %>%
    dplyr::left_join(
      y = unit_table %>% dplyr::select(unit_mrsa_data, unit, unit_other, building),
      by = c('unit_mrsa_data'))
  return(movement)
}

standardize_movement_transition_data = function(standard_movement_data) {
  typical = standard_movement_data %>%
    dplyr::filter(!is.na(time_in), !is.na(time_out)) %>%
    dplyr::group_by(building) %>%
    dplyr::summarize(
      stay = difftime(time_out, time_in, units = 'days') %>% mean(na.rm = TRUE)
    ) %>%
    dplyr::ungroup()

  bldgs = typical[['building']]
  typical = typical %>%
    dplyr::group_split(building) %>%
    purrr::map( ~ dplyr::pull(.x, stay))
  names(typical) = bldgs

  transition_data = standard_movement_data %>%
    dplyr::filter(!is.na(best_time)) %>%
    dplyr::arrange(record_id, best_time) %>%
    dplyr::select(-best_time, -time_discharge) %>% 
    dplyr::rename(timestamp = time_in, exit_timestamp = time_out) %>%
    dplyr::group_by(record_id) %>%
    dplyr::mutate(
      last_stable_unit = lag(unit, !stringr::str_detect(.x, 'RCDC')),
      last_stable_building = lag_friend(building, unit, !stringr::str_detect(.x, 'RCDC')),
      last_stable_timestamp = lag_friend(timestamp, unit, !stringr::str_detect(.x, 'RCDC')),
      last_unit = dplyr::lag(unit),
      last_building = dplyr::lag(building),
      last_timestamp = dplyr::lag(timestamp)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(
      record_id, 
      unit, last_unit, last_stable_unit, 
      timestamp, last_timestamp, last_stable_timestamp, 
      exit_timestamp,
      building, last_building, last_stable_building)
  return(transition_data)
}

standardize_event_data = function(data) {
  data = data %>%
    dplyr::group_by(record_id) %>%
    dplyr::mutate(
      move_type = dplyr::case_when(
        (unit == 'RCDC') & (timestamp == dplyr::last(timestamp)) ~ 'exit',
        (unit == last_unit) ~ 'stay',
        (stringr::str_detect(unit, 'RCDC')) ~ 'irrelevant',
        (unit != 'RCDC') & (last_stable_unit == 'Intake') ~ 'intake',
        (unit != last_unit) & (unit == last_stable_unit) ~ 'rejoin_unit',
        (unit != last_stable_unit) & (building == last_stable_building) ~ 'switch_unit',
        (building != last_stable_building) ~ 'switch_building',
        TRUE ~ NA_character_),
      duration = dplyr::case_when(
        move_type == 'exit' ~ difftime(exit_timestamp, timestamp, units = 'days'),
        move_type == 'irrelevant' ~ difftime(NA, NA, units = 'days'),
        move_type == 'intake' ~ difftime(timestamp, last_stable_timestamp, units = 'days'),
        move_type == 'stay' ~ difftime(timestamp, last_timestamp, units = 'days'),
        move_type == 'rejoin_unit' ~ difftime(timestamp, last_stable_timestamp, units = 'days'),
        move_type == 'switch_unit' ~ difftime(timestamp, last_stable_timestamp, units = 'days'),
        move_type == 'switch_building' ~ difftime(timestamp, last_stable_timestamp, units = 'days'),
        TRUE ~ difftime(NA, NA, units = 'days')
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(move_type != 'irrelevant')
  return(data)
}

#' Summarize duration of detention
summarize_duration_distribution = function(movement) {
  duration_of_detention = dplyr::left_join(
      x = movement %>%
          dplyr::group_by(record_id) %>%
          dplyr::summarize(time_detention = min(time_in, na.rm = TRUE)),
      y = movement %>% 
          dplyr::group_by(record_id) %>%
          dplyr::summarize(time_discharge = max(time_out, time_discharge, na.rm = TRUE)),
      by = 'record_id') %>%
    dplyr::filter(!is.na(time_detention) & !is.na(time_discharge)) %>%
    dplyr::mutate(duration = difftime(time_discharge, time_detention, units = 'days')) %>%
    dplyr::filter(is.finite(duration)) %>%
      dplyr::pull(duration) %>%  sort()
  return(duration_of_detention)
}

#' Calculate ECDF for duration of detention
summarize_duration_quantiles = function(duration) {
  dod = duration %>% as.numeric %>% ecdf()
  dod_table = c(0.025, 0.05, 0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 
                0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.95, 0.99) %>%
    purrr::map( ~ list(percentile = .x, 
      duration_of_detention = uniroot(f = function(x) dod(x) - .x, lower = 0, upper =1580, 
        f.lower = -.x, f.upper = dod(1580) - .x)$root)) %>% 
    purrr::lift_dl(dplyr::bind_rows)()
  return(dod_table)
}

#' Summarize movement trantision data to only include unit-to-unit movement
#'
#' Excludes RCDC moves
#'
#' @param movement_transition_data full standard movement transition data
#' @param unit_table table for recoding unit names across conventions
#'
#' @return data frame of only observed full moves
summarize_movement_observed = function(movement_transition_data, unit_table) {
  per_unit = movement_transition_data %>%
    dplyr::filter(unit != 'RCDC', 
      (last_unit == 'Intake' | !is.na(last_unit)), 
      (last_stable_unit == 'Intake' | !is.na(last_stable_unit))) %>%
    dplyr::mutate(
      last_stable_unit = dplyr::case_when(
        unit == 'Intake' & is.na(last_stable_unit) ~ "external",
        is.na(last_stable_unit) ~ "unknown",
        TRUE ~ last_stable_unit),
      last_stable_building = dplyr::case_when(
        unit == 'Intake' & is.na(last_stable_unit) ~ "external",
        is.na(last_stable_unit) ~ "unknown",
        TRUE ~ last_stable_building)
    ) %>%
    dplyr::group_by(last_stable_unit, unit, last_stable_building, building) %>% 
    dplyr::summarize(count = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::right_join(
      y = unit_table %>% 
        dplyr::transmute(last_stable_unit = unit, unit = unit) %>%
        unique() %>%
        purrr::lift_dl(tidyr::expand_grid)() %>%
        dplyr::left_join(
          y = unit_table %>% 
            dplyr::transmute(
              last_stable_building = building, 
              last_stable_unit = unit) %>%
            unique(),
          by = 'last_stable_unit') %>%
        dplyr::left_join(
          y = unit_table %>%
            dplyr::transmute(building = building, unit = unit) %>%
            unique(), 
          by = 'unit'),
      by = c('last_stable_building', 'last_stable_unit', 'building', 'unit')) %>% 
    dplyr::mutate(count = dplyr::if_else(is.na(count), 0.0, as.numeric(count)))
  per_building = movement_transition_data %>%
    dplyr::filter(unit != 'RCDC', 
      (last_unit == 'Intake' | !is.na(last_unit)), 
      (last_stable_unit == 'Intake' | !is.na(last_stable_unit))) %>%
    dplyr::mutate(
      last_stable_building = dplyr::case_when(
        unit == 'Intake' & is.na(last_stable_building) ~ "external",
        is.na(last_stable_building) ~ "unknown",
        TRUE ~ last_stable_building)
    ) %>%
    dplyr::group_by(last_stable_building, building) %>%
    dplyr::summarize(count = dplyr::n()) %>%
    dplyr::ungroup()
  movement_observed = list(per_unit = per_unit, per_building = per_building)
  return(movement_observed)
}

#' Calculate movement among building from individual observations
#'
#' @param movement_observed summarized movement data
#'
#' @return data frame of building-to-building moves
summarize_building_level_moves = function(movement_observed) {
  building_mmo = movement_observed %>%
    `[[`('per_building') %>% 
    dplyr::arrange(last_stable_building, desc(count)) %>% 
    dplyr::left_join(
      y = movement_observed %>% 
        `[[`('per_building') %>% 
        dplyr::group_by(last_stable_building) %>%
        dplyr::summarize(row_sums = sum(count)) %>%
        dplyr::ungroup(),
      by = 'last_stable_building') %>%
    dplyr::mutate(
      percentage = round((count / row_sums) * 100))
  return(building_mmo)
}

#' Calculate movement among unit from individual observations
#'
#' @param movement_observed summarized movement data
#'
#' @return data frame of unit-to-unit moves
summarize_unit_level_moves = function(movement_observed, unit_match_table) {
  unit_mmo = movement_observed %>%
    `[[`('per_unit') %>% 
    dplyr::arrange(last_stable_unit, desc(count)) %>% 
    dplyr::select(-last_stable_building, -building) %>%
    dplyr::group_by(last_stable_unit, unit) %>%
    dplyr::mutate(
      count = sum(count),
    ) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(
      y = movement_observed %>%
        `[[`('per_unit') %>% 
        dplyr::group_by(last_stable_unit) %>%
        dplyr::summarize(row_sums = sum(count)) %>%
        dplyr::ungroup(),
      by = 'last_stable_unit') %>%
    dplyr::mutate(
      percentage = dplyr::case_when(
        row_sums > 0 ~ round((count / row_sums) * 100),
        row_sums == 0 ~ NA_real_)) %>%
    dplyr::left_join(
      y = unit_match_table %>% dplyr::select(unit, building), 
      by = 'unit') %>%
    dplyr::left_join(
      y = unit_match_table %>% dplyr::transmute(
        last_stable_building = building,
        last_stable_unit = unit),
      by = 'last_stable_unit')
  return(unit_mmo)
}

summarize_event_proportions = function(data) data %>%
  dplyr::filter(move_type != 'irrelevant', move_type != 'stay') %>%
  dplyr::group_by(move_type) %>% 
  dplyr::summarize(count = dplyr::n()) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(percentage = (count / sum(count)) * 100)


summarize_event_quantiles = function(data, quantile_step = 0.01) data %>% 
  dplyr::group_by(move_type) %>% 
  dplyr::summarize(
    q = seq(from = 0, to = 1, by = quantile_step), 
    percentage = glue::glue("{percent}%", percent = round(q * 100)),
    duration = quantile(duration, probs = q, na.rm=TRUE)
  )

#' Count the number of observed building-level moves
#'
#' @param building_mmo summary of building-level moves
count_building_level_moves = function(building_mmo) {
  move_counts = building_mmo %>%
    dplyr::select(-percentage) %>%
    tidyr::pivot_wider(
      names_from = building, 
      values_from = count, 
      values_fill = 0) %>% 
    dplyr::select(last_stable_building, 
      `01`, `02`, `03AX`, `04`, `06`, `08`, `09`, `10`, `11`, `15`, `16`, Intake, row_sums) %>% 
    dplyr::rename(building = last_stable_building)
  return(move_counts)
}

#' Calculate percentage of building-level moves 
#'
#' @param building_mmo summary of building-level moves
percent_building_level_moves = function(building_mmo) {
  move_percent = building_mmo %>%
    dplyr::select(-count) %>%
    tidyr::pivot_wider(
      names_from = building, 
      values_from = percentage, 
      values_fill = 0) %>% 
    dplyr::select(last_stable_building, 
      `01`, `02`, `03AX`, `04`, `06`, `08`, `09`, `10`, `11`, `15`, `16`, Intake, row_sums) %>% 
    dplyr::rename(building = last_stable_building)
  return(move_percent)
}

#' Plot unit-to-unit transition intensity matrix
#'
#' @param unit_mmo summary of unit-to-unit moves observed
#'
#' @return plot
unit_level_transitions_plot = function(
  unit_mmo
) {
  pl = ggplot() + 
    geom_raster(
      data = unit_mmo %>% 
        dplyr::filter(!(building %in% c("01", "03", "16", "05", "RCDC"))) %>%
        dplyr::mutate(
          bldg = (last_stable_building == building)
        ),
      aes(x = unit, y = last_stable_unit, alpha = bldg),
      fill = 'azure'
    ) +
    geom_raster(
      data = unit_mmo %>% 
        dplyr::filter(!(building %in% c("01", "03", "16", "05", "RCDC"))) %>%
        dplyr::mutate(
          percentage = dplyr::if_else(is.na(percentage), 0, percentage),
          percentage = dplyr::if_else(percentage == 0, NA_real_, percentage)
        ),
      aes(x = unit, y = last_stable_unit, fill = percentage)
    ) + 
    theme_minimal() +
    scale_x_discrete("current unit (index)") +
    scale_y_discrete("previous unit (index)") +
    scale_fill_continuous(na.value = '00000000') +
    theme(
      legend.position = 'none',
      axis.text.x = element_blank(), 
      axis.text.y = element_blank(), 
      panel.grid = element_blank()
    )
  return(pl)
}

select_population_files = function(files) {
  pop_files = c(
    detained = purrr::keep(files, stringr::str_detect, 
      pattern = "\\/internal-population--simulation-[0-9]+--replicate-[0-9]+--time-.*.rds"),
    released = purrr::keep(files, stringr::str_detect, 
      pattern = "\\/released-population--simulation-[0-9]+--replicate-[0-9]+--time-.*.rds")
  )
  return(pop_files)
}

select_population_summary_files = function(files) {
  pop_files = c(
    detained = purrr::keep(files, stringr::str_detect, 
      pattern = "\\/internal-population-summary--simulation-[0-9]+--replicate-[0-9]+--time-.*.rds"),
    released = purrr::keep(files, stringr::str_detect, 
      pattern = "\\/released-population-summary--simulation-[0-9]+--replicate-[0-9]+--time-.*.rds")
  )
  return(pop_files)
}

simulation_id_from_path = function(files) {
  id = files %>%
    stringr::str_extract('--simulation-[0-9]+--') %>%
    stringr::str_extract('[0-9]+')
  return(id)
}

replicate_id_from_path = function(files) {
  id = files %>%
    stringr::str_extract('--replicate-[0-9]+--') %>%
    stringr::str_extract('[0-9]+')
  return(id)
}

time_index_from_path = function(files) {
  id = files %>%
    stringr::str_extract('--time-[0-9]+--') %>%
    stringr::str_extract('[0-9]+')
  return(id)
}


plot_simulated_state_time_series = function(data, parameters, id_string) {
  pl = ggplot() +
    geom_line(
      data = data %>% dplyr::filter(infection_status == 'S'),
      aes(x = time, y = n_cumulative_new_cases, color = population),
      linetype = 2
    ) +
    geom_line(
      data = data %>% 
        dplyr::group_by(time, location, isolation, population) %>%
        dplyr::summarize(count = sum(count)) %>%
        dplyr::ungroup(),
      aes(x = time, y = count, color = population)
    ) +
    facet_wrap( ~ isolation, scales = 'free_y', nrow = 4) +
    theme_minimal() +
    ggtitle(paste0(id_string, "\n", "location: ", data$location))
  return(pl)
}

