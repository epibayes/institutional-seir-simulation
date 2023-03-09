
#' Cumulative window function
#'
#' Apply f to each overlapping group of m values in x
#'
#' @param x values to transform
#' @param m window size
#' @param f function to apply to moving window
#'
#' @return result of f applied to each set of values where
#'         the value at i is for the window up to i
windowed = function(x, m, f) {
  n = length(x)
  o = rep(NA_real_,  n)
  if (n < m) {
    return(o)
  } 
  for (i in m:n) {
    j = i - m + 1
    o[i] = f(x[j:i])
  }
  return(o)
}
    
    
#' Find lag required to find an x not in exclu
#'
#' @param x vector to check for values
#' @param exclude vector of values to exclude
#' @return lag for each entry to previous value not in 'exclude'
which_lag = function(x, .p)  {
  .p = rlang::enquo(.p)
  m = length(x)
  o = vector(mode = 'numeric', length = m)
  o[1] = NA
  if (m == 1) {
    return(o)
  }
  e = rlang::new_environment()
  for (i in 2:m) {
    o[i] = NA
    for (j in (i-1):1) {
      rlang::env_bind(e, .x = x[j])
      accept = rlang::eval_tidy(expr = .p, data = rlang::new_data_mask(e))
      if (accept) {
        o[i] = i - j
        break
      }
    }
  }
  return(o)
}

#' Find lead required to find an x not in exclude
#'
#' @param x vector to check for values
#' @param exclude vector of values to exclude
#' @return lead for each entry to previous value not in 'exclude'
which_lead = function(x, .p)  {
  .p = rlang::enquo(.p)
  m = length(x)
  o = vector(mode = 'numeric', length = m)
  o[m] = NA
  if (m == 1) {
    return(o)
  }
  e = rlang::new_environment()
  for (i in (m-1):1) {
    o[i] = NA
    for (j in (i+1):m) {
      rlang::env_bind(e, .x = x[j])
      accept = rlang::eval_tidy(expr = .p, data = rlang::new_data_mask(e))
      if (accept) {
        o[i] = j - i
        break
      }
    }
  }
  return(o)
}

lag = function(x, .p) {
  o = x
  .p = rlang::enquo(.p)
  .l = which_lag(x, !!.p)
  for (i in seq_along(x)) {
    if (is.na(.l[i])) {
      o[i] = NA
    } else {
      o[i] = x[i - .l[i]]
    }
  }
  return(o)
}

lead = function(x, .p) {
  o = x
  .p = rlang::enquo(.p)
  .l = which_lead(x, !!.p)
  for (i in seq_along(x)) {
    if (is.na(.l[i])) {
      o[i] = NA
    } else {
      o[i] = x[i + .l[i]]
    }
  }
  return(o)
}

lag_friend = function(x, y, .p) {
  o = x
  .p = rlang::enquo(.p)
  .l = which_lag(y, !!.p)
  for (i in seq_along(x)) {
    if (is.na(.l[i])) {
      o[i] = NA
    } else {
      o[i] = x[i - .l[i]]
    }
  }
  return(o)
}

lead_friend = function(x, y, .p, .offset) {
  o = x
  .p = rlang::enquo(.p)
  .l = which_lead(y, !!.p)
  for (i in seq_along(x)) {
    if (is.na(.l[i])) {
      o[i] = NA
    } else {
      o[i] = x[i - .l[i] + .offset] 
    }
  }
  return(o)
}

simulate_first_event = function(parameters) {
  o = list(event_type = 'intake', duration = rexp(n = 1, parameters[['lambda[1]']]))
  return(o)
}

simulate_next_event = function(parameters, time_fence = NA) {
  times = c(
    t_stay = rexp(n = 1, rate = parameters[['lambda[2]']]),
    t_rejoin_unit = rexp(n = 1, rate = parameters[['lambda[3]']]),
    t_switch_unit = rexp(n = 1, rate = parameters[['lambda[4]']]),
    t_switch_building = rexp(n = 1, rate = parameters[['lambda[5]']]),
    t_exit = rexp(n = 1, rate = parameters[['lambda[6]']])
  )
  labels = c('stay', 'rejoin_unit', 'switch_unit', 'switch_building', 'exit')
  if (!is.na(time_fence)) {
    times['t_stay'] = time_fence
  }
  first = which.min(times)
  o = list(event_type = labels[first], duration = times[first])
  return(o)
}

simulate_events = function(parameters) {
  o = list(simulate_first_event(parameters))
  while(o[[length(o)]][['event_type']] != 'exit') {
    o = c(o, list(simulate_next_event(parameters)))
  }
  return(o)
}


event_coding_table = function() {
  o = tibble::tibble(
    event_type = c('intake', 'stay', 'rejoin_unit', 'switch_unit', 'switch_building', 'exit')
  ) %>% dplyr::mutate(
    event_type_int = 1:dplyr::n()
  )
  return(o)
}


elide_record_stays = function(data) {
  if (nrow(data) <= 1) {
    return(data)
  }
  stay_idx = which(data[['move_type']] == 'stay')
  if (length(stay_idx) == 0) {
    return(data)
  }
  will_stay_idx = stay_idx[stay_idx != 1] - 1
  will_stay_idx = will_stay_idx[!(will_stay_idx %in% stay_idx)]
  rows = 1:nrow(data)
  lead_idx = rows %>% 
    lead_friend(data[['move_type']], !stringr::str_detect(.x, 'stay'), -1)
  for (idx in will_stay_idx) {
    if (is.na(lead_idx[idx])) {
      next
    } else {
      data[idx,'exit_timestamp'] = data[lead_idx[idx], 'exit_timestamp']
    }
  }
  data = data %>% dplyr::filter(move_type != 'stay')
  data = elide_record_stays(data)
  return(data)
}


elide_stays = function(data) {
  data = data %>% 
    dplyr::group_split(record_id) %>% 
    purrr::map(elide_record_stays) %>%
    purrr::lift_dl(dplyr::bind_rows)()
  return(data)
}

save_upload_artifact = function(x, name, dir, remote_dir) {
  path = fs::path(dir, name)
  saveRDS(x, file = path)
  remote_path = fs::path(remote_dir, name)
  rdrop2::drop_upload(file = path, path = remote_path)
  return(path)
}

make_id_string = function(parameters) {
  s = glue::glue("simulation name: {name}\n",
                 "simulation id: {sim_id}\n",
                  "replicate id: {rep_id}",
    name = parameters[[1]]$root$simulation_name,
    sim_id = parameters[[1]]$root$simulation_id,
    rep_id = parameters[[1]]$root$replicate_id)
  return(s)
}




