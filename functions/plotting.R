
pl_ts = function(
  data, x, y, 
  xlab = "cohort age (days)", 
  ylab,
  vline = 14,
  xlim = c(0, vline + 6),
  ylim = c(0, 100),
  grouping,
  ... 
) {
  x = rlang::enquo(x)
  y = rlang::enquo(y)
  data = tidyr::pivot_longer(data, !!y, names_to = "metrics")
  grouping = rlang::enquo(grouping)
  groups = dplyr::select(data, !!grouping) %>% 
    purrr::pmap(paste, sep = '--') %>%
    purrr::flatten_chr()
  pl = split(data, f = groups) %>% purrr::map( ~ 
    ggplot() +
      geom_line(data = .x, aes(x = !!x, y = value, colour = metrics)) + 
      geom_vline(xintercept = vline, colour = 'grey', linetype = 2) +
      scale_x_continuous(xlab) +
      scale_y_continuous(ylab) + 
      coord_cartesian(xlim = xlim, ylim = ylim) +
      theme_minimal() +
      theme(legend.position = c(0.3, 0.9)))
  extras = list(...)
  for (item in extras) {
    pl = purrr::map(pl, ~ .x + item)
  }
  return(pl)
}



