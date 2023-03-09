
intake_simulation_figures = function(simulation_name, simulation_id, replicate_id) {
  require(simulator)
  require(ggplot2)
  require(magrittr)

  summary_dir = workflow::artifact_dir(simulation_name, simulation_id, replicate_id)
  fs::dir_create(path = summary_dir, recurse = TRUE)
 
  cohort_summary = readRDS(file = fs::path(summary_dir, "intake-cohort-summary.rds"))
  breakthrough_summary = readRDS(file = fs::path(summary_dir, "intake-breakthrough-summary.rds"))
  breakthrough_proportion = readRDS(file = fs::path(summary_dir, "intake-breakthrough-proportion.rds"))
  breakthrough_proportion_agg = readRDS(file = fs::path(summary_dir, "intake-breakthrough-proportion-agg.rds"))

  paths = character()
   
  #' Plot time-series
  pl_cohort_trace = cohort_summary %>% 
    dplyr::mutate(`infected, undetected` = n_infectious_breakthrough + n_exposed_breakthrough,
                  `infected, total` = n_infectious + n_exposed,
                  `total - isolated` = n_total - n_isolated,
                  `new` = n_infections) %>%
    pl_ts(x = cohort_age, y = c(tidyr::matches("infected, .*"), tidyr::matches("total - isolated"), new),
      xlab = "cohort age (days)", 
      ylab = "detainee count",
      vline = 14, xlim = c(0,20), ylim = c(0, 100),
      grouping = group)
 
  fig_path = fs::path(summary_dir, "cohort-trace-plots.pdf")
  pdf(file = fig_path)
  print(gridExtra::marrangeGrob(pl_cohort_trace, nrow = 2, ncol = 2))
  dev.off()
  paths = c(paths, fig_path)

  pl_overall_trace = cohort_summary %>%
    dplyr::group_by(cohort_age) %>%
    dplyr::summarize(
      simulation_id = unique(simulation_id),
      `infected, undetected` = mean(n_infectious_breakthrough + n_exposed_breakthrough),
      `infected, total` = mean(n_infectious + n_exposed),
      `isolated` = mean(n_isolated),
      `new` = mean(n_infections)) %>%
    dplyr::ungroup() %>%
    pl_ts(x = cohort_age, 
          y = c(tidyr::matches("infected, .*"), tidyr::matches("isolated"), new),
      xlab = "Cohort age (days)", 
      ylab = "Average detainee count (all cohorts)",
      vline = 14, xlim = c(0,20), ylim = c(0, max(cohort_summary$n_infectious)),
      grouping = simulation_id)
 
  fig_path = fs::path(summary_dir, "summary-trace-plot.pdf")
  pdf(file = fig_path); print(pl_overall_trace); dev.off()
  paths = c(paths, fig_path)
  fig_path = fs::path_ext_set(fig_path, 'svg')
  svg(file = fig_path); print(pl_overall_trace); dev.off()
  paths = c(paths, fig_path)
 
  pl_breakthrough_summary = ggplot() + 
    geom_bar(
      data = breakthrough_summary,
      aes(x = n_breakthrough_cases_end, y = cumulative_breakthrough_percentage),
      stat = 'identity') + 
    geom_bar(
      data = breakthrough_summary,
      aes(x = n_breakthrough_cases_end, y = breakthrough_percentage),
      stat = 'identity', fill = 'orange') +
    theme_minimal() + 
      scale_x_continuous("Breakthrough cases (count)", 
        labels = 0:10, breaks = 0:10) +
      scale_y_continuous("Percentage of cohorts (%)")
  
  fig_path = fs::path(summary_dir, "breakthrough-cases-summary.pdf")
  pdf(file = fig_path); print(pl_breakthrough_summary); dev.off()
  paths = c(paths, fig_path)
  fig_path = fs::path_ext_set(fig_path, 'svg')
  svg(file = fig_path); print(pl_breakthrough_summary); dev.off()
  paths = c(paths, fig_path)

  pl_breakthrough_pmf = ggplot() + 
    geom_bar(
      data = breakthrough_summary,
      aes(x = n_breakthrough_cases_end, y = breakthrough_percentage),
      stat = 'identity', fill = 'orange') +
    theme_minimal() + 
      scale_x_continuous("Breakthrough cases (count)", 
        labels = 0:10, breaks = 0:10) +
      scale_y_continuous("Percentage of cohorts (%)")
  
  fig_path = fs::path(summary_dir, "breakthrough-cases-pmf.pdf")
  pdf(file = fig_path); print(pl_breakthrough_pmf); dev.off()
  paths = c(paths, fig_path)
  fig_path = fs::path_ext_set(fig_path, 'svg')
  svg(file = fig_path);  print(pl_breakthrough_pmf); dev.off()
  paths = c(paths, fig_path)
    
  pl_breakthrough_cmf = ggplot() + 
    geom_bar(
      data = breakthrough_summary,
      aes(x = n_breakthrough_cases_end, y = cumulative_breakthrough_percentage),
      stat = 'identity', fill = 'orange') +
    theme_minimal() + 
      scale_x_continuous("Breakthrough cases (count)", 
        labels = 0:10, breaks = 0:10) +
      scale_y_continuous("Cumulative percentage of cohorts (%)")

  fig_path = fs::path(summary_dir, "breakthrough-cases-cmf.pdf")
  pdf(file = fig_path); print(pl_breakthrough_cmf); dev.off()
  paths = c(paths, fig_path)
  fig_path = fs::path_ext_set(fig_path, 'svg')
  svg(file = fig_path); print(pl_breakthrough_cmf); dev.off()
  paths = c(paths, fig_path)
  
  pl_individual_infection_risk = ggplot() +
    geom_line(data = cohort_summary %>% dplyr::filter((n_infectious + n_exposed + n_recovered) < n_total), 
      aes(x = cohort_age, y = 100 * p_infection, group = group), alpha = 0.1) +
    geom_vline(xintercept = 14, colour = 'grey', linetype = 2) +
    scale_x_continuous("cohort age (days)") +
    scale_y_continuous("individual infection risk (%)") +
    coord_cartesian(xlim = c(0,60)) +
    theme_minimal() +
    theme(legend.position = c(0.3, 0.9))
  
  fig_path = fs::path(summary_dir, "individual-infection-risk-plot.pdf")
  pdf(file = fig_path); print(pl_individual_infection_risk); dev.off()
  paths = c(paths, fig_path)
  fig_path = fs::path_ext_set(fig_path, 'svg')
  svg(file = fig_path); print(pl_individual_infection_risk); dev.off()
  paths = c(paths, fig_path)
  
  pl_breakthrough_case_percent = ggplot() + 
    geom_line(
      data = breakthrough_proportion_agg %>% dplyr::filter(n_breakthrough != 0),
      aes(x = cohort_age, y = (cohort_count / 365) * 100, colour = n_breakthrough)) +
    geom_vline(xintercept = 14, colour = 'grey', linetype = 2) +
    scale_x_continuous("cohort age (days)") +
    scale_y_continuous("frequency among cohorts (%)") +
    coord_cartesian(xlim = c(0, 60)) +
    theme_minimal() +
    theme(legend.position = c(0.7, 0.6))
  

  fig_path = fs::path(summary_dir, "breakthrough-cases-case-percent.pdf")
  pdf(file = fig_path); print(pl_breakthrough_case_percent); dev.off()
  paths = c(paths, fig_path)
  fig_path = fs::path_ext_set(fig_path, 'svg')
  svg(file = fig_path); print(pl_breakthrough_case_percent); dev.off()
  paths = c(paths, fig_path)

  return(paths)
}

