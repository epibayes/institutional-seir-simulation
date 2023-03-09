# Transform parameters from references to our own scales.

# Lauer 2020 (incubation time, time to symptoms from infection)
parameters = list(
  gamma = c(shape = 5.8, scale = 0.948)
)

curve(from = 0, to = 20, expr = pgamma(q = x,
  shape = parameters$gamma['shape'],
  scale = parameters$gamma['scale']))

# Ferretti 2020 (generation time)
curve(from = 0, to = 20, expr = pweibull(q = x,
  shape = 2.8,
  scale = 5.665), add = TRUE, col = 'green')


# Matching Kissler 2020
curve(from = 0, to = 30, expr = pgamma(q = x,
  shape = 5, scale = 2), add = TRUE, col = 'red')
