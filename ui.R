## Dataiku/Shiny environments sometimes set HOME to a non-writable path.
## bslib/sass uses ~/.cache; ensure a writable cache location to avoid noisy warnings.
.home <- Sys.getenv("HOME", unset = "")
if (!nzchar(.home) || !dir.exists(.home) || isTRUE(file.access(.home, 2) != 0)) {
  Sys.setenv(HOME = tempdir())
}
.xdg <- Sys.getenv("XDG_CACHE_HOME", unset = "")
if (!nzchar(.xdg) || !dir.exists(.xdg) || isTRUE(file.access(.xdg, 2) != 0)) {
  Sys.setenv(XDG_CACHE_HOME = tempdir())
}

library(shiny)
library(dplyr)
library(ggplot2)
library(tidyr)
library(dataiku)
library(bslib)

ui <- page_sidebar(
  theme = bs_theme(
    version = 5,
    bootswatch = "flatly",
    base_font = font_google("Inter"),
    heading_font = font_google("Inter"),
    font_scale = 1.05
  ),
  tags$head(
    tags$title("Overall Experience Simulation"),
    tags$link(rel = "icon", type = "image/svg+xml", href = "favicon.svg"),
    tags$style(HTML('
      .custom-title {
        font-family: "Inter", "Century Gothic", "Arial", sans-serif;
        font-size: 2.2em;
        color: #1b1b1b;
        text-align: center;
        margin-top: 20px;
        margin-bottom: 18px;
        letter-spacing: 0.5px;
      }
      .mickey-ears {
        font-size: 1.2em;
        margin-right: 10px;
      }
      .plot-title {
        text-align: center;
        font-weight: 700;
        font-size: 1.1em;
        margin: 10px 0 8px 0;
        font-family: Inter, "Century Gothic", Arial, sans-serif;
      }

      /* Slightly larger labels + better focus visibility */
      .form-label { font-weight: 600; }
      :focus { outline: 3px solid rgba(13,110,253,.35); outline-offset: 2px; }

      /* Prevent plot widgets from overlapping subsequent rows */
      .plot-card,
      .plot-card .card-body {
        overflow: hidden;
      }
      .plot-card { position: relative; }
    '))
  ),
  title = div(
    class = "custom-title",
    span(class = "mickey-ears", "\U0001F42D"),
    "Overall Experience Simulation",
    span(class = "mickey-ears", "\U0001F42D")
  ),
  sidebar = sidebar(
    width = 360,
    h4("Simulation setup"),
    helpText("Tip: hover labels for explanations."),

    tagList(
      bslib::tooltip(
        selectInput(
          "selected_park",
          "Park",
          choices = c("Magic Kingdom" = 1, "EPCOT" = 2, "Hollywood Studios" = 3, "Animal Kingdom" = 4),
          selected = 1
        ),
        "Choose which park to simulate."
      ),

      bslib::tooltip(
        sliderInput(
          "n_runs",
          label = "Simulation runs",
          min = 1,
          max = 100,
          value = 5,
          step = 1
        ),
        "More runs = more stability, but longer runtime."
      ),

      h5("Experiences"),
      uiOutput("exp_select_ui"),
      uiOutput("selected_exp_dates"),

      h5("Progress"),
      uiOutput("sim_status_ui"),

      actionButton("simulate", "Run simulation", class = "btn-primary w-100")
    )
  ),
  navset_card_tab(
    nav_panel(
      "Summary",
      uiOutput("summary_boxes"),
      br(),
      uiOutput("summary_text")
    ),
    nav_panel(
      "Plots",
      div(class = "plot-title", "Overall Experience Impact (bootstrap 95% CI)"),
      bslib::layout_column_wrap(
        width = 1 / 3,
        card(class = "plot-card", uiOutput("boxplot_park_ui")),
        card(class = "plot-card", uiOutput("boxplot_lifestage_ui")),
        card(class = "plot-card", uiOutput("boxplot_genre_ui"))
      ),
      div(style = "height: 18px;"),
      # Row 3: cannibalization (full width)
      div(class = "plot-title", "Cannibalization (ordered by Actuals)"),
      card(class = "plot-card", uiOutput("histplot_ui"))
    ),
    nav_panel(
      "Downloads",
      h5("Download simulation results"),
      helpText("Downloads include the per-run simulation output with Actual vs Simulation EARS and Incremental_EARS."),
      downloadButton("download_sim", "Download full results (CSV)", class = "btn-outline-secondary"),
      br()
    )
  ),
  tags$footer(
    class = "text-muted",
    style = "margin: 16px 0 6px 0; font-size: 0.95rem;",
    tags$hr(),
    div(
      "Need help? See internal documentation or contact the owning analytics team."
    )
  )
)

# End of file