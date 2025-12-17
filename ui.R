library(shiny)
library(dplyr)
library(ggplot2)
library(tidyr)
library(dataiku)
library(bslib)




exp_data <- dkuReadDataset("MetaDataTool_Exp")

ui <- fluidPage(
  theme = bs_theme(
    version = 5,
    bootswatch = "flatly",
    base_font = font_google("Inter"),
    heading_font = font_google("Inter")
  ),
  tags$head(
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
      .sidebar .well { background: #ffffff; border-radius: 12px; border: 1px solid rgba(0,0,0,0.08); }
      .main-panel { background: #fff; border-radius: 12px; }
      .plot-title {
        text-align: center;
        font-weight: 700;
        font-size: 1.1em;
        margin: 10px 0 8px 0;
        font-family: Inter, "Century Gothic", Arial, sans-serif;
      }
    '))
  ),
  div(
    class = "custom-title",
  span(class = "mickey-ears", "\U0001F42D"),
  "Overall Experience Simulation",
  span(class = "mickey-ears", "\U0001F42D")
  ),
  sidebarLayout(
    sidebarPanel(
      width = 3,
      selectInput(
        "selected_park",
        "Select Park",
        choices = c("Magic Kingdom" = 1, "EPCOT" = 2, "Hollywood Studios" = 3, "Animal Kingdom" = 4),
        selected = 1
      ),
      numericInput(
        "n_runs",
        label = HTML("<b>Simulation runs</b><br><span style='font-size:0.9em; color:#6c757d;'>Each run is approximately ~1 minute.</span>"),
        value = 5,
        min = 1,
        max = 100,
        step = 1
      ),
      uiOutput("exp_select_ui"),
      uiOutput("selected_exp_dates"),
      actionButton("simulate", "Run simulation", class = "btn-primary w-100")
    ),
    mainPanel(
  div(class = "plot-title", 'Overall Experience Impact (bootstrap 95% CI)'),
      fluidRow(
        column(4, plotOutput("boxplot_park", height = 250)),
        column(4, plotOutput("boxplot_lifestage", height = 250)),
        column(4, plotOutput("boxplot_genre", height = 250))
      ),
        div(class = "plot-title", 'Cannibalization (ordered by Actuals)'),
      plotOutput("histplot", height = 300),
              downloadButton("download_sim", "Download simulation results", class = "btn-outline-secondary")
    )
  )
)


