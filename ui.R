library(shiny)
library(dplyr)
library(ggplot2)
library(tidyr)
library(dataiku)




exp_data <- dkuReadDataset("MetaDataTool_Exp")

ui <- fluidPage(
  tags$head(
    tags$style(HTML('
      .custom-title {
        font-family: "Century Gothic", "Arial", sans-serif;
        font-size: 2.5em;
        color: #3b3b3b;
        text-align: center;
        margin-top: 20px;
        margin-bottom: 10px;
        letter-spacing: 2px;
      }
      .mickey-ears {
        font-size: 1.2em;
        margin-right: 10px;
      }
      .sidebar .well { background: #f7f7f7; border-radius: 10px; }
      .main-panel { background: #fff; border-radius: 10px; }
    '))
  ),
  div(
    class = "custom-title",
  span(class = "mickey-ears", "\U0001F42D"),
  "Overall Experience Simulation",
  span(class = "mickey-ears", "\U0001F42D")
  ),
  sidebarLayout(
    sidebarPanel(selectInput(
  "selected_park",
  "Select Park",
  choices = c("Magic Kingdom" = 1, "EPCOT" = 2, "Hollywood Studios" = 3, "Animal Kingdom" = 4),
  selected = 1
),
                 numericInput(
  "n_runs",
  label = HTML("<b>Simulation Runs</b> <br><span style='font-size:0.9em; color:#888;'>Each run is approximately 1 min long.</span>"),
  value = 5,
  min = 1,
  max = 100,
  step = 1
),
  uiOutput("exp_select_ui"),
  uiOutput("selected_exp_dates"),
  actionButton("simulate", "Simulate")
),
    mainPanel(
  div(style = 'text-align:center; font-weight:bold; font-size:1.3em; margin-bottom: 5px; margin-top: 15px; font-family: Century Gothic, Arial, sans-serif;', 'Overall Experience Impact (per Sim Run)'),
      fluidRow(
        column(4, plotOutput("boxplot_park", height = 250)),
        column(4, plotOutput("boxplot_lifestage", height = 250)),
        column(4, plotOutput("boxplot_genre", height = 250))
      ),
        div(style = 'text-align:center; font-weight:bold; font-size:1.3em; margin-bottom: 5px; margin-top: 15px; font-family: Century Gothic, Arial, sans-serif;', 'Cannibalization (ordered by Actuals)'),
      plotOutput("histplot", height = 300),
              downloadButton("download_sim", "Download Simulation Results")
    )
  )
)


