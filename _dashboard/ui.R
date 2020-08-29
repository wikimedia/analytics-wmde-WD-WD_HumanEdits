### ---------------------------------------------------------------------------
### --- WD_HumanEditsPerClass, v 0.0.1
### --- script: ui.R
### --- Author: Goran S. Milovanovic, Data Scientist, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### --- June 2020.
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- ETL and Analytics for the Wikidata Human Edits Per Class Project
### --- (WHEPC)
### ---------------------------------------------------------------------------
### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of Wikidata Human Edits Per Class Project (WHEPC)
### ---
### --- WHEPC is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WHEPC is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WHEPC If not, see <http://www.gnu.org/licenses/>.

### --- Setup
library(shiny)
library(shinycssloaders)

### --- shinyUI
shinyUI(
  
  fluidPage(
  
    fluidRow(
      column(width = 6,
             br(),
             img(src = 'Wikidata-logo-en.png',
                 align = "left"),
             br(), br(), br(), br(), br(), br(), br(),
             HTML('<p style="font-size:100%;"align="left"><b>Human vs Bot Edits per Class in Wikidata</b></p>'
                  ),
             htmlOutput('timestamp')
      )
    ),
    fluidRow(
      column(width = 6,
             includeHTML('html/dashboard_header.html'),
             hr()
             )
    ),
    fluidRow(
      column(width = 6,
             withSpinner(plotOutput('human_to_bot_ratio', height = 550)),
             br(),
             includeHTML('html/human_to_bot_ratio_legend.html'),
             hr()
             ),
      column(width = 6,
             withSpinner(plotOutput('median_unique_editors', height = 550)),
             br(),
             includeHTML('html/median_unique_editors_legend.html'),
             hr()
      )
    ),
    fluidRow(
      column(width = 6,
             withSpinner(plotOutput('proportion_items_touched', height = 550)),
             includeHTML('html/proportion_items_touched_legend.html')
      ),
      column(width = 6,
             withSpinner(plotOutput('total_edits_class', height = 550)),
             includeHTML('html/total_edits_class_legend.html')
      ),
      hr()
    ),
    fluidRow(
      column(width = 12,
        hr(),
        HTML('<p style="font-size:100%;"align="left"><b>Human vs Bot Edits Statistics</b></p>'),
        includeHTML('html/table_legend.html'),
        DT::dataTableOutput('overviewDT', width = "100%"),
        hr(),
        HTML('<p style="font-size:80%;"align="left"><b>Contact:</b> Goran S. Milovanovic, Data Scientist, WMDE<br><b>e-mail:</b> goran.milovanovic_ext@wikimedia.de
                          <br><b>IRC:</b> goransm</p>'),
        hr(),
        br(),
        hr()
      )
    )
  )
)









