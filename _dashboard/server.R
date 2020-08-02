### ---------------------------------------------------------------------------
### --- WD_HumanEditsPerClass, v 0.0.1
### --- script: server.R
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
library(XML)
library(curl)
library(tidyverse)
library(data.table)
library(ggrepel)
library(scales)
library(DT)

### --- publicDir
publicDir <- 
  'https://analytics.wikimedia.org/published/datasets/wmde-analytics-engineering/Wikidata/WD_HumanEdits/'

### --- pars params
params <- xmlParse("wdHumanEditsPerClass_Config.xml")
params <- xmlToList(params)

### --- functions
get_WDCM_table <- function(url_dir, filename, row_names) {
  read.csv(paste0(url_dir, filename), 
           header = T, 
           stringsAsFactors = F,
           check.names = F)
}

# - get update stamp:
h <- new_handle()
handle_setopt(h,
              copypostfields = "WD_humanEditsPerClass");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
timestamp <- 
  curl_fetch_memory(publicDir)
timestamp <- rawToChar(timestamp$content)
timestamp <- str_extract_all(timestamp, "[[:digit:]]+.+[[:digit:]]+")[[1]][3]
timestamp <- gsub("<.+", "", timestamp)
timestamp <- trimws(timestamp, which = "right")
timestamp <- paste0("Updated: ", timestamp, " UTC")

### --- Serve table

### --- shinyServer
shinyServer(function(input, output, session) {
  
  # - get current data file:
  filename <- 'WD_HumanEdits.csv'
  withProgress(message = 'Downloading data', detail = "Please be patient.", value = 0, {
    dataSet <- get_WDCM_table(publicDir, filename)
    dataSet[, 1] <- NULL
    dataSet$wd_class <- gsub('""', '"', dataSet$wd_class) 
  })
  
  ### --- timestamp
  output$timestamp <- renderText({
    paste0('<p style="font-size:90%;"align="left"><b>',
           timestamp, '</b></p>'
    )
  })
  
  ### --- wmf.wikidata_entity timestamp
  output$wikidata_entity_timestamp <- renderText({
    paste0('<p style="font-size:80%;"align="left">',
           'The currently used version of the wmf.wikidata_entity (Wikidata JSON dump) table: <b>',
           params$general$wikidataEntitySnapshot, '</b></p>'
    )
  })
  
  ### --- wmf.mediawiki_history timestamp
  output$mediawiki_history_timestamp <- renderText({
    paste0('<p style="font-size:80%;"align="left">',
           'The currently used version of the wmf.mediawiki_history table: <b>',
           params$general$mwwikiSnapshot, '</b></p>'
    )
  })
  
  ### --- TABLE
  ### --- output$overviewDT
  output$overviewDT <- DT::renderDataTable({
    dSet <- dataSet
    colnames(dSet) <- c('Class', 'Label', 'Items', 'Human Edited Items', 
                        '% Items Touched', 'Median Unique Editors', 'Human Edits', 
                        'Bot Edits', 'Total Edits', 'Human/Bot Edits Ratio', 
                        '% Human Edits', '% Bot Edits')
    DT::datatable(dSet, 
              options = list(
                escape = F,
                pageLength = 100,
                width = '100%',
                columnDefs = list(list(className = 'dt-left', targets = "_all"))
              ),
              rownames = FALSE,
              escape = F
    )
  }, 
  server = T)
  
  ### --- human_to_bot_ratio
  output$human_to_bot_ratio <- renderPlot({
    # - distribution: human_to_bot_ratio
    ggplot(dataSet, 
           aes(x = log(human_to_bot_ratio))) +
      geom_density(color = "darkblue", 
                   fill = "lightblue", 
                   size = .05, 
                   alpha = .5) + 
      geom_vline(xintercept = 0, size = .15, linetype = "dashed") + 
      xlab("log(Human to Bot Edits Ratio)") + 
      ggtitle(label = "Human to Bots Edit Ratio (per Class) in Wikidata", 
              subtitle = "Note: Classes with 0 bot edits are not accounted for (Ratio = Infinite).") + 
      theme_bw() + 
      theme(panel.border = element_blank()) + 
      theme(plot.title = element_text(hjust = 0.5, size = 14)) + 
      theme(plot.subtitle = element_text(hjust = 0.5, size = 11))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- median_unique_editors
  output$median_unique_editors <- renderPlot({
    histUnique <- hist(dataSet$median_unique_editors, 
                       plot = F)
    bins <- numeric()
    for (i in 2:length(histUnique$breaks)) {
      bins[i-1] <- paste0("(", histUnique$breaks[i-1], 
                          " - ",
                          histUnique$breaks[i], ")")
    }
    histUnique <- data.frame(mids = histUnique$mids, 
                             counts = histUnique$counts, 
                             bins = paste0(bins, ": ", histUnique$counts), 
                             stringsAsFactors = F)
    histUnique$bins <- ifelse(histUnique$counts == 0, 
                              "",
                              histUnique$bins)
    histUnique$counts[histUnique$counts == 0] <- NA
    ggplot(histUnique, 
           aes(x = log(mids), 
               y = log(counts),
               label = bins)) +
      geom_smooth(method = "lm", 
                  size = .1,
                  color = "darkred",
                  linetype = "dashed") + 
      geom_path(size = .15, color = "red") + 
      geom_point(size = 2, color = "red") + 
      geom_point(size = 1.5, color = "white") + 
      geom_text_repel(size = 3.5, min.segment.length = 10) + 
      xlab("log(Median Unique Human Editors per Class)") + 
      ggtitle("Median Unique Human Editors (per Class) in Wikidata") + 
      theme_bw() + 
      theme(panel.border = element_blank()) + 
      theme(plot.title = element_text(hjust = 0.5, size = 14)) + 
      theme(axis.text.x = element_text(size = 8, 
                                       hjust = 0.95, 
                                       vjust = 0.2))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- proportion of items touched
  output$proportion_items_touched <- renderPlot({
    ggplot(dataSet, 
           aes(x = percent_items_touched/100)) +
      geom_density(color = "darkorange", 
                   fill = "orange", 
                   size = .15, 
                   alpha = .5) + 
      xlab("Proportion") + 
      ggtitle("Proportion of Items Ever Touched by Human Editors (per Class) in Wikidata") + 
      theme_bw() + 
      theme(panel.border = element_blank()) + 
      theme(plot.title = element_text(hjust = 0.5, size = 14))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- total number of edits per class
  output$total_edits_class <- renderPlot({
    # - distribution: total number of edits
    histUnique <- hist(dataSet$total_edits,
                       breaks = c(0, 1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 100, 
                                  200, 300, 400, 500, 
                                  1000, 2000, 3000, 4000, 5000, 
                                  1e4, 2e4, 3e4, 4e4, 5e4, 1e5, 
                                  5e5, 1e6, 2e6, 3e6 , max(dataSet$total_edits)),
                       plot = F)
    bins <- numeric()
    for (i in 2:length(histUnique$breaks)) {
      bins[i-1] <- paste0(histUnique$breaks[i-1], 
                          " - ",
                          histUnique$breaks[i])
    }
    histUnique <- data.frame(mids = histUnique$mids, 
                             counts = histUnique$counts, 
                             bins = bins, 
                             stringsAsFactors = F)
    histUnique$bins <- ifelse(histUnique$counts == 0, 
                              "",
                              histUnique$bins)
    histUnique$counts[histUnique$counts == 0] <- NA
    ggplot(histUnique, 
           aes(x = log(mids), 
               y = counts,
               label = counts)) +
      geom_path(size = .15, color = "darkred") + 
      geom_point(size = 2, color = "darkred") + 
      geom_point(size = 1.5, color = "white") +
      geom_text_repel(size = 3.5) + 
      scale_x_continuous(breaks = log(histUnique$mids),
                         labels = histUnique$bins) +
      xlab("Edits Range") + 
      ylab("Classes") + 
      ggtitle("Total Edits per Wikidata Class") + 
      theme_bw() + 
      theme(panel.border = element_blank()) + 
      theme(plot.title = element_text(hjust = 0.5, size = 14)) + 
      theme(axis.text.x = element_text(angle = 90, 
                                       size = 8, 
                                       hjust = 0.95, 
                                       vjust = 0.2))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
}) # - END Shiny Server
  
  







