# Load necessary libraries
library(shiny)
library(leaflet)
library(sf)
library(tidyverse)
library(RColorBrewer)
library(paletteer)

# setwd("random-requests/code/new-phytologist-maps/")

# Define UI for the Shiny app
ui <- fluidPage(
  titlePanel("Herbarium/iNat Ratio Map"),
  sidebarLayout(
    sidebarPanel(
      # p("This app maps the herbarium_ratio data on top of OpenStreetMaps."),
      sliderInput("opacity", "Polygon Opacity:",
        min = 0, max = 1, value = 0.7, step = 0.1
      ),
      selectInput("resolution", "Spatial Resolution:",
        choices = c( # "0.01 degrees" = ".01",
          "0.05 degrees" = ".05",
          "0.1 degrees" = ".1",
          "0.25 degrees" = ".25"
        ),
        selected = ".1"
      )
    ),
    mainPanel(
      leafletOutput("herbariumMap", width = "100%", height = "90vh")
    )
  )
)

# Define server logic for the Shiny app
server <- function(input, output, session) {
  # Reactive expression to load the appropriate file based on the selected resolution
  herbarium_data <- reactive({
    file_name <- switch(input$resolution,
      # ".01" = "herbrat_0_01heatmap.gpkg",
      ".05" = "herbrat_0_05heatmap.gpkg",
      ".1" = "herbrat_0_1heatmap.gpkg",
      ".25" = "herbrat_0_25heatmap.gpkg"
    )
    st_read(file_name) %>%
      mutate(
        log_herbarium_iNat_ratio = log10(herbarium_iNat_ratio),
        log_observation_cnt = log10(observation_cnt),
        log_specimen_cnt = log10(specimen_cnt)
      ) #%>%
      # filter(is.finite(log_herbarium_iNat_ratio))
  })


  # Render the initial leaflet map
  output$herbariumMap <- renderLeaflet({
    leaflet() %>%
      addProviderTiles(providers$OpenStreetMap) %>%
      setView(lng = -119.4179, lat = 36.7783, zoom = 5.5)
  })

  # Observe changes in the opacity slider and the spatial resolution slider and update the map
  observe({
    # browser()
    palette <- colorNumeric(
      palette = rev(brewer.pal(11, "RdBu")), # Reverse the RdBu palette for correct divergent effect
      # palette = colorRampPalette(c(
      #   "#A50021FF", "#D82632FF", "#F76D5EFF", "#FFAD72FF", "#FFE099FF",
      #   "#FFFFBFFF", "#E0FFFFFF", "#AAF7FFFF", "#72D8FFFF", "#3FA0FFFF", "#264CFFFF"
      # ))(100),
      domain = range(
        herbarium_data() %>%
          filter(is.finite(log_herbarium_iNat_ratio)) %>%
          pull(log_herbarium_iNat_ratio),
        na.rm = TRUE, finite = T
      )
    )

    inatPal <- colorNumeric(
      palette = colorRampPalette(c("#A50021FF", "#FFE099FF"))(100),
      domain = range(herbarium_data()$log_observation_cnt, na.rm = TRUE, finite = T)
    )
    # D2FBD4FF, #A5DBC2FF, #7BBCB0FF, #559C9EFF, #3A7C89FF, #235D72FF, #123F5AFF
    herbPal <- colorNumeric(
      palette = colorRampPalette(
        # c("#A50021FF", "#FFE099FF")
        c("#123F5AFF", "#D2FBD4FF")
      )(100),
      domain = range(herbarium_data()$log_specimen_cnt, na.rm = TRUE, finite = T)
    )

    # print(herbarium_data() %>% names())
    leafletProxy("herbariumMap") %>%
      clearShapes() %>%
      clearControls() %>%
      # hideGroup("Ratio") %>% 
      hideGroup("iNat Count") %>% 
      hideGroup("Herbarium Count") %>% 

      # Ratio group
      addPolygons(
        data = herbarium_data(),
        fillColor = ~ palette(herbarium_data()$log_herbarium_iNat_ratio),
        # fillColor = ~ palette(log_herbarium_iNat_ratio),
        weight = 0,
        opacity = 1,
        color = "white",
        dashArray = "3",
        fillOpacity = input$opacity,
        highlightOptions = highlightOptions(
          # weight = 5,
          color = "#666",
          dashArray = "",
          fillOpacity = 0.7,
          bringToFront = TRUE
        ),
        label = ~ paste(herbarium_data()$herbarium_iNat_ratio),
        labelOptions = labelOptions(
          style = list("font-weight" = "normal", padding = "3px 8px"),
          textsize = "15px",
          direction = "auto"
        ),
        group = "Ratio"
      ) %>%
      # addLegend(
      #   pal = palette,
      #   values = herbarium_data()$log_herbarium_iNat_ratio,
      #   title = "Herbarium / iNat Ratio",
      #   position = "bottomright",
      #   labFormat = labelFormat(transform = function(x) 10^x),
      #   group = "Ratio"
      # ) %>% 

      # iNat Count
      addPolygons(
        data = herbarium_data(),
        fillColor = ~ inatPal(herbarium_data()$log_observation_cnt),
        weight = 0,
        opacity = 1,
        color = "white",
        dashArray = "3",
        fillOpacity = input$opacity,
        highlightOptions = highlightOptions(
          # weight = 5,
          color = "#666",
          dashArray = "",
          fillOpacity = 0.7,
          bringToFront = TRUE
        ),
        label = ~ paste(herbarium_data()$observation_cnt),
        labelOptions = labelOptions(
          style = list("font-weight" = "normal", padding = "3px 8px"),
          textsize = "15px",
          direction = "auto"
        ),
        group = "iNat Count"
      ) %>%
      # addLegend(
      #   pal = inatPal,
      #   values = herbarium_data()$log_observation_cnt,
      #   title = "iNat Count",
      #   position = "bottomright",
      #   labFormat = labelFormat(transform = function(x) 10^x),
      #   group = "iNat Count"
      # ) %>% 

      # Herbarium Count
      addPolygons(
        data = herbarium_data(),
        fillColor = ~ herbPal(herbarium_data()$log_specimen_cnt),
        weight = 0,
        opacity = 1,
        color = "white",
        dashArray = "3",
        fillOpacity = input$opacity,
        highlightOptions = highlightOptions(
          # weight = 5,
          color = "#666",
          dashArray = "",
          fillOpacity = 0.7,
          bringToFront = TRUE
        ),
        label = ~ paste(herbarium_data()$specimen_cnt),
        labelOptions = labelOptions(
          style = list("font-weight" = "normal", padding = "3px 8px"),
          textsize = "15px",
          direction = "auto"
        ),
        group = "Herbarium Count"
      ) %>%
      # addLegend(
      #   pal = herbPal,
      #   values = herbarium_data()$log_specimen_cnt,
      #   title = "Herbarium Count",
      #   position = "bottomright",
      #   labFormat = labelFormat(transform = function(x) 10^x),
      #   group = "Herbarium Count"
      # ) %>%

      addLayersControl(
        baseGroups = c("Ratio", "iNat Count", "Herbarium Count"),
        options = layersControlOptions(collapsed = FALSE)
      )

    
  })


  observeEvent(input$herbariumMap_groups,{
    my_map <- leafletProxy("herbariumMap") %>% clearControls()

    palette <- colorNumeric(
      palette = rev(brewer.pal(11, "RdBu")), # Reverse the RdBu palette for correct 
      domain = range(herbarium_data()$log_herbarium_iNat_ratio, 
                     na.rm = TRUE, finite = T)
    )

    inatPal <- colorNumeric(
      palette = colorRampPalette(c("#A50021FF", "#FFE099FF"))(100),
      domain = range(herbarium_data()$log_observation_cnt, na.rm = TRUE, finite = T)
    )

    herbPal <- colorNumeric(
      palette = colorRampPalette(
        c("#123F5AFF", "#D2FBD4FF")
      )(100),
      domain = range(herbarium_data()$log_specimen_cnt, na.rm = TRUE, finite = T)
    )

    # Fix legends
    if (input$herbariumMap_groups == "Ratio") {
      my_map <- my_map %>%
        addLegend(
          pal = palette,
          # values = herbarium_data()$log_herbarium_iNat_ratio,
          values = range(herbarium_data()$log_herbarium_iNat_ratio, 
                         na.rm = TRUE, finite = T),
          title = "Herbarium / iNat Ratio",
          position = "bottomright",
          labFormat = labelFormat(transform = function(x) 10^x),
          group = "Ratio"
        )
    } else if (input$herbariumMap_groups == "iNat Count") {
      my_map <- my_map %>%
        addLegend(
          pal = inatPal,
          # values = herbarium_data()$log_observation_cnt,
          values = range(herbarium_data()$log_observation_cnt, 
                         na.rm = TRUE, finite = T),
          title = "iNat Count",
          position = "bottomright",
          labFormat = labelFormat(transform = function(x) 10^x),
          group = "iNat Count"
        )
    } else if (input$herbariumMap_groups == "Herbarium Count") {
      my_map <- my_map %>%
        addLegend(
          pal = herbPal,
          # values = herbarium_data()$log_specimen_cnt,
          values = range(herbarium_data()$log_specimen_cnt, 
                         na.rm = TRUE, finite = T),
          title = "Herbarium Count",
          position = "bottomright",
          labFormat = labelFormat(transform = function(x) 10^x),
          group = "Herbarium Count"
        )
    }
  })
}

# Run the application
shinyApp(ui = ui, server = server)
