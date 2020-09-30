library(sparklyr)
library(synapser)
library(synhelper)
library(tidyverse)

TREMOR_TABLE <- "syn12977322"
SAMPLE_SIZE <- 10
SQL_OFFSET <- 1000
STEP_PATH <- c("Tremor/Tremor/left/tremor",
               "Tremor/Tremor/right/tremor",
               "Tremor/left/tremor",
               "Tremor/right/tremor")
SENSOR_TYPE <- c("userAcceleration",
                 "rotationRate")

read_motion_file <- function(path) {
  if (is.na(path)) {
    return(NULL)
  } else {
    tryCatch({
      path %>%
        jsonlite::read_json() %>%
        dplyr::bind_rows()
    }, error = function(e) {
      return(NULL)
    })
  }
}

load_tremor_data <- function(sc) {
  sample_data <- synhelper::synGetTableFiles(
    synapse_id = TREMOR_TABLE,
    file_columns = c("left_motion.json", "right_motion.json"),
    limit = SAMPLE_SIZE,
    offset = SQL_OFFSET) %>%
    select(recordId,
           path_left = path_left_motion.json,
           path_right = path_right_motion.json)
  tremor_data <- purrr::pmap_dfr(sample_data, function(recordId, path_left, path_right) {
    left_hand <- read_motion_file(path_left)
    right_hand <- read_motion_file(path_right)
    if (is.null(left_hand) && is.null(right_hand)) {
      return(tibble())
    } else if (is.null(left_hand)) {
      right_hand <- right_hand %>%
        mutate(recordId = recordId)
      return(right_hand)
    } else if (is.null(right_hand)) {
      left_hand <- left_hand %>%
        mutate(recordId = recordId)
      return(left_hand)
    } else {
      both_hands <- bind_rows(left_hand, right_hand) %>%
        mutate(recordId = recordId)
      return(both_hands)
      }
  })
  tremor_data <- sparklyr::copy_to(sc, tremor_data)
  return(tremor_data)
}

extract_features <- function(sc) {

}

main <- function() {
  synLogin()
  sc <- sparklyr::spark_connect(master="local")
  tremor_data <- prepare_tremor_data(sc)
  features <- extract_features(sc)
}

#main()