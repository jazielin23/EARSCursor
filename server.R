# app.R (Dataiku Shiny WebApp)
 
library(shiny)
library(dataiku)
library(dplyr)
library(ggplot2)
library(tidyr)
library(scales)
library(bslib)
 
## -----------------------------------------------------------------------------
## Embedded simulation runner (ported from Sim.R)
## -----------------------------------------------------------------------------
## This is the same foreach/doParallel simulation logic as Sim.R, but embedded
## directly in server.R so the Shiny runtime does not need to source external files.
run_simulation_simR_embedded <- function(
    n_runs = 10L,
    num_cores = 5L,
    yearauto = 2024L,
    park_for_sim = 1L,
    exp_name = c("tron"),
    exp_date_ranges = list(tron = c("2023-10-11", "2025-09-02")),
    maxFQ = 4L,
    verbose = FALSE
) {
  suppressPackageStartupMessages({
    library(dataiku)
    library(foreach)
    library(doParallel)
    library(nnet)
    library(gtools)
    library(data.table)
    library(dplyr)
    library(sqldf)
    library(reshape2)
    library(reshape)
  })

  # Read inputs (same as Sim.R)
  meta_prepared_new <- dkuReadDataset("MetaDataFinalTaxonomy")
  AttQTR_new <- dkuReadDataset("Attendance")
  QTRLY_GC_new <- dkuReadDataset("GuestCarriedFinal")
  SurveyDataCheck_new <- dkuReadDataset("FY24_prepared")
  POG_new <- dkuReadDataset("Charcter_Entertainment_POG")
  SurveyData_new <- dkuReadDataset("FY24_prepared")

  cl <- makeCluster(as.integer(num_cores))
  on.exit({
    try(stopCluster(cl), silent = TRUE)
  }, add = TRUE)
  registerDoParallel(cl)

  # Parallel loop (same as Sim.R)
  EARSTotal_list <- foreach(
    run = 1:as.integer(n_runs),
    .combine = rbind,
    .packages = c("dataiku", "nnet", "sqldf", "data.table", "dplyr", "reshape2", "reshape"),
    .export = c("meta_prepared_new", "AttQTR_new", "QTRLY_GC_new", "SurveyDataCheck_new", "POG_new", "SurveyData_new",
                "yearauto", "park_for_sim", "exp_name", "exp_date_ranges", "maxFQ", "verbose")
  ) %dopar% {
    EARSFinal_Final <- c()
    EARSx <- c()
    EARSTotal <- c()

    SurveyData <- SurveyData_new
    names(SurveyData) <- tolower(names(SurveyData))
    # Explicit LifeStage mapping (keeps all other values unchanged)
    SurveyData$newgroup <- SurveyData$newgroup1
    map_from <- c(4, 5, 6, 7)
    map_to <- c(3, 4, 5, 5)
    m <- match(SurveyData$newgroup, map_from)
    SurveyData$newgroup[!is.na(m)] <- map_to[m[!is.na(m)]]
    SurveyData_new <- SurveyData

    # --- Monte Carlo simulation: create SurveyData_copy ---
    meta_prepared <- meta_prepared_new
    park <- as.integer(park_for_sim)

    for (name in exp_name) {
      matched_row <- meta_prepared[meta_prepared$name == name & meta_prepared$Park == park, ]
      exp_ride_col <- matched_row$Variable
      exp_group_col <- matched_row$SPEC

      ride_exists <- exp_ride_col %in% colnames(SurveyData)
      group_exists <- exp_group_col %in% colnames(SurveyData)

      if (ride_exists) {
        segments <- unique(SurveyData$newgroup[SurveyData$park == park])
        date_range <- exp_date_ranges[[name]]
        for (seg in segments) {
          seg_idx <- which(
            SurveyData$park == park &
              SurveyData$newgroup == seg &
              SurveyData$visdate_parsed >= as.Date(date_range[1]) &
              SurveyData$visdate_parsed <= as.Date(date_range[2])
          )
          seg_data <- SurveyData[seg_idx, ]
          if (group_exists) {
            for (wanted in c(1, 0)) {
              to_replace_idx <- which(!is.na(seg_data[[exp_ride_col]]) & seg_data[[exp_group_col]] == wanted)
              pool_idx <- which(is.na(seg_data[[exp_ride_col]]) & seg_data[[exp_group_col]] == wanted)
              if (length(to_replace_idx) > 0 && length(pool_idx) > 0) {
                sampled_rows <- seg_data[sample(pool_idx, size = length(to_replace_idx), replace = TRUE), , drop = FALSE]
                SurveyData[seg_idx[to_replace_idx], ] <- sampled_rows
              }
            }
          } else {
            to_replace_idx <- which(!is.na(seg_data[[exp_ride_col]]))
            pool_idx <- which(is.na(seg_data[[exp_ride_col]]))
            if (length(to_replace_idx) > 0 && length(pool_idx) > 0) {
              sampled_rows <- seg_data[sample(pool_idx, size = length(to_replace_idx), replace = TRUE), , drop = FALSE]
              SurveyData[seg_idx[to_replace_idx], ] <- sampled_rows
            }
          }
        }
      }
    }

    SurveyDataSim <- SurveyData

    # Quarter range
    FQ <- 1
    maxFQ <- as.integer(maxFQ)

    # Load once per simulation run (was loaded each quarter)
    EARS <- dkuReadDataset("EARS_Taxonomy")

    while (FQ < maxFQ + 1) {
      SurveyData <- SurveyDataSim
      yearauto <- as.integer(yearauto)

      meta_prepared <- meta_prepared_new
      AttQTR <- AttQTR_new
      QTRLY_GC <- QTRLY_GC_new

      SurveyData <- SurveyData[SurveyData$fiscal_quarter == FQ, ]

      # POG setup (same as Sim.R)
      vars <- meta_prepared$Variable
      base_var <- sub(".*(charexp_|entexp_|ridesexp_)", "", vars)
      name <- sub(".*_", "", base_var)
      pahk_str <- sub("_.*", "", base_var)
      park_map <- c(mk = 1, ec = 2, dhs = 3, dak = 4)
      park <- unname(park_map[pahk_str])

      # NOTE: preserve existing behavior (numerator is not park-filtered)
      num <- colSums(SurveyData[, vars, drop = FALSE] > 0, na.rm = TRUE)
      denom_by_park <- table(SurveyData$park)
      den <- as.numeric(denom_by_park[as.character(park)])
      expd <- num / den

      meta_preparedPOG <- data.frame(name, Park = park, POG = meta_prepared$POG, expd)
      AttQTR <- AttQTR[AttQTR$FQ == FQ, ]
      QTRLY_GC <- QTRLY_GC[QTRLY_GC$FQ == FQ, ]

      meta_preparedPOG <- merge(meta_preparedPOG, AttQTR, by = "Park")
      meta_preparedPOG$NEWPOG <- meta_preparedPOG$expd * meta_preparedPOG$Factor
      meta_preparedPOG$NEWGC <- meta_preparedPOG$Att * meta_preparedPOG$NEWPOG
      metaPOG <- merge(meta_prepared, meta_preparedPOG[, c("Park", "name", "NEWGC")], by = c("name", "Park"))

      meta_prepared2 <- sqldf("select a.*,b.GC as QuarterlyGuestCarried from meta_prepared a left join
QTRLY_GC b on a.name=b.name and a.Park = b.Park")
      setDT(meta_prepared2)
      setDT(metaPOG)
      meta_prepared2[metaPOG, on = c("name", "Park"), QuarterlyGuestCarried := i.NEWGC]

      metadata <- data.frame(meta_prepared2)
      names(SurveyData) <- tolower(names(SurveyData))
      FY <- yearauto
      SurveyData[is.na(SurveyData)] <- 0

      SurveyData <- cbind(SurveyData, FY)
      SurveyData <- SurveyData[SurveyData$fiscal_quarter == FQ, ]

      # ---------- multinom weights (same as Sim.R) ----------
      five_Play <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Play", 2]]] == 5, na.rm = TRUE)
      four_Play <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Play", 2]]] == 4, na.rm = TRUE)
      three_Play <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Play", 2]]] == 3, na.rm = TRUE)
      two_Play <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Play", 2]]] == 2, na.rm = TRUE)
      one_Play <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Play", 2]]] == 1, na.rm = TRUE)
      weights_Play <- data.frame(cbind(q1 = SurveyData$q1, one_Play, two_Play, three_Play, four_Play, five_Play, Park = SurveyData$park, FY = SurveyData$FY))

      five_Show <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show", 2]]] == 5, na.rm = TRUE)
      four_Show <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show", 2]]] == 4, na.rm = TRUE)
      three_Show <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show", 2]]] == 3, na.rm = TRUE)
      two_Show <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show", 2]]] == 2, na.rm = TRUE)
      one_Show <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show", 2]]] == 1, na.rm = TRUE)
      weights_Show <- data.frame(cbind(q1 = SurveyData$q1, one_Show, two_Show, three_Show, four_Show, five_Show, Park = SurveyData$park, FY = SurveyData$FY))

      five_Preferred <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]]] == 5, na.rm = TRUE)
      four_Preferred <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]]] == 4, na.rm = TRUE)
      three_Preferred <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]]] == 3, na.rm = TRUE)
      two_Preferred <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]]] == 2, na.rm = TRUE)
      one_Preferred <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Genre == "Flaship" | metadata$Genre == "Anchor", 2]]] == 1, na.rm = TRUE)
      weights_Preferred <- data.frame(cbind(q1 = SurveyData$q1, one_Preferred, two_Preferred, three_Preferred, four_Preferred, five_Preferred, Park = SurveyData$park, FY = SurveyData$FY))

      five_RA <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride", 2]]] == 5, na.rm = TRUE)
      four_RA <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride", 2]]] == 4, na.rm = TRUE)
      three_RA <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride", 2]]] == 3, na.rm = TRUE)
      two_RA <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride", 2]]] == 2, na.rm = TRUE)
      one_RA <- rowSums(SurveyData[, names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride", 2]]] == 1, na.rm = TRUE)

      CouldntRide <- c()
      Experience <- c()
      for (i in 1:length(metadata[, 2])) {
        CouldntRide <- c(CouldntRide, metadata[, 18][i])
        Experience <- c(Experience, metadata[which(!is.na(metadata[, 18])), 2][i])
      }
      CouldntRide <- tolower(CouldntRide[!is.na(CouldntRide)])
      Experience <- tolower(Experience[!is.na(Experience)])

      cantgeton <- as.numeric(SurveyData[, Experience[1]] == 0 & SurveyData[, CouldntRide[1]] == 1 & SurveyData$ovpropex < 6)
      for (i in 2:length(Experience)) {
        cantgeton <- cantgeton + as.numeric(SurveyData[, Experience[i]] == 0 & SurveyData[, CouldntRide[i]] == 1 & SurveyData$ovpropex < 6)
      }
      cant <- data.frame(cbind(ovpropex = SurveyData$ovpropex, cantgeton, Park = SurveyData$park, FY = SurveyData$FY))
      weights_RA <- data.frame(cbind(ovpropex = SurveyData$ovpropex, one_RA, two_RA, three_RA, four_RA, five_RA, Park = SurveyData$park, FY = SurveyData$FY))
      weights <- cbind(weights_Play, weights_Show, weights_RA, cant, weights_Preferred)
      weights$ovpropex <- relevel(factor(weights$ovpropex), ref = "5")

      # Magic Kingdom Weights
      weights_mk <- weights[weights$Park == 1 & weights$FY == yearauto, ]
      test <- multinom(ovpropex ~ cantgeton + one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_mk)
      odds <- exp(confint(test, level = 0.995))
      z1 <- apply(odds, 3L, c)
      z2 <- expand.grid(dimnames(odds)[1:2])
      jz <- glm((ovpropex == 5) ~ one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_mk, family = "binomial")
      EXPcol <- exp(jz$coefficients[-1])
      weightedEEs <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.8 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][1:15, ], X5 = EXPcol[1:15])
      weightedEEs[weightedEEs$Var1 == "five_Play", 3:7] <- weightedEEs[weightedEEs$Var1 == "five_Play", 3:7] * 1
      weightedEEs[weightedEEs$Var1 == "five_Show", 3:7] <- weightedEEs[weightedEEs$Var1 == "five_Show", 3:7] * 1
      cantride <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.8 %" & data.frame(z2, z1)$Var1 == "cantgeton"), ], X5 = rep(1, 1))
      weightsPref1 <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.8 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][16:20, ], X5 = EXPcol[16:20])

      # EPCOT Weights
      weights_ep <- weights[weights$Park == 2 & weights$FY == yearauto, ]
      test <- multinom(ovpropex ~ cantgeton + one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_ep)
      odds <- exp(confint(test, level = 0.99))
      z1 <- apply(odds, 3L, c)
      z2 <- expand.grid(dimnames(odds)[1:2])
      jz <- glm((ovpropex == 5) ~ one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_ep, family = "binomial")
      EXPcol <- exp(jz$coefficients[-1])
      weightedEEs_EP <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.5 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][1:15, ], X5 = EXPcol[1:15])
      weightedEEs_EP[weightedEEs_EP$Var1 == "five_Play", 3:7] <- weightedEEs_EP[weightedEEs_EP$Var1 == "five_Play", 3:7] * 1
      weightedEEs_EP[weightedEEs_EP$Var1 == "five_Show", 3:7] <- weightedEEs_EP[weightedEEs_EP$Var1 == "five_Show", 3:7] * 1
      weightedEEs <- rbind(weightedEEs, weightedEEs_EP)
      cantride_EP <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.5 %" & data.frame(z2, z1)$Var1 == "cantgeton"), ], X5 = rep(1, 1))
      cantride <- rbind(cantride, cantride_EP)
      weightsPref2 <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.5 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][16:20, ], X5 = EXPcol[16:20])

      # STUDIOS Weights
      weights_st <- weights[weights$Park == 3 & weights$FY == yearauto, ]
      test <- multinom(ovpropex ~ cantgeton + one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_st)
      odds <- exp(confint(test, level = 0.80))
      z1 <- apply(odds, 3L, c)
      z2 <- expand.grid(dimnames(odds)[1:2])
      jz <- glm((ovpropex == 5) ~ one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_st, family = "binomial")
      EXPcol <- exp(jz$coefficients[-1])
      weightedEEs_ST <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "90 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][1:15, ], X5 = EXPcol[1:15])
      weightedEEs_ST[weightedEEs_ST$Var1 == "five_Play", 3:7] <- weightedEEs_ST[weightedEEs_ST$Var1 == "five_Play", 3:7] * 1
      weightedEEs_ST[weightedEEs_ST$Var1 == "five_Show", 3:7] <- weightedEEs_ST[weightedEEs_ST$Var1 == "five_Show", 3:7] * 1
      weightedEEs <- rbind(weightedEEs, weightedEEs_ST)
      cantride_ST <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "90 %" & data.frame(z2, z1)$Var1 == "cantgeton"), ], X5 = rep(1, 1))
      cantride <- rbind(cantride, cantride_ST)
      weightsPref3 <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "90 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][16:20, ], X5 = EXPcol[16:20])

      # Animal Kingdom Weights
      weights_ak <- weights[weights$Park == 4 & weights$FY == yearauto, ]
      test <- multinom(ovpropex ~ cantgeton + one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_ak)
      odds <- exp(confint(test, level = 0.70))
      z1 <- apply(odds, 3L, c)
      z2 <- expand.grid(dimnames(odds)[1:2])
      jz <- glm((ovpropex == 5) ~ one_Play + two_Play + three_Play + four_Play + five_Play + one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA + one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights_ak, family = "binomial")
      EXPcol <- exp(jz$coefficients[-1])
      weightedEEs_AK <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "15 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][1:15, ], X5 = EXPcol[1:15])
      weightedEEs_AK[weightedEEs_AK$Var1 == "five_Play", 3:7] <- weightedEEs_AK[weightedEEs_AK$Var1 == "five_Play", 3:7] * 1
      weightedEEs_AK[weightedEEs_AK$Var1 == "five_Show", 3:7] <- weightedEEs_AK[weightedEEs_AK$Var1 == "five_Show", 3:7] * 1
      weightedEEs <- rbind(weightedEEs, weightedEEs_AK)
      weightedEEs2 <- data.frame(FY = yearauto, weightedEEs)
      cantride_AK <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "15 %" & data.frame(z2, z1)$Var1 == "cantgeton"), ], X5 = rep(1, 1))
      cantride2 <- data.frame(FY = yearauto, rbind(cantride, cantride_AK))
      weightsPref4 <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "15 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton"), ][16:20, ], X5 = EXPcol[16:20])

      weights22 <- rbind(weightedEEs, weightsPref1, weightsPref2, weightsPref3, weightsPref4)
      CantRideWeight22 <- cantride2[, -1]
      if (verbose) print(weights22)

      #The weights above (weightedEEs) are now multiplied to the experience data so we can convert the data into overall excellent experiences
      #The Cant Ride weights are a separate weight for Guests that wanted to ride but couldnt.  Think of these weights as runs against our team

      SurveyData <- SurveyData[SurveyData$fiscal_quarter == FQ, ]
      names(SurveyData) <- tolower(names(SurveyData))

      FY <- yearauto
      SurveyData[is.na(SurveyData)] <- 0
      SurveyData <- cbind(SurveyData, FY)

      metadata$POG[(metadata$Type == "Show" & is.na(metadata$POG) | metadata$Type == "Play" & is.na(metadata$POG))] <- 0
      metadata <- metadata[!is.na(metadata$Category1) | metadata$Type == "Ride", ]

      SurveyData22 <- SurveyData
      CountData22 <- SurveyData22

      metadata[, 2] <- tolower(metadata[, 2])
      metadata[, 3] <- tolower(metadata[, 3])
      metadata[, 18] <- tolower(metadata[, 18])
      metadata[, 19] <- tolower(metadata[, 19])

      rideagain <- metadata[, 3]
      rideexp <- metadata[!is.na(metadata[, 3]), 2]
      rideexp_fix <- metadata[!is.na(metadata[, 2]), 2]

      rideagain <- rideagain[!is.na(rideagain)]
      rideexp <- rideexp[!is.na(rideexp)]
      rideexp_fix <- rideexp_fix[!is.na(rideexp_fix)]

      for (i in 1:length(rideagain)) {
        SurveyData22[which(SurveyData22[, rideexp[i]] != 0 & SurveyData22[, rideagain[i]] == 0), rideagain[i]] <- 1
      }

      ridesexp_full <- metadata[, 2]
      ridesexp_full <- ridesexp_full[!is.na(ridesexp_full) & grepl("ridesexp_", ridesexp_full, fixed = TRUE)]
      ridesexp <- sub("^.*ridesexp_", "", ridesexp_full)
      ridesexp <- sub(".*_", "", ridesexp)

      entexp_full <- metadata[, 2]
      entexp_full <- entexp_full[!is.na(entexp_full) & grepl("entexp_", entexp_full, fixed = TRUE)]
      entexp <- sub("^.*entexp_", "", entexp_full)
      entexp <- sub(".*_", "", entexp)

      charexp_full <- metadata[, 2]
      charexp_full <- charexp_full[!is.na(charexp_full) & grepl("charexp_", charexp_full, fixed = TRUE)]
      charexp_after_prefix <- sub("^.*charexp_", "", charexp_full)
      howexp <- paste("charhow", charexp_after_prefix, sep = "_")
      howexp <- howexp[howexp != "charhow_NA"]
      charexp <- sub(".*_", "", charexp_after_prefix)

      for (i in 1:length(charexp)) {
        SurveyData22[, as.character(colnames(SurveyData22)[grepl("charexp_", colnames(SurveyData22))])[i]] <-
          SurveyData22[, as.character(colnames(SurveyData22)[grepl("charexp_", colnames(SurveyData22))])[i]] *
          as.numeric(SurveyData22[, colnames(SurveyData22) == howexp[i]] < 2 |
                       SurveyData22[, colnames(SurveyData22) == howexp[i]] == 3 |
                       SurveyData22[, colnames(SurveyData22) == howexp[i]] == 4)
      }

      rideagainx <- metadata[, 18]
      RIDEX <- metadata[!is.na(metadata[, 18]), 2]
      rideagainx <- tolower(rideagainx[!is.na(rideagainx)])
      RIDEX <- tolower(RIDEX[!is.na(RIDEX)])

      for (i in 1:length(RIDEX)) {
        SurveyData22[which(SurveyData22[, RIDEX[i]] == 0 & SurveyData22[, rideagainx[i]] == 1 & SurveyData22$ovpropex < 6), RIDEX[i]] <- -1
      }

      new_cols <- unique(c(
        paste0(ridesexp, "2"), paste0(ridesexp, "3"),
        paste0(entexp, "2"), paste0(entexp, "3"),
        paste0(charexp, "2"), paste0(charexp, "3")
      ))
      SurveyData22[, new_cols] <- 0

      idx_non_na <- which(!is.na(metadata[, 2]))
      rideagain_fix <- metadata[, 3]
      i <- 5
      for (j in 1:5) {
        for (park in 1:4) {
          idx_pj <- which(SurveyData22$park == park & SurveyData22$ovpropex == j)
          # Ride assignments
          idx_ride <- which(metadata$Type[idx_non_na] == "Ride" & metadata$Park[idx_non_na] == park)
          for (k in idx_ride) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15 + 10, j + 2]
              }
            }
          }
          # Ent assignments
          idx_ent <- which(metadata$Type[idx_non_na] == "Play" & metadata$Park[idx_non_na] == park)
          for (k in idx_ent) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15, j + 2]
              }
            }
          }

          # Flaship/Anchor assignments
          idx_flash <- which((metadata$Genre[idx_non_na] == "Flaship" | metadata$Genre[idx_non_na] == "Anchor") & metadata$Park[idx_non_na] == park)
          for (k in idx_flash) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 5 + 60, j + 2]
              }
            }
          }
          # Show assignments
          idx_show <- which(metadata$Type[idx_non_na] == "Show" & metadata$Genre[idx_non_na] != "Anchor" & metadata$Park[idx_non_na] == park)
          for (k in idx_show) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15 + 5, j + 2]
              }
            }
          }
        }
        if (verbose) print(c(i, j))
      }

      for (i in 1:4) {
        for (j in 1:5) {
          for (park in 1:4) {
            idx_pj <- which(SurveyData22$park == park & SurveyData22$ovpropex == j)
            idx_p5 <- which(SurveyData22$park == park & SurveyData22$ovpropex == 5)
            # --- Rides ---
            idx_ride <- which(metadata$Type == "Ride" & metadata$Park == park)
            for (k in idx_ride) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15 + 10, j + 2]
                }
                # "Can't ride" assignment
                rows_j <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == -1)]
                rows_5 <- idx_p5[which(SurveyData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  SurveyData22[rows_j, colname2] <- CantRideWeight22[park, j + 2] * (-1 * SurveyData22[rows_j, rideexp_fix[k]])
                }
                if (length(rows_5) > 0) {
                  SurveyData22[rows_5, colname2] <- CantRideWeight22[park, 5 + 2] * (1 * SurveyData22[rows_5, rideexp_fix[k]])
                  if (verbose && colname2 == "safaris3" && FQ == 4) {
                    print(c(colname2, rideagain_fix[k]))
                    print(weights22[i + (park - 1) * 15 + 10, j + 2])
                    print(SurveyData22[rows, rideexp_fix[k]])
                    print(length(rows))
                  }
                }
              }
            }

            # --- Flaship/Anchor ---
            idx_flash <- which((metadata$Genre == "Flaship" | metadata$Genre == "Anchor") & metadata$Park == park)
            for (k in idx_flash) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 5 + 60, j + 2]
                }
                # "Can't ride" assignment
                rows_j <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == -1)]
                rows_5 <- idx_p5[which(SurveyData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  SurveyData22[rows_j, colname2] <- CantRideWeight22[park, j + 2] * (-1 * SurveyData22[rows_j, rideexp_fix[k]])
                }
                if (length(rows_5) > 0) {
                  SurveyData22[rows_5, colname2] <- CantRideWeight22[park, 5 + 2] * (1 * SurveyData22[rows_5, rideexp_fix[k]])
                }
              }
            }

            # --- Show ---
            idx_show <- which(metadata$Type == "Show" & metadata$Genre != "Anchor" & metadata$Park == park)
            for (k in idx_show) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15 + 5, j + 2]
                }
                # "Can't ride" assignment
                rows_j <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == -1)]
                rows_5 <- idx_p5[which(SurveyData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  SurveyData22[rows_j, colname2] <- CantRideWeight22[park, j + 2] * (-1 * SurveyData22[rows_j, rideexp_fix[k]])
                }
                if (length(rows_5) > 0) {
                  SurveyData22[rows_5, colname2] <- CantRideWeight22[park, 5 + 2] * (1 * SurveyData22[rows_5, rideexp_fix[k]])
                }
              }
            }

            # --- Play ---
            idx_play <- which(metadata$Type == "Play" & metadata$Park == park)
            for (k in idx_play) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15, j + 2]
                }
                # "Can't ride" assignment
                rows_j <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == -1)]
                rows_5 <- idx_p5[which(SurveyData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  SurveyData22[rows_j, colname2] <- CantRideWeight22[park, j + 2] * (-1 * SurveyData22[rows_j, rideexp_fix[k]])
                }
                if (length(rows_5) > 0) {
                  SurveyData22[rows_5, colname2] <- CantRideWeight22[park, 5 + 2] * (1 * SurveyData22[rows_5, rideexp_fix[k]])
                }
              }
            }
          }
          if (verbose) print(c(i, j))
        }
      }

      # ---- Block 7: fast aggregation of runs/against (weighted) ----
      # Build mapping from metadata variable to short "base" (e.g., safaris -> safaris2/safaris3)
      meta_var <- as.character(metadata[, 2])
      meta_map <- data.table(
        Park = metadata$Park,
        Type = metadata$Type,
        Category1 = metadata$Category1,
        base = sub(".*_", "", sub(".*(ridesexp_|entexp_|charexp_)", "", meta_var))
      )
      meta_map <- meta_map[!is.na(Park) & !is.na(Type) & !is.na(base) & base != ""]

      ride_bases <- unique(meta_map[Type == "Ride", base])
      show_map <- unique(meta_map[Type == "Show" & !is.na(Category1), .(Park, base, Category1)])
      play_map <- unique(meta_map[Type == "Play" & !is.na(Category1), .(Park, base, Category1)])
      show_names <- unique(show_map$Category1)
      play_names <- unique(play_map$Category1)

      agg_by_base <- function(dt, bases, suffix, skeleton, all_bases) {
        cols <- paste0(bases, suffix)
        cols <- cols[cols %in% names(dt)]

        out <- copy(as.data.table(skeleton))
        if (length(cols) > 0) {
          bases2 <- sub(paste0(suffix, "$"), "", cols)
          tmp <- dt[, c("park", "newgroup", "fiscal_quarter", cols), with = FALSE]
          setnames(tmp, cols, bases2)
          tmp_out <- tmp[, lapply(.SD, sum, na.rm = TRUE),
                         by = .(Park = park, LifeStage = newgroup, QTR = fiscal_quarter),
                         .SDcols = bases2]
          out <- merge(out, tmp_out, by = c("Park", "LifeStage", "QTR"), all.x = TRUE)
        }

        for (nm in all_bases) {
          if (!nm %in% names(out)) out[, (nm) := 0]
          out[is.na(get(nm)), (nm) := 0]
        }
        setcolorder(out, c("Park", "LifeStage", "QTR", all_bases))
        as.data.frame(out)
      }

      agg_by_category <- function(dt, map_dt, suffix, skeleton, all_names) {
        out <- copy(as.data.table(skeleton))
        for (nm in all_names) out[, (nm) := 0]
        if (nrow(map_dt) == 0) {
          setcolorder(out, c("Park", "LifeStage", "QTR", all_names))
          return(as.data.frame(out))
        }

        cols <- unique(paste0(map_dt$base, suffix))
        cols <- cols[cols %in% names(dt)]
        if (length(cols) == 0) {
          setcolorder(out, c("Park", "LifeStage", "QTR", all_names))
          return(as.data.frame(out))
        }

        dt_sub <- dt[, c("park", "newgroup", "fiscal_quarter", cols), with = FALSE]
        long <- data.table::melt(
          dt_sub,
          id.vars = c("park", "newgroup", "fiscal_quarter"),
          measure.vars = cols,
          variable.name = "col",
          value.name = "value",
          variable.factor = FALSE
        )
        long[, base := sub(paste0(suffix, "$"), "", col)]

        map2 <- unique(as.data.table(map_dt)[, .(park = Park, base, NAME = Category1)])
        long <- merge(long, map2, by = c("park", "base"), all = FALSE)

        grp <- long[, .(value = sum(value, na.rm = TRUE)),
                    by = .(Park = park, LifeStage = newgroup, QTR = fiscal_quarter, NAME)]
        wide <- data.table::dcast(grp, Park + LifeStage + QTR ~ NAME, value.var = "value", fill = 0)
        wide <- merge(out[, .(Park, LifeStage, QTR)], wide, by = c("Park", "LifeStage", "QTR"), all.x = TRUE)

        for (nm in all_names) {
          if (!nm %in% names(wide)) wide[, (nm) := 0]
          wide[is.na(get(nm)), (nm) := 0]
        }
        setcolorder(wide, c("Park", "LifeStage", "QTR", all_names))
        as.data.frame(wide)
      }

      dtw <- as.data.table(SurveyData22)
      skeleton_w <- unique(dtw[, .(Park = park, LifeStage = newgroup, QTR = fiscal_quarter)])

      Ride_Runs20 <- agg_by_base(dtw, ride_bases, "2", skeleton_w, ride_bases)
      Ride_Against20 <- agg_by_base(dtw, ride_bases, "3", skeleton_w, ride_bases)
      Show_Runs20 <- agg_by_category(dtw, show_map, "2", skeleton_w, show_names)
      Show_Against20 <- agg_by_category(dtw, show_map, "3", skeleton_w, show_names)
      Play_Runs20 <- agg_by_category(dtw, play_map, "2", skeleton_w, play_names)
      Play_Against20 <- agg_by_category(dtw, play_map, "3", skeleton_w, play_names)
      # ---- end block 7 (weighted) ----

      ############################################################################################
      # Now that we have the weighted data we need the raw counts for our weighted metric
      # CountData = Raw
      # SurveyData = Weighted
      ############################################################################################

      CountData22 <- SurveyData22

      for (i in 1:length(RIDEX)) {
        CountData22[which(CountData22[, RIDEX[i]] == 0 & CountData22[, rideagainx[i]] == 1 & CountData22$ovpropex < 6), RIDEX[i]] <- -1
      }

      CountData22[, new_cols] <- 0

      idx_non_na <- which(!is.na(metadata[, 2]))
      rideagain_fix <- metadata[, 3]
      i <- 5
      for (j in 1:5) {
        for (park in 1:4) {
          idx_pj <- which(CountData22$park == park & CountData22$ovpropex == j)
          # Ride assignments
          idx_ride <- which(metadata$Type[idx_non_na] == "Ride" & metadata$Park[idx_non_na] == park)
          for (k in idx_ride) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                CountData22[rows, colname2] <- 1
              }
            }
          }
          # Ent assignments
          idx_ent <- which(metadata$Type[idx_non_na] == "Play" & metadata$Park[idx_non_na] == park)
          for (k in idx_ent) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                CountData22[rows, colname2] <- 1
              }
            }
          }

          # Flaship/Anchor assignments
          idx_flash <- which((metadata$Genre[idx_non_na] == "Flaship" | metadata$Genre[idx_non_na] == "Anchor") & metadata$Park[idx_non_na] == park)
          for (k in idx_flash) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                CountData22[rows, colname2] <- 1
              }
            }
          }
          # Show assignments
          idx_show <- which(metadata$Type[idx_non_na] == "Show" & metadata$Genre[idx_non_na] != "Anchor" & metadata$Park[idx_non_na] == park)
          for (k in idx_show) {
            if (!is.na(rideexp_fix[k])) {
              colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
              rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
              if (length(rows) > 0) {
                CountData22[rows, colname2] <- 1
              }
            }
          }
        }
        if (verbose) print(c(i, j))
      }

      for (i in 1:4) {
        for (j in 1:5) {
          for (park in 1:4) {
            idx_pj <- which(CountData22$park == park & CountData22$ovpropex == j)
            idx_p5 <- which(CountData22$park == park & CountData22$ovpropex == 5)
            # --- Rides ---
            idx_ride <- which(metadata$Type == "Ride" & metadata$Park == park)
            for (k in idx_ride) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  CountData22[rows, colname2] <- 1
                }
                rows_j <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  CountData22[rows_j, colname2] <- -1 * CountData22[rows_j, rideexp_fix[k]]
                }
                rows_5 <- idx_p5[which(CountData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_5) > 0) {
                  CountData22[rows_5, colname2] <- 1 * CountData22[rows_5, rideexp_fix[k]]
                }
              }
            }

            # --- Flaship/Anchor ---
            idx_flash <- which((metadata$Genre == "Flaship" | metadata$Genre == "Anchor") & metadata$Park == park)
            for (k in idx_flash) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  CountData22[rows, colname2] <- 1
                }
                rows_j <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  CountData22[rows_j, colname2] <- -1 * CountData22[rows_j, rideexp_fix[k]]
                }
                rows_5 <- idx_p5[which(CountData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_5) > 0) {
                  CountData22[rows_5, colname2] <- 1 * CountData22[rows_5, rideexp_fix[k]]
                }
              }
            }

            # --- Show ---
            idx_show <- which(metadata$Type == "Show" & metadata$Genre != "Anchor" & metadata$Park == park)
            for (k in idx_show) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  CountData22[rows, colname2] <- 1
                }
                rows_j <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  CountData22[rows_j, colname2] <- -1 * CountData22[rows_j, rideexp_fix[k]]
                }
                rows_5 <- idx_p5[which(CountData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_5) > 0) {
                  CountData22[rows_5, colname2] <- 1 * CountData22[rows_5, rideexp_fix[k]]
                }
              }
            }

            # --- Play ---
            idx_play <- which(metadata$Type == "Play" & metadata$Park == park)
            for (k in idx_play) {
              if (!is.na(rideexp_fix[k])) {
                colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "3")
                rows <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == i)]
                if (length(rows) > 0) {
                  CountData22[rows, colname2] <- 1
                }
                rows_j <- idx_pj[which(CountData22[idx_pj, rideexp_fix[k]] == -1)]
                if (length(rows_j) > 0) {
                  CountData22[rows_j, colname2] <- -1 * CountData22[rows_j, rideexp_fix[k]]
                }
                rows_5 <- idx_p5[which(CountData22[idx_p5, rideexp_fix[k]] == -1)]
                if (length(rows_5) > 0) {
                  CountData22[rows_5, colname2] <- 1 * CountData22[rows_5, rideexp_fix[k]]
                }
              }
            }
          }
          if (verbose) print(c(i, j))
        }
      }

      # ---- Block 7: fast aggregation of runs/against (original) ----
      dto <- as.data.table(CountData22)
      skeleton_o <- unique(dto[, .(Park = park, LifeStage = newgroup, QTR = fiscal_quarter)])

      Ride_OriginalRuns20 <- agg_by_base(dto, ride_bases, "2", skeleton_o, ride_bases)
      Ride_OriginalAgainst20 <- agg_by_base(dto, ride_bases, "3", skeleton_o, ride_bases)
      Show_OriginalRuns20 <- agg_by_category(dto, show_map, "2", skeleton_o, show_names)
      Show_OriginalAgainst20 <- agg_by_category(dto, show_map, "3", skeleton_o, show_names)
      Play_OriginalRuns20 <- agg_by_category(dto, play_map, "2", skeleton_o, play_names)
      Play_OriginalAgainst20 <- agg_by_category(dto, play_map, "3", skeleton_o, play_names)
      # ---- end block 7 (original) ----

      ############################################################################################
      # END OF WEIGHTING NOW WE HAVE TO COMBINE EVERYTHING TO MAKE THE STATISTIC
      ############################################################################################

      Show_OriginalRuns <- rbind(Show_OriginalRuns20)
      Show_OriginalAgainst <- rbind(Show_OriginalAgainst20)
      Show_Runs <- rbind(Show_Runs20)
      Show_Against <- rbind(Show_Against20)

      one <- reshape2::melt(Show_OriginalRuns, id = (c("Park", "LifeStage", "QTR")))
      two <- reshape2::melt(Show_OriginalAgainst, id = (c("Park", "LifeStage", "QTR")))
      three <- reshape2::melt(Show_Runs, id = (c("Park", "LifeStage", "QTR")))
      four <- reshape2::melt(Show_Against, id = (c("Park", "LifeStage", "QTR")))

      Show <- cbind(one, two[, -c(1:4)], three[, -c(1:4)], four[, -c(1:4)])
      Show <- Show[!is.na(Show$Park), ]
      Show <- unique(Show[apply(Show != 0, 1, all), ])
      names(Show) <- c("Park", "LifeStage", "QTR", "NAME", "Original_RS", "Original_RA", "wEE", "wEEx")

      Play_OriginalRuns <- rbind(Play_OriginalRuns20)
      Play_OriginalAgainst <- rbind(Play_OriginalAgainst20)
      Play_Runs <- rbind(Play_Runs20)
      Play_Against <- rbind(Play_Against20)

      one <- reshape2::melt(Play_OriginalRuns, id = (c("Park", "LifeStage", "QTR")))
      two <- reshape2::melt(Play_OriginalAgainst, id = (c("Park", "LifeStage", "QTR")))
      three <- reshape2::melt(Play_Runs, id = (c("Park", "LifeStage", "QTR")))
      four <- reshape2::melt(Play_Against, id = (c("Park", "LifeStage", "QTR")))

      Play <- cbind(one, two[, -c(1:4)], three[, -c(1:4)], four[, -c(1:4)])
      Play <- unique(Play[apply(Play != 0, 1, all), ])
      names(Play) <- c("Park", "LifeStage", "QTR", "NAME", "Original_RS", "Original_RA", "wEE", "wEEx")
      Play <- Play[Play$NAME != "Park.1", ]
      Play <- Play[Play$NAME != "LifeStage.1", ]
      Play <- Play[Play$NAME != "QTR.1", ]

      Ride_OriginalRuns <- rbind(Ride_OriginalRuns20)
      Ride_OriginalAgainst <- rbind(Ride_OriginalAgainst20)
      Ride_Runs <- rbind(Ride_Runs20)
      Ride_Against <- rbind(Ride_Against20)

      one <- reshape2::melt(Ride_OriginalRuns, id = (c("Park", "LifeStage", "QTR")))
      two <- reshape2::melt(Ride_OriginalAgainst, id = (c("Park", "LifeStage", "QTR")))
      three <- reshape2::melt(Ride_Runs, id = (c("Park", "LifeStage", "QTR")))
      four <- reshape2::melt(Ride_Against, id = (c("Park", "LifeStage", "QTR")))

      Ride <- cbind(one, two[, -c(1:4)], three[, -c(1:4)], four[, -c(1:4)])
      Ride <- unique(Ride[apply(Ride != 0, 1, all), ])
      names(Ride) <- c("Park", "LifeStage", "QTR", "NAME", "Original_RS", "Original_RA", "wEE", "wEEx")

      wRAA_Table <- data.frame(Ride, Genre = "Ride")
      Show$Genre <- "Show"
      Play$Genre <- "Play"
      wRAA_Table <- rbind(wRAA_Table, Show, Play)

      twox <- wRAA_Table %>% group_by(NAME, Park, Genre, QTR, LifeStage) %>% mutate(Original_RS = sum(Original_RS), Original_RA = sum(Original_RA), wEE = sum(wEE), wEEx = sum(wEEx)) %>% distinct(.keep_all = FALSE)
      onex <- twox %>% group_by(NAME, Park, QTR) %>% mutate(sum = sum(Original_RS + Original_RA))
      onex$Percent <- (onex$Original_RS + onex$Original_RA) / onex$sum
      wRAA_Table <- sqldf("select a.*,b.Percent from twox a left join onex b on a.Park = b.Park and a.QTR=b.QTR and a.LifeStage = b.LifeStage and a.Name = b.Name")

      wRAA_Table <- wRAA_Table[!is.na(wRAA_Table$Park), ]
      wRAA_Table <- cbind(wRAA_Table, wOBA = wRAA_Table$wEE / (wRAA_Table$Original_RS + wRAA_Table$Original_RA))

      wRAA_Table <- merge(x = wRAA_Table, y = aggregate(wRAA_Table$Original_RS, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = sum), by = c("QTR", "Park"), all.x = TRUE)
      names(wRAA_Table)[length(names(wRAA_Table))] <- "OBP1"
      wRAA_Table <- merge(x = wRAA_Table, y = aggregate(wRAA_Table$Original_RA, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = sum), by = c("QTR", "Park"), all.x = TRUE)
      names(wRAA_Table)[length(names(wRAA_Table))] <- "OBP2"
      wRAA_Table$OBP <- wRAA_Table$OBP1 / (wRAA_Table$OBP1 + wRAA_Table$OBP2)

      wRAA_Table <- merge(x = wRAA_Table, y = aggregate(wRAA_Table$wOBA, by = list(QTR = wRAA_Table$QTR, Park = wRAA_Table$Park), FUN = median), by = c("QTR", "Park"), all.x = TRUE)
      names(wRAA_Table)[length(names(wRAA_Table))] <- "wOBA_Park"
      wRAA_Table <- data.frame(wRAA_Table, wOBA_Scale = wRAA_Table$wOBA_Park / wRAA_Table$OBP)

      join_keys <- c("NAME", "Park", "Genre", "QTR", "LifeStage")
      table2_nodup <- EARS[, setdiff(names(EARS), setdiff(names(wRAA_Table), join_keys))]
      EARSFinal_Final <- merge(wRAA_Table, table2_nodup, by = join_keys, all = FALSE)

      EARSx <- EARSFinal_Final
      EARSx$wRAA <- ((EARSx$wOBA - EARSx$wOBA_Park) / EARSx$wOBA_Scale) * (EARSx$wRAA_Table.AnnualGuestsCarried)
      EARSx$EARS <- EARSx$wRAA / EARSx$RPW
      EARSx$EARS <- (EARSx$EARS + EARSx$replacement) * EARSx$p
      if (verbose) print(unique(EARSx$QTR))
      EARSTotal <- rbind(EARSTotal, EARSx)

      FQ <- FQ + 1
    }

    EARS$Actual_EARS <- EARS$EARS
    result <- full_join(EARSTotal, EARS[, c("NAME", "Park", "Genre", "QTR", "LifeStage", "Actual_EARS")], by = c("NAME", "Park", "Genre", "QTR", "LifeStage"))
    result$EARS[is.na(result$EARS)] <- 0
    colnames(result)[colnames(result) == "EARS"] <- "Simulation_EARS"
    result$Incremental_EARS <- result$Simulation_EARS - result$Actual_EARS
    EARSTotal2 <- result
    EARSTotal2$sim_run <- run
    EARSTotal2
  }

  Simulation_Results <- EARSTotal_list
  Simulation_Results
}


 
server <- function(input, output, session) {
 
  has_plotly <- requireNamespace("plotly", quietly = TRUE)

  exp_data <- dkuReadDataset("MetaDataTool_Exp")

  # ---- Fiscal calendar mapping (DT -> FY24) ----
  # If a user picks dates outside FY24, map them to the corresponding FY24 date
  # using the fiscal calendar table (DT) by fiscal week/day (or fiscal day-of-year).
  dt_cache <- reactiveVal(NULL)

  # ---- Survey max date (FY24_prepared) ----
  # Used to clamp the *end* of date ranges if user selects beyond available data.
  survey_max_cache <- reactiveVal(list())

  get_survey_max_date <- function(park) {
    park_key <- as.character(park)
    cache <- survey_max_cache()
    if (!is.null(cache[[park_key]])) return(cache[[park_key]])

    mx <- tryCatch({
      sd <- dkuReadDataset("FY24_prepared")
      names(sd) <- tolower(names(sd))

      date_col <- NULL
      if ("visdate_parsed" %in% names(sd)) date_col <- "visdate_parsed"
      if (is.null(date_col) && "visdate" %in% names(sd)) date_col <- "visdate"
      if (is.null(date_col)) stop("FY24_prepared missing visdate_parsed/visdate")

      if (!("park" %in% names(sd))) stop("FY24_prepared missing park column")

      d <- as.Date(sd[[date_col]])
      d <- d[sd$park == as.numeric(park) & !is.na(d)]
      if (length(d) == 0) stop("No dates for selected park")
      max(d, na.rm = TRUE)
    }, error = function(e) {
      # Fallback: don't clamp if we can't determine max
      as.Date(NA)
    })

    cache[[park_key]] <- mx
    survey_max_cache(cache)
    mx
  }

  get_fiscal_dt <- function() {
    dt <- dt_cache()
    if (!is.null(dt)) return(dt)

    dt <- tryCatch(dkuReadDataset("DT"), error = function(e) NULL)
    if (is.null(dt)) {
      dt_cache(NULL)
      return(NULL)
    }
    names(dt) <- tolower(names(dt))

    # Identify date column
    date_candidates <- c("date", "cal_date", "calendar_date", "day", "dt", "visdate", "visdate_parsed")
    date_col <- date_candidates[date_candidates %in% names(dt)][1]
    if (is.na(date_col) || is.null(date_col)) return(NULL)

    # Identify fiscal year column
    fy_candidates <- c("fy", "fiscal_year", "fiscalyear")
    fy_col <- fy_candidates[fy_candidates %in% names(dt)][1]
    if (is.na(fy_col) || is.null(fy_col)) return(NULL)

    # Optional keys
    week_candidates <- c("fiscal_week", "fiscalweek", "fw", "week_of_fy", "fiscal_week_of_year")
    dow_candidates <- c("fiscal_dow", "fiscal_day_of_week", "dow", "day_of_week", "weekday")
    fday_candidates <- c("fiscal_day", "fiscal_day_of_year", "day_of_fy", "fy_day")

    week_col <- week_candidates[week_candidates %in% names(dt)][1]
    dow_col <- dow_candidates[dow_candidates %in% names(dt)][1]
    fday_col <- fday_candidates[fday_candidates %in% names(dt)][1]

    out <- data.frame(
      date = as.Date(dt[[date_col]]),
      fy_raw = dt[[fy_col]],
      stringsAsFactors = FALSE
    )

    # Normalize FY values (e.g. "FY24" -> 2024)
    fy_num <- suppressWarnings(as.integer(out$fy_raw))
    if (anyNA(fy_num)) {
      fy_chr <- as.character(out$fy_raw)
      fy_digits <- suppressWarnings(as.integer(gsub("\\D+", "", fy_chr)))
      # If FY is given as 24, convert to 2024 (assume 2000s)
      fy_digits <- ifelse(!is.na(fy_digits) & fy_digits < 100, 2000 + fy_digits, fy_digits)
      fy_num <- fy_digits
    }
    out$fy <- fy_num

    if (!is.na(week_col) && !is.null(week_col)) out$fweek <- suppressWarnings(as.integer(dt[[week_col]])) else out$fweek <- NA_integer_
    if (!is.na(fday_col) && !is.null(fday_col)) out$fday <- suppressWarnings(as.integer(dt[[fday_col]])) else out$fday <- NA_integer_

    if (!is.na(dow_col) && !is.null(dow_col)) {
      dow_val <- dt[[dow_col]]
      if (is.numeric(dow_val)) {
        out$dow <- suppressWarnings(as.integer(dow_val))
      } else {
        # map weekday names -> 1..7 (Mon..Sun) if needed
        dow_chr <- tolower(as.character(dow_val))
        map <- c(mon = 1, tue = 2, wed = 3, thu = 4, fri = 5, sat = 6, sun = 7)
        out$dow <- unname(map[substr(dow_chr, 1, 3)])
      }
    } else {
      out$dow <- as.integer(format(out$date, "%u"))
    }

    out <- out[!is.na(out$date) & !is.na(out$fy), ]
    dt_cache(out)
    out
  }

  map_date_to_fy24 <- function(d) {
    dt <- get_fiscal_dt()
    d <- as.Date(d)
    if (is.null(dt) || is.na(d)) return(d)

    # Find exact match, else nearest date in DT
    idx <- which(dt$date == d)
    if (length(idx) == 0) {
      idx <- which.min(abs(as.numeric(dt$date - d)))
    } else {
      idx <- idx[1]
    }

    row <- dt[idx, , drop = FALSE]
    if (is.na(row$fy) || row$fy == 2024L) return(d)

    fy24 <- dt[dt$fy == 2024L, , drop = FALSE]
    if (nrow(fy24) == 0) return(d)

    # Prefer fiscal day-of-year mapping if present
    if (!is.na(row$fday[1]) && any(!is.na(fy24$fday))) {
      tgt <- fy24[fy24$fday == row$fday[1], , drop = FALSE]
      if (nrow(tgt) > 0) return(tgt$date[1])
      # fallback: nearest fiscal day
      fy24n <- fy24[!is.na(fy24$fday), , drop = FALSE]
      if (nrow(fy24n) > 0) return(fy24n$date[which.min(abs(fy24n$fday - row$fday[1]))])
    }

    # Else map by fiscal week + day-of-week
    if (!is.na(row$fweek[1]) && any(!is.na(fy24$fweek))) {
      tgt <- fy24[fy24$fweek == row$fweek[1] & fy24$dow == row$dow[1], , drop = FALSE]
      if (nrow(tgt) > 0) return(tgt$date[1])
      # fallback: same week, any dow
      tgt2 <- fy24[fy24$fweek == row$fweek[1], , drop = FALSE]
      if (nrow(tgt2) > 0) return(tgt2$date[which.min(abs(tgt2$dow - row$dow[1]))])
      # fallback: nearest week
      fy24w <- fy24[!is.na(fy24$fweek), , drop = FALSE]
      if (nrow(fy24w) > 0) {
        near <- fy24w[which.min(abs(fy24w$fweek - row$fweek[1])), , drop = FALSE]
        return(near$date[1])
      }
    }

    # Final fallback: nearest calendar date in FY24
    fy24$date[which.min(abs(as.numeric(fy24$date - d)))]
  }

  map_range_to_fy24 <- function(dr, park = input$selected_park) {
    if (is.null(dr) || length(dr) != 2 || any(is.na(dr))) return(dr)
    out <- c(map_date_to_fy24(dr[1]), map_date_to_fy24(dr[2]))

    # Clamp end date to max available survey date (per park) if needed
    mx <- get_survey_max_date(park)
    if (!is.na(mx)) {
      out <- as.Date(out)
      if (!is.na(out[2]) && out[2] > mx) out[2] <- mx
      if (!is.na(out[1]) && out[1] > mx) out[1] <- mx
    }

    if (out[1] > out[2]) out <- c(out[2], out[1])
    out
  }
 
  get_exp_date_ranges <- function(input, selected_exps) {
    adjusted_any <- FALSE

    out <- lapply(selected_exps, function(exp_name) {
      dr_in <- input[[paste0("daterange_", exp_name)]]
      dr_out <- map_range_to_fy24(dr_in)
      if (!identical(as.Date(dr_in), as.Date(dr_out))) adjusted_any <<- TRUE
      dr_out
    }) |> setNames(selected_exps)

    if (adjusted_any) {
      showNotification(
        "One or more date ranges were mapped to the corresponding dates in FY24 (via fiscal calendar DT).",
        type = "message",
        duration = 6
      )
    }

    out
  }
 
  # Simulation runner (fully embedded in this server.R via run_simulation_simR_embedded()).
  # This avoids any dependency on external R files at Shiny runtime.
  run_simulation <- function(park, exp_name, exp_date_ranges, n_runs, num_cores) {
    run_simulation_simR_embedded(
      n_runs = as.integer(n_runs),
      num_cores = as.integer(num_cores),
      yearauto = 2024L,
      park_for_sim = as.integer(park),
      exp_name = exp_name,
      exp_date_ranges = exp_date_ranges,
      maxFQ = 4L,
      verbose = FALSE
    )
  }
 
  # ---- Experience selector UI ----
  output$exp_select_ui <- renderUI({
    req(input$selected_park)
    df <- exp_data[exp_data$Park == as.numeric(input$selected_park), ]
    selectizeInput(
      "selected_exps",
      "Select experiences",
      choices = setNames(df$name, df$Repository.Offering.Name),
      multiple = TRUE,
      options = list(placeholder = "Choose one or more experiences...")
    )
  })
 
  selected_exps_rv <- reactiveVal(character(0))
 
  observeEvent(input$selected_exps, {
    selected_exps_rv(if (is.null(input$selected_exps)) character(0) else input$selected_exps)
  })
 
  observe({
    # keep this observer lightweight; removal buttons are no longer used
    invisible(NULL)
  })
 
  output$selected_exp_dates <- renderUI({
    exps <- selected_exps_rv()
    if (length(exps) == 0) {
      return(helpText("Select one or more experiences to configure date ranges."))
    }

    lapply(exps, function(exp_name) {
      exp_label <- as.character(exp_data$Repository.Offering.Name[exp_data$name == exp_name][1])
      input_id <- paste0("daterange_", exp_name)
      bslib::tooltip(
        dateRangeInput(
          inputId = input_id,
          label = exp_label,
          start = Sys.Date() - 30,
          end = Sys.Date()
        ),
        "If dates are not in FY24, they will be mapped to the corresponding FY24 dates using fiscal calendar DT."
      )
    })
  })

  # ---- Lightweight status/progress UI (no shinyWidgets dependency) ----
  sim_status <- reactiveVal(list(state = "idle", pct = 0, msg = ""))

  output$sim_status_ui <- renderUI({
    s <- sim_status()
    state_badge <- switch(
      s$state,
      running = span(class = "badge text-bg-primary", "Running"),
      done = span(class = "badge text-bg-success", "Done"),
      error = span(class = "badge text-bg-danger", "Error"),
      span(class = "badge text-bg-secondary", "Idle")
    )

    tagList(
      state_badge,
      tags$div(style = "height: 8px;"),
      tags$progress(value = s$pct, max = 100, style = "width: 100%; height: 14px;"),
      if (nzchar(s$msg)) tags$div(class = "text-muted", style = "margin-top: 6px; font-size: 0.95rem;", s$msg)
    )
  })
 
  # ---- Run simulation ----
  simulation_results <- eventReactive(input$simulate, {
    req(input$selected_park)
    exp_name <- selected_exps_rv()
    validate(need(length(exp_name) > 0, "Select at least one experience to run the simulation."))
    exp_date_ranges <- get_exp_date_ranges(input, exp_name)
 
    sim_status(list(state = "running", pct = 10, msg = "Simulation started…"))

    withProgress(
      message = "Running simulation…",
      detail = "This can take a few minutes depending on number of runs.",
      value = 0,
      {
        incProgress(0.1)

        result <- tryCatch(
          run_simulation(
            park = as.numeric(input$selected_park),
            exp_name = exp_name,
            exp_date_ranges = exp_date_ranges,
            n_runs = as.numeric(input$n_runs),
            num_cores = 5
          ),
          error = function(e) {
            sim_status(list(state = "error", pct = 0, msg = conditionMessage(e)))
            stop(e)
          }
        )

        # Clean up any invalid metrics that can break downstream summaries/plots.
        # User request: remove rows where wEEx is negative.
        if ("wEEx" %in% names(result)) {
          result <- result[!(is.finite(result$wEEx) & result$wEEx < 0), , drop = FALSE]
        }

        incProgress(0.85)
        sim_status(list(state = "done", pct = 100, msg = "Simulation complete."))
        incProgress(0.05)
        result
      }
    )
  })
 
  # ---- Summary helpers ----
  park_summary <- reactive({
    sim_df <- simulation_results()
    req(nrow(sim_df) > 0)

    df_park <- sim_df %>% filter(Park == input$selected_park)
    req(nrow(df_park) > 0)

    total_actuals <- df_park %>%
      group_by(sim_run) %>%
      summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
      summarise(mean(Total_Actuals, na.rm = TRUE), .groups = "drop") %>%
      pull(1)

    df_overall <- df_park %>%
      group_by(sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))

    x <- df_overall$Inc_EARS_Pct
    x <- x[is.finite(x)]
    mu <- mean(x, na.rm = TRUE)

    set.seed(1)
    B <- 2000L
    boot_means <- replicate(B, mean(sample(x, size = length(x), replace = TRUE), na.rm = TRUE))
    ci <- stats::quantile(boot_means, probs = c(0.025, 0.975), na.rm = TRUE, names = FALSE)

    list(
      total_actuals = total_actuals,
      overall = df_overall,
      mu = mu,
      ci = ci
    )
  })

  output$summary_boxes <- renderUI({
    s <- park_summary()

    layout_column_wrap(
      width = 1 / 3,
      value_box(
        title = "Overall impact (mean)",
        value = sprintf("%.2f%%", s$mu)
      ),
      value_box(
        title = "95% CI (bootstrap)",
        value = sprintf("%.2f%% to %.2f%%", s$ci[1], s$ci[2])
      ),
      value_box(
        title = "Runs",
        value = as.character(input$n_runs)
      )
    )
  })

  output$summary_text <- renderUI({
    exps <- selected_exps_rv()
    if (length(exps) == 0) return(NULL)

    labels <- exp_data %>%
      filter(name %in% exps) %>%
      distinct(name, Repository.Offering.Name) %>%
      arrange(Repository.Offering.Name)

    park_labels <- c(
      "1" = "Magic Kingdom",
      "2" = "EPCOT",
      "3" = "Hollywood Studios",
      "4" = "Animal Kingdom"
    )
    park_name <- unname(park_labels[as.character(input$selected_park)])
    if (is.na(park_name) || is.null(park_name)) park_name <- as.character(input$selected_park)

    tagList(
      h5("Inputs"),
      tags$ul(
        tags$li(strong("Park: "), park_name),
        tags$li(strong("Experiences selected: "), paste(labels$Repository.Offering.Name, collapse = ", "))
      )
    )
  })
 
  # ---- Plot output selectors (plotly optional) ----
  # NOTE: keep these UI outputs unconditional (no req()), otherwise cards render empty
  # until after the simulation runs.
  get_histplot_height <- function() {
    sim_df <- tryCatch(simulation_results(), error = function(e) NULL)
    if (is.null(sim_df) || !is.data.frame(sim_df) || nrow(sim_df) == 0) return(900L)

    df_park <- sim_df %>% filter(Park == input$selected_park)
    total_actuals <- df_park %>%
      group_by(sim_run) %>%
      summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
      summarise(mean(Total_Actuals, na.rm = TRUE), .groups = "drop") %>%
      pull(1)

    df_name_simrun <- df_park %>%
      group_by(NAME, sim_run) %>%
      summarise(
        Sum_Inc_EARS = sum(Incremental_EARS, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(Sum_Inc_EARS_Pct = 100 * Sum_Inc_EARS / total_actuals)

    df_mean <- df_name_simrun %>%
      group_by(NAME) %>%
      summarise(Mean_Inc_EARS_Pct = mean(Sum_Inc_EARS_Pct, na.rm = TRUE), .groups = "drop")

    sim_inputs <- selected_exps_rv()
    if (length(sim_inputs) > 0) df_mean <- df_mean %>% filter(!NAME %in% sim_inputs)
    df_mean <- df_mean %>% filter(is.finite(Mean_Inc_EARS_Pct))

    n <- nrow(df_mean)
    # ~20-24px per bar works well in Dataiku; cap to avoid huge pages.
    as.integer(max(650, min(1400, 22 * n + 200)))
  }

  output$histplot_ui <- renderUI({
    h <- get_histplot_height()
    if (has_plotly) plotly::plotlyOutput("histplot", height = h) else plotOutput("histplot", height = h)
  })
  output$boxplot_park_ui <- renderUI({
    if (has_plotly) plotly::plotlyOutput("boxplot_park", height = 460) else plotOutput("boxplot_park", height = 460)
  })
  output$boxplot_lifestage_ui <- renderUI({
    if (has_plotly) plotly::plotlyOutput("boxplot_lifestage", height = 620) else plotOutput("boxplot_lifestage", height = 620)
  })
  output$boxplot_genre_ui <- renderUI({
    if (has_plotly) plotly::plotlyOutput("boxplot_genre", height = 620) else plotOutput("boxplot_genre", height = 620)
  })

  # ---- Plots ----
  # Cannibalization: Plotly bar chart, preserving all bars by widening categorical axis.
  if (has_plotly) {
    output$histplot <- plotly::renderPlotly({
      sim_df <- simulation_results()
      req(nrow(sim_df) > 0)

      df_park <- sim_df %>% filter(Park == input$selected_park)

      total_actuals <- df_park %>%
        group_by(sim_run) %>%
        summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
        summarise(mean(Total_Actuals, na.rm = TRUE), .groups = "drop") %>%
        pull(1)

      df_name_simrun <- df_park %>%
        group_by(NAME, sim_run) %>%
        summarise(
          Sum_Inc_EARS = sum(Incremental_EARS, na.rm = TRUE),
          Actuals = sum(Actual_EARS, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Sum_Inc_EARS_Pct = 100 * Sum_Inc_EARS / total_actuals)

      df_mean <- df_name_simrun %>%
        group_by(NAME) %>%
        summarise(
          Mean_Inc_EARS_Pct = mean(Sum_Inc_EARS_Pct, na.rm = TRUE),
          Actuals = sum(Actuals, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        arrange(desc(Actuals))

      # Remove experiences used as simulation inputs from cannibalization view
      sim_inputs <- selected_exps_rv()
      if (length(sim_inputs) > 0) df_mean <- df_mean %>% filter(!NAME %in% sim_inputs)

      df_mean <- df_mean %>% filter(is.finite(Mean_Inc_EARS_Pct))
      df_mean$NAME <- factor(df_mean$NAME, levels = unique(df_mean$NAME))

      n_cats <- nrow(df_mean)
      # Ensure bars remain visible by giving each category enough pixels.
      width_px <- max(1200, n_cats * 18)
      height_px <- get_histplot_height()

      plt <- plotly::plot_ly(
        data = df_mean,
        x = ~NAME,
        y = ~Mean_Inc_EARS_Pct,
        type = "bar",
        marker = list(color = "#2C7FB8"),
        hovertemplate = paste(
          "<b>%{x}</b>",
          "<br>Avg Incremental EARS: %{y:.2f}%",
          "<extra></extra>"
        )
      )

      plt <- plotly::layout(
        plt,
        height = height_px,
        width = width_px,
        bargap = 0.05,
        margin = list(l = 80, r = 20, t = 10, b = 320),
        xaxis = list(
          type = "category",
          automargin = TRUE,
          tickangle = 90,
          categoryorder = "array",
          categoryarray = as.character(levels(df_mean$NAME))
        ),
        yaxis = list(
          automargin = TRUE,
          ticksuffix = "%",
          tickformat = ".2f"
        )
      )

      plt <- plotly::config(plt, responsive = FALSE)

      # Force the widget element to the wider width so bars aren't sub-pixel thin.
      htmlwidgets::onRender(
        plt,
        sprintf(
          "function(el,x){el.style.width='%dpx'; if(window.Plotly){Plotly.Plots.resize(el);} }",
          width_px
        )
      )
    })
  } else {
    output$histplot <- renderPlot({
      sim_df <- simulation_results()
      req(nrow(sim_df) > 0)

      df_park <- sim_df %>% filter(Park == input$selected_park)

      total_actuals <- df_park %>%
        group_by(sim_run) %>%
        summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
        summarise(mean(Total_Actuals, na.rm = TRUE), .groups = "drop") %>%
        pull(1)

      df_name_simrun <- df_park %>%
        group_by(NAME, sim_run) %>%
        summarise(
          Sum_Inc_EARS = sum(Incremental_EARS, na.rm = TRUE),
          Actuals = sum(Actual_EARS, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Sum_Inc_EARS_Pct = 100 * Sum_Inc_EARS / total_actuals)

      df_mean <- df_name_simrun %>%
        group_by(NAME) %>%
        summarise(
          Mean_Inc_EARS_Pct = mean(Sum_Inc_EARS_Pct, na.rm = TRUE),
          Actuals = sum(Actuals, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        arrange(desc(Actuals))

      sim_inputs <- selected_exps_rv()
      if (length(sim_inputs) > 0) df_mean <- df_mean %>% filter(!NAME %in% sim_inputs)

      df_mean <- df_mean %>% filter(is.finite(Mean_Inc_EARS_Pct))
      df_mean$NAME <- factor(df_mean$NAME, levels = unique(df_mean$NAME))

      ggplot(df_mean, aes(x = NAME, y = Mean_Inc_EARS_Pct)) +
        geom_col(fill = "#2C7FB8") +
        geom_hline(yintercept = 0, linewidth = 0.4, color = "#6c757d") +
        labs(
          title = NULL,
          x = "Attraction",
          y = "Average Incremental EARS (% of Park Actuals)"
        ) +
        theme_minimal(base_family = "Century Gothic") +
        theme(
          plot.title = element_text(hjust = 0.5, family = "Century Gothic"),
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1, family = "Century Gothic"),
          axis.title.x = element_text(family = "Century Gothic"),
          axis.title.y = element_text(family = "Century Gothic"),
          legend.position = "none"
        ) +
        scale_y_continuous(labels = scales::percent_format(scale = 1))
    })
  }
 
  output$boxplot_park <- (if (has_plotly) plotly::renderPlotly else renderPlot)({
    sim_df <- simulation_results()
    req(nrow(sim_df) > 0)
 
    df_park <- sim_df %>% filter(Park == input$selected_park) %>%
      mutate(Incremental_EARS = ifelse(is.finite(Incremental_EARS), Incremental_EARS, 0))
 
    total_actuals <- df_park %>%
      group_by(sim_run) %>%
      summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
      summarise(mean(Total_Actuals, na.rm = TRUE)) %>%
      pull(1)
 
    df_box <- df_park %>%
      group_by(sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(!is.na(Inc_EARS_Pct) & !is.nan(Inc_EARS_Pct))
 
    if (nrow(df_box) == 0 || is.null(total_actuals) || is.na(total_actuals) || total_actuals == 0) {
      plot.new()
      text(0.5, 0.5, "No data available for boxplot", cex = 1.5)
      return()
    }
 
    # Bootstrap CI for the MEAN overall impact across sim runs
    x <- df_box$Inc_EARS_Pct
    x <- x[is.finite(x)]
    if (!length(x)) {
      plot.new()
      text(0.5, 0.5, "No data available", cex = 1.5)
      return()
    }

    set.seed(1)
    B <- 2000L
    boot_means <- replicate(B, mean(sample(x, size = length(x), replace = TRUE), na.rm = TRUE))
    ci <- stats::quantile(boot_means, probs = c(0.025, 0.975), na.rm = TRUE, names = FALSE)
    mu <- mean(x, na.rm = TRUE)

    p <- ggplot(df_box, aes(x = Inc_EARS_Pct)) +
      geom_histogram(bins = min(30L, max(10L, length(x))), fill = "#5DADE2", color = "white", alpha = 0.9) +
      geom_vline(xintercept = mu, linewidth = 0.9, color = "#1B4F72") +
      geom_vline(xintercept = ci[1], linetype = "dashed", linewidth = 0.9, color = "#1B4F72") +
      geom_vline(xintercept = ci[2], linetype = "dashed", linewidth = 0.9, color = "#1B4F72") +
      annotate("text", x = mu, y = Inf, label = "Mean", vjust = 1.2, size = 3, family = "Century Gothic", color = "#1B4F72") +
      annotate("text", x = ci[1], y = Inf, label = "2.5%", vjust = 2.6, size = 3, family = "Century Gothic", color = "#1B4F72") +
      annotate("text", x = ci[2], y = Inf, label = "97.5%", vjust = 2.6, size = 3, family = "Century Gothic", color = "#1B4F72") +
      labs(
        title = NULL,
        x = "Incremental EARS (% of Park Actuals)",
        y = "Count of simulation runs"
      ) +
      theme_minimal(base_family = "Century Gothic") +
      theme(
        plot.title = element_text(hjust = 0.5, family = "Century Gothic"),
        legend.position = "none",
        axis.title.x = element_text(family = "Century Gothic"),
        axis.title.y = element_text(family = "Century Gothic"),
        axis.text.x = element_text(family = "Century Gothic"),
        axis.text.y = element_text(family = "Century Gothic")
      ) +
      scale_x_continuous(labels = scales::percent_format(scale = 1))

    if (has_plotly) plotly::layout(plotly::ggplotly(p, tooltip = c("x", "y")), height = 460, margin = list(l = 60, r = 20, t = 20, b = 60)) else p
  })
 
  output$boxplot_lifestage <- (if (has_plotly) plotly::renderPlotly else renderPlot)({
    sim_df <- simulation_results()
    req(nrow(sim_df) > 0)
 
    df_park <- sim_df %>% filter(Park == input$selected_park)
 
    total_actuals <- df_park %>%
      group_by(sim_run) %>%
      summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
      summarise(mean(Total_Actuals, na.rm = TRUE)) %>%
      pull(1)
 
    df_life <- df_park %>%
      group_by(LifeStage, sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))
 
    df_genre <- df_park %>%
      group_by(Genre, sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))
 
    df_box <- df_park %>%
      group_by(sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))
 
    y_range <- range(c(df_box$Inc_EARS_Pct, df_life$Inc_EARS_Pct, df_genre$Inc_EARS_Pct), na.rm = TRUE)
 
    lifestage_labels <- c(
      "1" = "Family 0-5",
      "2" = "Family 6-9",
      "3" = "Family 10-17",
      "4" = "Young Adult (18-34)",
      "5" = "Adult 35+"
    )
 
    df_life$LifeStage <- factor(df_life$LifeStage, levels = sort(unique(df_life$LifeStage)))
 
    p <- ggplot(df_life, aes(x = LifeStage, y = Inc_EARS_Pct, fill = LifeStage)) +
      geom_boxplot() +
      labs(
        title = NULL,
        x = "LifeStage",
        y = "Incremental EARS (% of Park Actuals)"
      ) +
      theme_minimal(base_family = "Century Gothic") +
      theme(
        plot.title = element_text(hjust = 0.5, family = "Century Gothic"),
        legend.position = "none",
        axis.text.x = element_text(angle = 45, hjust = 1, family = "Century Gothic"),
        axis.title.x = element_text(family = "Century Gothic"),
        axis.title.y = element_text(family = "Century Gothic"),
        axis.text.y = element_text(family = "Century Gothic")
      ) +
      scale_fill_brewer(palette = "Dark2") +
      scale_x_discrete(labels = lifestage_labels) +
      scale_y_continuous(labels = scales::percent_format(scale = 1), limits = y_range)

    if (has_plotly) plotly::layout(plotly::ggplotly(p, tooltip = c("x", "y")), height = 620, margin = list(l = 80, r = 20, t = 20, b = 80)) else p
  })
 
  output$boxplot_genre <- (if (has_plotly) plotly::renderPlotly else renderPlot)({
    sim_df <- simulation_results()
    req(nrow(sim_df) > 0)
    req("Genre" %in% names(sim_df))
 
    df_park <- sim_df %>% filter(Park == input$selected_park)
 
    total_actuals <- df_park %>%
      group_by(sim_run) %>%
      summarise(Total_Actuals = sum(Actual_EARS, na.rm = TRUE), .groups = "drop") %>%
      summarise(mean(Total_Actuals, na.rm = TRUE)) %>%
      pull(1)
 
    df_genre <- df_park %>%
      group_by(Genre, sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))
 
    df_life <- df_park %>%
      group_by(LifeStage, sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))
 
    df_box <- df_park %>%
      group_by(sim_run) %>%
      summarise(Inc_EARS = sum(Incremental_EARS, na.rm = TRUE), .groups = "drop") %>%
      mutate(Inc_EARS_Pct = 100 * Inc_EARS / total_actuals) %>%
      filter(is.finite(Inc_EARS_Pct))
 
    y_range <- range(c(df_box$Inc_EARS_Pct, df_life$Inc_EARS_Pct, df_genre$Inc_EARS_Pct), na.rm = TRUE)
 
    df_genre$Genre <- factor(df_genre$Genre, levels = sort(unique(df_genre$Genre)))
 
    p <- ggplot(df_genre, aes(x = Genre, y = Inc_EARS_Pct, fill = Genre)) +
      geom_boxplot() +
      labs(
        title = NULL,
        x = "Genre",
        y = NULL
      ) +
      theme_minimal(base_family = "Century Gothic") +
      theme(
        legend.position = "none",
        axis.text.x = element_text(angle = 45, hjust = 1, family = "Century Gothic"),
        axis.title.x = element_text(family = "Century Gothic"),
        axis.title.y = element_text(family = "Century Gothic"),
        axis.text.y = element_text(family = "Century Gothic")
      ) +
      scale_fill_brewer(palette = "Dark2") +
      scale_y_continuous(labels = scales::percent_format(scale = 1), limits = y_range)

    if (has_plotly) plotly::layout(plotly::ggplotly(p, tooltip = c("x", "y")), height = 620, margin = list(l = 80, r = 20, t = 20, b = 80)) else p
  })
 
  # (Details tab removed per request)

  # ---- Download ----
  output$download_sim <- downloadHandler(
    filename = function() {
      paste0("simulation_results_", Sys.Date(), ".csv")
    },
    content = function(file) {
      write.csv(simulation_results(), file, row.names = FALSE)
    }
  )

  # (Summary download removed per request)
}
 
# UI is not included in your snippet; keep your existing ui <- ... definition
# and ensure it has: input$selected_park, input$n_runs, input$simulate, and output placeholders.
 
# shinyApp(ui = ui, server = server)

# In some runtimes (e.g. Dataiku/Shiny wrappers), `server.R` must *return* the server function.
server
