library(dataiku)
library(foreach)
library(doParallel)

meta_prepared_new <- dkuReadDataset("MetaDataFinalTaxonomy")
AttQTR_new <- dkuReadDataset("Attendance")
QTRLY_GC_new <- dkuReadDataset("GuestCarriedFinal")
SurveyDataCheck_new <- dkuReadDataset("FY24_prepared")
POG_new <- dkuReadDataset("Charcter_Entertainment_POG")
SurveyData_new <- dkuReadDataset("FY24_prepared")

n_runs <- 10
num_cores <- 5
cl <- makeCluster(num_cores)
registerDoParallel(cl)


# Reading in data and setting initial parameters for loop over each quarter
SurveyData <- SurveyData_new
replacementsave<-c()
library(nnet)
library(gtools)
verbose <- FALSE
maxFQ<-max(SurveyData$fiscal_quarter)
minFQ<-min(SurveyData$fiscal_quarter)
SurveyCheckFinal<-c()
CountCheckFinal<-c()

maxFQ<-4

# Parallel loop
EARSTotal_list <- foreach(
  run = 1:n_runs,
  .combine = rbind,
  .packages = c("dataiku", "nnet", "sqldf", "data.table", "dplyr", "reshape2")
) %dopar% {
 EARSFinal_Final<-c()
    EARSx<-c()
    EARSTotal<-c()
    #set.seed(run)

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
park <- 1
exp_name <- c("tron")

# Define date ranges for each experience
exp_date_ranges <- list(
  tron = c("2023-10-11", "2025-09-02")
)

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
                                               SurveyDataSim<-SurveyData
                     #meta_prepared_new <- meta_prepared_new[!meta_prepared_new$Variable %in% removed_rides, ]
# Quarter range (DT read removed; values were hardcoded anyway)
FQ <- 1
maxFQ <- 4

# Load once per simulation run (was loaded each quarter)
EARS <- dkuReadDataset("EARS_Taxonomy")
while(FQ<maxFQ+1){
SurveyData <- SurveyDataSim
#Setting the FY but this could be changed to be read in automatically from the data
yearauto<-2024

    #The metadata is a lookup table for all of the loops so the code knows what data to grab for each element

    meta_prepared <-  meta_prepared_new

#meta_prepared$Category1<-paste(meta_prepared$name,meta_prepared$R_Park,sep="_")
AttQTR <- AttQTR_new
#AttQTR<-AttQTR %>%
#  group_by(Park,FY) %>%
#  summarise(Att = sum(Att),FirstClick = sum(FirstClick))
#AttQTR$Factor<-AttQTR$Att/AttQTR$FirstClick

QTRLY_GC <- QTRLY_GC_new
#QTRLY_GC$GC[is.na(QTRLY_GC$GC)]<-0
#QTRLY_GC<-QTRLY_GC %>%
#  group_by(Park,FY,NAME) %>%
#  summarise(GC = sum(as.numeric(GC)))

SurveyData<-SurveyData[SurveyData$fiscal_quarter==FQ,]

    #This code is for setting up the Pecent of Gate (POG) calculation for things with no Guest Carried
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
meta_preparedPOG

AttQTR<-AttQTR[AttQTR$FQ ==FQ,]
QTRLY_GC<-QTRLY_GC[QTRLY_GC$FQ ==FQ,]

meta_preparedPOG<-merge(meta_preparedPOG,AttQTR, by='Park')
meta_preparedPOG$NEWPOG<-meta_preparedPOG$expd*meta_preparedPOG$Factor


meta_preparedPOG

meta_preparedPOG$NEWGC<-meta_preparedPOG$Att*meta_preparedPOG$NEWPOG
metaPOG<-merge(meta_prepared,meta_preparedPOG[,c("Park","name","NEWGC")],by=c('name', "Park"))

library('sqldf')
meta_prepared2<-sqldf("select a.*,b.GC as QuarterlyGuestCarried from meta_prepared a left join
QTRLY_GC b on a.name=b.name and a.Park = b.Park")
setDT(meta_prepared2)
setDT(metaPOG)
meta_prepared2[metaPOG, on=c("name","Park"), QuarterlyGuestCarried:=i.NEWGC]
meta_prepared2

metadata<-data.frame(meta_prepared2)
names(SurveyData) <- tolower(names(SurveyData))
FY<-yearauto
SurveyData[is.na(SurveyData)]<-0

SurveyData<-cbind(SurveyData,FY)
SurveyData<-SurveyData[SurveyData$fiscal_quarter==FQ,]

# The following code is a multinomial logistic regression which is used to get weights for the EARS statistic.
# Using a baseball analogy this is where we translate 'hits' (experiences for characters, entertainment and rides) into 'runs'
# These 'runs' are abstract runs but when we add them all up we can then translate those runs into 'wins' (overall excellents) for each experience
# The multinomial Logistic Regression is modelled for each of the 4 parks separately

#Play

five_Play<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Type == "Play",2]]] == 5, na.rm=TRUE)
four_Play<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Type == "Play",2]]] == 4, na.rm=TRUE)
three_Play<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Type == "Play",2]]] == 3, na.rm=TRUE)
two_Play<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Type == "Play",2]]] == 2, na.rm=TRUE)
one_Play<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Type == "Play",2]]] == 1, na.rm=TRUE)

weights_Play<-data.frame(cbind(q1=SurveyData$q1,one_Play, two_Play , three_Play, four_Play, five_Play, Park = SurveyData$park, FY = SurveyData$FY))



#Show

five_Show<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show" ,2]]] == 5, na.rm=TRUE)
four_Show<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show" ,2]]] == 4, na.rm=TRUE)
three_Show<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show" ,2]]] == 3, na.rm=TRUE)
two_Show<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show" ,2]]] == 2, na.rm=TRUE)
one_Show<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Show" ,2]]] == 1, na.rm=TRUE)

weights_Show<-data.frame(cbind(q1=SurveyData$q1,one_Show, two_Show , three_Show, four_Show, five_Show, Park = SurveyData$park, FY = SurveyData$FY))

#Preferred
five_Preferred<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Genre == "Flaship" | metadata$Genre == "Anchor",2]]] == 5, na.rm=TRUE)
four_Preferred<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Genre == "Flaship" | metadata$Genre == "Anchor",2]]] == 4, na.rm=TRUE)
three_Preferred<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Genre == "Flaship" | metadata$Genre == "Anchor",2]]] == 3, na.rm=TRUE)
two_Preferred<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Genre == "Flaship" | metadata$Genre == "Anchor",2]]] == 2, na.rm=TRUE)
one_Preferred<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[ metadata$Genre == "Flaship" | metadata$Genre == "Anchor",2]]] == 1, na.rm=TRUE)

weights_Preferred<-data.frame(cbind(q1=SurveyData$q1,one_Preferred, two_Preferred , three_Preferred, four_Preferred, five_Preferred, Park = SurveyData$park, FY = SurveyData$FY))


#Rides / Att

five_RA<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride"  ,2]]] == 5, na.rm=TRUE)
four_RA<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride" ,2]]] == 4, na.rm=TRUE)
three_RA<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride" ,2]]] == 3, na.rm=TRUE)
two_RA<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride" ,2]]] == 2, na.rm=TRUE)
one_RA<-rowSums(SurveyData[,  names(SurveyData)[names(SurveyData) %in% metadata[metadata$Type == "Ride" ,2]]] == 1, na.rm=TRUE)


    #Magic Kingdom Weights

CouldntRide<-c()
Experience<-c()
for(i in 1:length(metadata[,2])){
CouldntRide<-c(CouldntRide,metadata[,18][i])
    Experience<-c(Experience,metadata[which(!is.na(metadata[,18])),2][i])

    }
CouldntRide <- tolower(CouldntRide[!is.na(CouldntRide)])
Experience <- tolower(Experience[!is.na(Experience)])


cantgeton<-as.numeric(SurveyData[,Experience[1]] ==0 & SurveyData[,CouldntRide[1]] ==1 & SurveyData$ovpropex <6 )
for(i in 2:length(Experience)){
cantgeton<-cantgeton+as.numeric(SurveyData[,Experience[i]] ==0 & SurveyData[,CouldntRide[i]] ==1  & SurveyData$ovpropex <6 )

    }
cant<-data.frame(cbind(ovpropex=SurveyData$ovpropex,cantgeton, Park = SurveyData$park, FY = SurveyData$FY))

weights_RA<-data.frame(cbind(ovpropex=SurveyData$ovpropex,one_RA, two_RA , three_RA, four_RA, five_RA, Park = SurveyData$park, FY = SurveyData$FY))
weights<-cbind(weights_Play, weights_Show, weights_RA, cant,weights_Preferred)
weights$ovpropex <- relevel(factor(weights$ovpropex), ref = "5")
weights<-weights[weights$Park ==1&weights$FY ==yearauto,]


test <- multinom(ovpropex ~ cantgeton+  one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+ one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights)
odds<-exp(confint(test, level=0.995))

z1 <- apply(odds, 3L, c)
z2 <- expand.grid(dimnames(odds)[1:2])
data.frame(z2, z1)

jz<-glm((ovpropex==5) ~   one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+ one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights, family="binomial")
EXPcol<-exp(jz$coefficients[-1])

weightedEEs <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.8 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][1:15,], X5=EXPcol[1:15])
weightedEEs[weightedEEs$Var1 == "five_Play" ,3:7] <-weightedEEs[weightedEEs$Var1 == "five_Play" ,3:7]*1
weightedEEs[weightedEEs$Var1 == "five_Show" ,3:7] <-weightedEEs[weightedEEs$Var1 == "five_Show" ,3:7]*1
#Attends <- data.frame(data.frame(z2, log(z1))[which(data.frame(z2, z1)$Var2 == "97.5 %" & data.frame(z2, z1)$Var1 == "Attend" ),], X5=rep(1,1), Park=1)
cantride <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.8 %" & data.frame(z2, z1)$Var1 == "cantgeton" ),], X5=rep(1,1))
weightsPref1<-data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.8 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][16:20,], X5=EXPcol[16:20])


    #EPCOT Weights


weights<-cbind(weights_Play, weights_Show, weights_RA, cant,weights_Preferred)
weights$ovpropex <- relevel(factor(weights$ovpropex), ref = "5")
weights<-weights[weights$Park ==2&weights$FY ==yearauto,]
test <- multinom(ovpropex ~cantgeton+  one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred , data = weights)

odds<-exp(confint(test, level=0.99))




z1 <- apply(odds, 3L, c)
z2 <- expand.grid(dimnames(odds)[1:2])
jz<-glm((ovpropex==5) ~   one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+ one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights, family="binomial")
EXPcol<-exp(jz$coefficients[-1])

weightedEEs_EP <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.5 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][1:15,], X5=EXPcol[1:15])
weightedEEs_EP[weightedEEs_EP$Var1 == "five_Play" ,3:7] <-weightedEEs_EP[weightedEEs_EP$Var1 == "five_Play" ,3:7]*1
weightedEEs_EP[weightedEEs_EP$Var1 == "five_Show" ,3:7] <-weightedEEs_EP[weightedEEs_EP$Var1 == "five_Show" ,3:7]*1
weightedEEs<- rbind(weightedEEs,weightedEEs_EP)
cantride_EP <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.5 %" & data.frame(z2, z1)$Var1 == "cantgeton" ),], X5=rep(1,1))
cantride<-rbind(cantride,cantride_EP)
weightsPref2<-data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "99.5 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][16:20,], X5=EXPcol[16:20])

    #STUDIOS Weights


weights<-cbind(weights_Play, weights_Show, weights_RA, cant,weights_Preferred)
weights$ovpropex <- relevel(factor(weights$ovpropex), ref = "5")
weights<-weights[weights$Park ==3&weights$FY ==yearauto,]
test <- multinom(ovpropex ~ cantgeton+  one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred , data = weights)

odds<-exp(confint(test, level=0.80))




z1 <- apply(odds, 3L, c)
z2 <- expand.grid(dimnames(odds)[1:2])
jz<-glm((ovpropex==5) ~   one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+ one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights, family="binomial")
EXPcol<-exp(jz$coefficients[-1])

weightedEEs_ST <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "90 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][1:15,], X5=EXPcol[1:15])
weightedEEs_ST[weightedEEs_ST$Var1 == "five_Play" ,3:7] <-weightedEEs_ST[weightedEEs_ST$Var1 == "five_Play" ,3:7]*1
weightedEEs_ST[weightedEEs_ST$Var1 == "five_Show" ,3:7] <-weightedEEs_ST[weightedEEs_ST$Var1 == "five_Show" ,3:7]*1
weightedEEs<- rbind(weightedEEs,weightedEEs_ST)
cantride_ST <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "90 %" & data.frame(z2, z1)$Var1 == "cantgeton" ),], X5=rep(1,1))
cantride<-rbind(cantride,cantride_ST)
weightsPref3<-data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "90 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][16:20,], X5=EXPcol[16:20])




    #Animal Kingdom Weights


weights<-cbind(weights_Play, weights_Show, weights_RA, cant,weights_Preferred)
weights$ovpropex <- relevel(factor(weights$ovpropex), ref = "5")
weights<-weights[weights$Park ==4&weights$FY ==yearauto,]
test <- multinom(ovpropex ~ cantgeton+  one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred , data = weights)
odds<-exp(confint(test, level=0.70))




z1 <- apply(odds, 3L, c)
z2 <- expand.grid(dimnames(odds)[1:2])
jz<-glm((ovpropex==5) ~   one_Play + two_Play + three_Play + four_Play + five_Play  +one_Show + two_Show + three_Show + four_Show + five_Show + one_RA + two_RA + three_RA + four_RA + five_RA+ one_Preferred + two_Preferred + three_Preferred + four_Preferred + five_Preferred, data = weights, family="binomial")
EXPcol<-exp(jz$coefficients[-1])

weightedEEs_AK <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "15 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][1:15,], X5=EXPcol[1:15])
weightedEEs_AK[weightedEEs_AK$Var1 == "five_Play" ,3:7] <-weightedEEs_AK[weightedEEs_AK$Var1 == "five_Play" ,3:7]*1
weightedEEs_AK[weightedEEs_AK$Var1 == "five_Show" ,3:7] <-weightedEEs_AK[weightedEEs_AK$Var1 == "five_Show" ,3:7]*1
weightedEEs<- rbind(weightedEEs,weightedEEs_AK)
weightedEEs2<-data.frame(FY=yearauto,weightedEEs)
cantride_AK <- data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "15 %" & data.frame(z2, z1)$Var1 == "cantgeton" ),], X5=rep(1,1))
cantride2<-data.frame(FY=yearauto,rbind(cantride,cantride_AK))
weightsPref4<-data.frame(data.frame(z2, z1)[which(data.frame(z2, z1)$Var2 == "15 %" & data.frame(z2, z1)$Var1 != "(Intercept)" & data.frame(z2, z1)$Var1 != "Attend" & data.frame(z2, z1)$Var1 != "cantgeton" ),][16:20,], X5=EXPcol[16:20])

weights22<-rbind(weightedEEs,weightsPref1,weightsPref2,weightsPref3,weightsPref4)
CantRideWeight22<-cantride2[,-1]
    CantRideWeight22
  #  CantRideWeight22[!is.na(CantRideWeight22),]<-0
if (verbose) print(weights22)
    #The weights above (weightedEEs) are now multiplied to the experience data so we can convert the data into overall excellent experiences
    #The Cant Ride weights are a separate weight for Guests that wanted to ride but couldnt.  Think of these weights as runs against our team


#weightedEEs$X1[weightedEEs$X1>10]<-1
#weightedEEs$X2[weightedEEs$X2>10]<-1
#weightedEEs$X3[weightedEEs$X3>10]<-1
#weightedEEs$X4[weightedEEs$X4>10]<-1
SurveyData<-SurveyData[SurveyData$fiscal_quarter==FQ,]
names(SurveyData) <- tolower(names(SurveyData))

FY<-yearauto
SurveyData[is.na(SurveyData)]<-0
SurveyData<-cbind(SurveyData,FY)
    #Rescaling Lifetage
    #1 - Family Oldest <6
    #2 - Family 6 to 9
    #3 - Family 10 to 17
    #4 - Young Adult (18-24)
    #5 - Adult 25+



metadata$POG[(metadata$Type =='Show'& is.na(metadata$POG) |metadata$Type =='Play'& is.na(metadata$POG) )]<-0
metadata<-metadata[!is.na(metadata$Category1)|metadata$Type == 'Ride',]

SurveyData22<-SurveyData
CountData22<-SurveyData22

metadata[,2]<-tolower(metadata[,2])
metadata[,3]<-tolower(metadata[,3])
metadata[,18]<-tolower(metadata[,18])
metadata[,19]<-tolower(metadata[,19])

rideagain <- metadata[,3]
rideexp <- metadata[!is.na(metadata[,3]),2]
rideexp_fix <- metadata[!is.na(metadata[,2]),2]

rideagain <- rideagain[!is.na(rideagain)]
rideexp <- rideexp[!is.na(rideexp)]
rideexp_fix <- rideexp_fix[!is.na(rideexp_fix)]


for(i in 1:length(rideagain)){
SurveyData22[which(SurveyData22[,rideexp[i]] !=0 & SurveyData22[,rideagain[i]] ==0),rideagain[i]]<-1
    }

ridesexp_full <- metadata[,2]
ridesexp_full <- ridesexp_full[!is.na(ridesexp_full) & grepl("ridesexp_", ridesexp_full, fixed = TRUE)]
ridesexp <- sub("^.*ridesexp_", "", ridesexp_full)
ridesexp <- sub(".*_", "", ridesexp)



entexp_full <- metadata[,2]
entexp_full <- entexp_full[!is.na(entexp_full) & grepl("entexp_", entexp_full, fixed = TRUE)]
entexp <- sub("^.*entexp_", "", entexp_full)
entexp <- sub(".*_", "", entexp)

charexp_full <- metadata[,2]
charexp_full <- charexp_full[!is.na(charexp_full) & grepl("charexp_", charexp_full, fixed = TRUE)]
charexp_after_prefix <- sub("^.*charexp_", "", charexp_full)
howexp <- paste("charhow", charexp_after_prefix, sep = "_")
howexp <- howexp[howexp != "charhow_NA"]
charexp <- sub(".*_", "", charexp_after_prefix)

for(i in 1:length(charexp)){
SurveyData22[,as.character(colnames(SurveyData22)[grepl('charexp_', colnames(SurveyData22))])[i]]<-SurveyData22[,as.character(colnames(SurveyData22)[grepl('charexp_', colnames(SurveyData22))])[i]]*as.numeric(SurveyData22[,colnames(SurveyData22) == howexp[i]]<2|SurveyData22[,colnames(SurveyData22) == howexp[i]]==3|SurveyData22[,colnames(SurveyData22) == howexp[i]]==4)
    }

rideagainx <- metadata[,18]
RIDEX <- metadata[!is.na(metadata[,18]),2]
rideagainx <- tolower(rideagainx[!is.na(rideagainx)])
RIDEX <- tolower(RIDEX[!is.na(RIDEX)])



for(i in 1:length(RIDEX)){
SurveyData22[which(SurveyData22[,RIDEX[i]] ==0 & SurveyData22[,rideagainx[i]] ==1 &SurveyData22$ovpropex<6),RIDEX[i]]<- -1

    }

new_cols <- unique(c(
  paste0(ridesexp, "2"), paste0(ridesexp, "3"),
  paste0(entexp, "2"), paste0(entexp, "3"),
  paste0(charexp, "2"), paste0(charexp, "3")
))
SurveyData22[, new_cols] <- 0

idx_non_na <- which(!is.na(metadata[,2]))
 rideagain_fix<-metadata[,3]
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
        # Suffix "2" (Ride): offset = 0

        if (length(rows) > 0) {
          SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15+10, j + 2]
        }
      }
    }
    # Ent assignments
    idx_ent <- which(metadata$Type[idx_non_na] == "Play" & metadata$Park[idx_non_na] == park)
    for (k in idx_ent) {
      if (!is.na(rideexp_fix[k])) {
        colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
        rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
        # Suffix "2" (Ent): offset = 0

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
        # Suffix "2" (Flaship/Anchor): offset = 40

        if (length(rows) > 0) {
          SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 5+60 , j + 2]
        }
      }
    }
    # Show assignments
    idx_show <- which(metadata$Type[idx_non_na] == "Show" & metadata$Genre[idx_non_na] != "Anchor" & metadata$Park[idx_non_na] == park)
    for (k in idx_show) {
      if (!is.na(rideexp_fix[k])) {
        colname2 <- paste0(sub("^[^_]+_[^_]+_", "", rideexp_fix[k]), "2")
        rows <- idx_pj[which(SurveyData22[idx_pj, rideexp_fix[k]] == i)]
        # Suffix "2" (Show): offset = 15

        if (length(rows) > 0) {
          SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15+5, j + 2]
        }
      }
    }
  }
  if (verbose) print(c(i, j))
}

for (i in 1:4){
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
        # Suffix "2" (Ride): offset = 0
        if (length(rows) > 0) {
          SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15+10, j + 2]
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
              print(weights22[i + (park - 1) * 15+10, j + 2])
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
        # Suffix "2" (Flaship/Anchor): offset = 40
        if (length(rows) > 0) {
          SurveyData22[rows, colname2] <- weights22[i + (park - 1) *  5+60 , j + 2]
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
        # Suffix "2" (Show): offset = 15
        if (length(rows) > 0) {
          SurveyData22[rows, colname2] <- weights22[i + (park - 1) * 15+5, j + 2]
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
        # Suffix "2" (Play): offset = 0
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
    library(data.table)
    
    # Build mapping from metadata variable to short "base" (e.g., safaris -> safaris2/safaris3)
    meta_var <- as.character(metadata[,2])
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
    
    if (FALSE) {
    jorja<-c()
for(ii in 1:length(metadata[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadata[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
    }
jorja <- jorja[metadata$Type== "Ride"]

Ride_Runs20<-c()
for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}
Ride_Runs20<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
colnames(Ride_Runs20)

jorja<-c()
for(ii in 1:length(metadata[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadata[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))

   }
jorja <- jorja[metadata$Type== "Ride"]

Ride_Against20<-c()
for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}

Ride_Against20<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))





Show_Runsx<-c()
Show_Runs20<-Ride_Runs20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
      }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Show_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Runsx[Show_Runsx$Park!=1,-c(1:3)]<-0
    if(dim(Show_Runsx)[2]==4){test<-Show_Runsx[,4]}else{
    test<-apply(Show_Runsx[,-c(1:3)], 1, sum)}
    Show_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_Runs20<-data.frame(Show_Runs20,Show_Runsx1)

}

jorja<-c()
for(ii in 1:length(metadata[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadata[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))

   }
jorja <- jorja[metadata$Type== "Ride"]

Ride_Against20<-c()
for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}

Ride_Against20<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))






Show_Runsx<-c()
Show_Runs20<-Ride_Runs20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  &metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
      }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Show_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Runsx[Show_Runsx$Park!=1,-c(1:3)]<-0
    if(dim(Show_Runsx)[2]==4){test<-Show_Runsx[,4]}else{
    test<-apply(Show_Runsx[,-c(1:3)], 1, sum)}
    Show_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_Runs20<-data.frame(Show_Runs20,Show_Runsx1)

}

Show_Againstx<-c()
Show_Against20<-Ride_Runs20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
     }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Show_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Againstx[Show_Againstx$Park!=1,-c(1:3)]<-0
    if(dim(Show_Againstx)[2]==4){test<-Show_Againstx[,4]}else{
    test<-apply(Show_Againstx[,-c(1:3)], 1, sum)}
    Show_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_Against20<-data.frame(Show_Against20,Show_Againstx1)

}

Play_Againstx<-c()
Play_Against20<-Ride_Runs20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==1 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play" &metadata$Park ==1 ])[zz]),]

    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))

    }

jorja<-jorja[jorja!=""]

for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Play_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Againstx[Play_Againstx$Park!=1,-c(1:3)]<-0
    if(dim(Play_Againstx)[2]==4){test<-Play_Againstx[,4]}else{
    test<-apply(Play_Againstx[,-c(1:3)], 1, sum)}
    Play_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==1 ])[zz])
Play_Against20<-data.frame(Play_Against20,Play_Againstx1)

}

Show_Runsx<-c()
length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2 ]))
for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"   &metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))

    }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Show_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Runsx[Show_Runsx$Park!=2,-c(1:3)]<-0
    if(dim(Show_Runsx)[2]==4){test<-Show_Runsx[,4]}else{
    test<-apply(Show_Runsx[,-c(1:3)], 1, sum)}
    Show_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2 ])[zz])
Show_Runs20<-data.frame(Show_Runs20,Show_Runsx1)

}

Play_Runsx<-c()
Play_Runs20<-Ride_Runs20[,1:3]
length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==1  ]))
for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play" &metadata$Park ==1  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"  &metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
      }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Play_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Runsx[Play_Runsx$Park!=1,-c(1:3)]<-0
    if(dim(Play_Runsx)[2]==4){test<-Play_Runsx[,4]}else{
    test<-apply(Play_Runsx[,-c(1:3)], 1, sum)}
    Play_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play" &metadata$Park ==1 ])[zz])

    Play_Runs20<-data.frame(Play_Runs20,Play_Runsx1)

}

Show_Againstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
       }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Show_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Againstx[Show_Againstx$Park!=2,-c(1:3)]<-0
    if(dim(Show_Againstx)[2]==4){test<-Show_Againstx[,4]}else{
    test<-apply(Show_Againstx[,-c(1:3)], 1, sum)}
    Show_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_Against20<-data.frame(Show_Against20,Show_Againstx1)

}

Play_Runsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"  &metadata$Park ==2  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"  &metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
      }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Play_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Runsx[Play_Runsx$Park!=2,-c(1:3)]<-0
    if(dim(Play_Runsx)[2]==4){test<-Play_Runsx[,4]}else{
    test<-apply(Play_Runsx[,-c(1:3)], 1, sum)}
    Play_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"  &metadata$Park ==2 ])[zz])
Play_Runs20<-data.frame(Play_Runs20,Play_Runsx1)

}




Play_Againstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==2 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"  &metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
       }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Play_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Againstx[Play_Againstx$Park!=2,-c(1:3)]<-0
    if(dim(Play_Againstx)[2]==4){test<-Play_Againstx[,4]}else{
    test<-apply(Play_Againstx[,-c(1:3)], 1, sum)}
    Play_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"  &metadata$Park ==2 ])[zz])
Play_Against20<-data.frame(Play_Against20,Play_Againstx1)

}

Show_Runsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==3 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"    &metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
      }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Show_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Runsx[Show_Runsx$Park!=3,-c(1:3)]<-0
    if(dim(Show_Runsx)[2]==4){test<-Show_Runsx[,4]}else{
    test<-apply(Show_Runsx[,-c(1:3)], 1, sum)}
    Show_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==3 ])[zz])
Show_Runs20<-data.frame(Show_Runs20,Show_Runsx1)

}

Show_Againstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==3  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
      }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Show_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Againstx[Show_Againstx$Park!=3,-c(1:3)]<-0
    if(dim(Show_Againstx)[2]==4){test<-Show_Againstx[,4]}else{
    test<-apply(Show_Againstx[,-c(1:3)], 1, sum)}
    Show_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_Against20<-data.frame(Show_Against20,Show_Againstx1)

}

Play_Runsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==3  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
     }
jorja<-jorja[jorja!=""]

for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Play_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Runsx[Play_Runsx$Park!=3,-c(1:3)]<-0
    if(dim(Play_Runsx)[2]==4){test<-Play_Runsx[,4]}else{
    test<-apply(Play_Runsx[,-c(1:3)], 1, sum)}
    Play_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz])
Play_Runs20<-data.frame(Play_Runs20,Play_Runsx1)

}




Play_Againstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==3 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
      }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Play_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Againstx[Play_Againstx$Park!=3,-c(1:3)]<-0
    if(dim(Play_Againstx)[2]==4){test<-Play_Againstx[,4]}else{
    test<-apply(Play_Againstx[,-c(1:3)], 1, sum)}
    Play_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz])
Play_Against20<-data.frame(Play_Against20,Play_Againstx1)

}

Show_Runsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==4 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"    &metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
      }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Show_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Runsx[Show_Runsx$Park!=4,-c(1:3)]<-0
    if(dim(Show_Runsx)[2]==4){test<-Show_Runsx[,4]}else{
    test<-apply(Show_Runsx[,-c(1:3)], 1, sum)}
    Show_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==4 ])[zz])
Show_Runs20<-data.frame(Show_Runs20,Show_Runsx1)

}




Show_Againstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==4  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
      }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Show_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_Againstx[Show_Againstx$Park!=4,-c(1:3)]<-0
    if(dim(Show_Againstx)[2]==4){test<-Show_Againstx[,4]}else{
    test<-apply(Show_Againstx[,-c(1:3)], 1, sum)}
    Show_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_Against20<-data.frame(Show_Against20,Show_Againstx1)

}

Play_Runsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==4  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
      }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"2",sep="")]

}


Play_Runsx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Runsx[Play_Runsx$Park!=4,-c(1:3)]<-0
    if(dim(Play_Runsx)[2]==4){test<-Play_Runsx[,4]}else{
    test<-apply(Play_Runsx[,-c(1:3)], 1, sum)}
    Play_Runsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz])
Play_Runs20<-data.frame(Play_Runs20,Play_Runsx1)

}

Play_Againstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==4 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
    
    }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    SurveyData22[,noquote(jorja[i])]<- SurveyData22[,paste(jorja[i],"3",sep="")]

}


Play_Againstx<-setNames(aggregate(SurveyData22[,noquote(jorja)], by=list(Park=SurveyData22$park,LifeStage = SurveyData22$newgroup, QTR=SurveyData22$fiscal_quarter), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_Againstx[Play_Againstx$Park!=4,-c(1:3)]<-0
    if(dim(Play_Againstx)[2]==4){test<-Play_Againstx[,4]}else{
    test<-apply(Play_Againstx[,-c(1:3)], 1, sum)}
    Play_Againstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz])
Play_Against20<-data.frame(Play_Against20,Play_Againstx1)

}

}


############################################################################################
# Now that we have the weighted data we need the raw counts for our weighted metric
# CountData = Raw
# SurveyData = Weighted
############################################################################################


CountData22<-SurveyData22

## Reuse ridesexp/entexp/charexp/rideagainx/RIDEX computed above



for(i in 1:length(RIDEX)){
CountData22[which(CountData22[,RIDEX[i]] ==0 & CountData22[,rideagainx[i]] ==1 &CountData22$ovpropex<6),RIDEX[i]]<- -1

    }

CountData22[, new_cols] <- 0


idx_non_na <- which(!is.na(metadata[,2]))
 rideagain_fix<-metadata[,3]
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
        # Suffix "2" (Ride): offset = 0

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
        # Suffix "2" (Ent): offset = 0

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
        # Suffix "2" (Flaship/Anchor): offset = 40

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
        # Suffix "2" (Show): offset = 15

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
          # --- "Can't ride" logic ---
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
          # --- "Can't ride" logic ---
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
          # --- "Can't ride" logic ---
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
          # --- "Can't ride" logic ---
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

if (FALSE) {

jorja<-c()
for(ii in 1:length(metadata[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadata[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
    }
jorja <- jorja[metadata$Type== "Ride"]

Ride_OriginalRuns20<-c()
for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}
Ride_OriginalRuns20<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))


jorja<-c()
for(ii in 1:length(metadata[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadata[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
    }
jorja <- jorja[metadata$Type== "Ride"]

Ride_OriginalAgainst20<-c()
for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}

Ride_OriginalAgainst20<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))


Show_OriginalRunsx<-c()
Show_OriginalRuns20<-Ride_OriginalRuns20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"    &metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
    }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Show_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalRunsx[Show_OriginalRunsx$Park!=1,-c(1:3)]<-0
    if(dim(Show_OriginalRunsx)[2]==4){test<-Show_OriginalRunsx[,4]}else{
    test<-apply(Show_OriginalRunsx[,-c(1:3)], 1, sum)}
    Show_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_OriginalRuns20<-data.frame(Show_OriginalRuns20,Show_OriginalRunsx1)

}




Show_OriginalAgainstx<-c()
Show_OriginalAgainst20<-Ride_OriginalRuns20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
    }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Show_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalAgainstx[Show_OriginalAgainstx$Park!=1,-c(1:3)]<-0
    if(dim(Show_OriginalAgainstx)[2]==4){test<-Show_OriginalAgainstx[,4]}else{
    test<-apply(Show_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Show_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_OriginalAgainst20<-data.frame(Show_OriginalAgainst20,Show_OriginalAgainstx1)

}

Play_OriginalRunsx<-c()
Play_OriginalRuns20<-Ride_OriginalRuns20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==1  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
    }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Play_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalRunsx[Play_OriginalRunsx$Park!=1,-c(1:3)]<-0
    if(dim(Play_OriginalRunsx)[2]==4){test<-Play_OriginalRunsx[,4]}else{
    test<-apply(Play_OriginalRunsx[,-c(1:3)], 1, sum)}
    Play_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==1 ])[zz])
Play_OriginalRuns20<-data.frame(Play_OriginalRuns20,Play_OriginalRunsx1)

}




Play_OriginalAgainstx<-c()
Play_OriginalAgainst20<-Ride_OriginalRuns20[,1:3]
for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==1 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==1 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))

    }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Play_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalAgainstx[Play_OriginalAgainstx$Park!=1,-c(1:3)]<-0
    if(dim(Play_OriginalAgainstx)[2]==4){test<-Play_OriginalAgainstx[,4]}else{
    test<-apply(Play_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Play_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==1 ])[zz])
Play_OriginalAgainst20<-data.frame(Play_OriginalAgainst20,Play_OriginalAgainstx1)

}


Show_OriginalRunsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"    &metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
   }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Show_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalRunsx[Show_OriginalRunsx$Park!=2,-c(1:3)]<-0
    if(dim(Show_OriginalRunsx)[2]==4){test<-Show_OriginalRunsx[,4]}else{
    test<-apply(Show_OriginalRunsx[,-c(1:3)], 1, sum)}
    Show_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2 ])[zz])
Show_OriginalRuns20<-data.frame(Show_OriginalRuns20,Show_OriginalRunsx1)

}




Show_OriginalAgainstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==2  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
  }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Show_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalAgainstx[Show_OriginalAgainstx$Park!=2,-c(1:3)]<-0
    if(dim(Show_OriginalAgainstx)[2]==4){test<-Show_OriginalAgainstx[,4]}else{
    test<-apply(Show_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Show_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_OriginalAgainst20<-data.frame(Show_OriginalAgainst20,Show_OriginalAgainstx1)

}




Play_OriginalRunsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==2  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
   }
jorja<-jorja[jorja!=""]

for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Play_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalRunsx[Play_OriginalRunsx$Park!=2,-c(1:3)]<-0
    if(dim(Play_OriginalRunsx)[2]==4){test<-Play_OriginalRunsx[,4]}else{
    test<-apply(Play_OriginalRunsx[,-c(1:3)], 1, sum)}
    Play_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==2 ])[zz])
Play_OriginalRuns20<-data.frame(Play_OriginalRuns20,Play_OriginalRunsx1)

}




Play_OriginalAgainstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==2 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==2 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
   }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Play_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalAgainstx[Play_OriginalAgainstx$Park!=2,-c(1:3)]<-0
    if(dim(Play_OriginalAgainstx)[2]==4){test<-Play_OriginalAgainstx[,4]}else{
    test<-apply(Play_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Play_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==2 ])[zz])
Play_OriginalAgainst20<-data.frame(Play_OriginalAgainst20,Play_OriginalAgainstx1)

}



Show_OriginalRunsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==3 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"    &metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
   }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Show_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalRunsx[Show_OriginalRunsx$Park!=3,-c(1:3)]<-0
    if(dim(Show_OriginalRunsx)[2]==4){test<-Show_OriginalRunsx[,4]}else{
    test<-apply(Show_OriginalRunsx[,-c(1:3)], 1, sum)}
    Show_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==3 ])[zz])
Show_OriginalRuns20<-data.frame(Show_OriginalRuns20,Show_OriginalRunsx1)

}




Show_OriginalAgainstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==3  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
  }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Show_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalAgainstx[Show_OriginalAgainstx$Park!=3,-c(1:3)]<-0
    if(dim(Show_OriginalAgainstx)[2]==4){test<-Show_OriginalAgainstx[,4]}else{
    test<-apply(Show_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Show_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_OriginalAgainst20<-data.frame(Show_OriginalAgainst20,Show_OriginalAgainstx1)

}




Play_OriginalRunsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==3  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
    }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Play_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalRunsx[Play_OriginalRunsx$Park!=3,-c(1:3)]<-0
    if(dim(Play_OriginalRunsx)[2]==4){test<-Play_OriginalRunsx[,4]}else{
    test<-apply(Play_OriginalRunsx[,-c(1:3)], 1, sum)}
    Play_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz])
Play_OriginalRuns20<-data.frame(Play_OriginalRuns20,Play_OriginalRunsx1)

}




Play_OriginalAgainstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==3 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
   }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Play_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalAgainstx[Play_OriginalAgainstx$Park!=3,-c(1:3)]<-0
    if(dim(Play_OriginalAgainstx)[2]==4){test<-Play_OriginalAgainstx[,4]}else{
    test<-apply(Play_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Play_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==3 ])[zz])
Play_OriginalAgainst20<-data.frame(Play_OriginalAgainst20,Play_OriginalAgainstx1)

}






Show_OriginalRunsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==4 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"    &metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
    }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Show_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalRunsx[Show_OriginalRunsx$Park!=4,-c(1:3)]<-0
    if(dim(Show_OriginalRunsx)[2]==4){test<-Show_OriginalRunsx[,4]}else{
    test<-apply(Show_OriginalRunsx[,-c(1:3)], 1, sum)}
    Show_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==4 ])[zz])
Show_OriginalRuns20<-data.frame(Show_OriginalRuns20,Show_OriginalRunsx1)

}




Show_OriginalAgainstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==4  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[metadata$Type == "Show"  & metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))[1]))
   }
jorja<-jorja[jorja !=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}


Show_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Show_OriginalAgainstx[Show_OriginalAgainstx$Park!=4,-c(1:3)]<-0
    if(dim(Show_OriginalAgainstx)[2]==4){test<-Show_OriginalAgainstx[,4]}else{
    test<-apply(Show_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Show_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[metadata$Type == "Show" & metadata$Park ==1 ])[zz])
Show_OriginalAgainst20<-data.frame(Show_OriginalAgainst20,Show_OriginalAgainstx1)

}




Play_OriginalRunsx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"   &metadata$Park ==4  ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
    }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"2",sep="")]

}


Play_OriginalRunsx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalRunsx[Play_OriginalRunsx$Park!=4,-c(1:3)]<-0
    if(dim(Play_OriginalRunsx)[2]==4){test<-Play_OriginalRunsx[,4]}else{
    test<-apply(Play_OriginalRunsx[,-c(1:3)], 1, sum)}
    Play_OriginalRunsx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz])
Play_OriginalRuns20<-data.frame(Play_OriginalRuns20,Play_OriginalRunsx1)

}




Play_OriginalAgainstx<-c()

for(zz in 1:length(unique(metadata$Category1[metadata$Type== "Play"    &metadata$Park ==4 ]))){
jorja<-c()

metadatax<-metadata[which(metadata$Category1 == unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz]),]


    for(ii in 1:length(metadatax[,2])){
jorja<-c(jorja,sub(".*_","",unlist(strsplit(unlist(strsplit(unlist(strsplit(metadatax[ii,2], split=c( 'ridesexp_'), fixed=TRUE)), split=c( 'entexp_'), fixed=TRUE)), split=c( 'charexp_'), fixed=TRUE))))
   }
jorja<-jorja[jorja!=""]


for(i in 1: length(jorja)){

    CountData22[,noquote(jorja[i])]<- CountData22[,paste(jorja[i],"3",sep="")]

}

Play_OriginalAgainstx<-setNames(aggregate(CountData22[,noquote(jorja)], by=list(Park=CountData22$park,LifeStage = CountData22$newgroup, QTR=CountData22$fiscal_quarter ), FUN=sum), c("Park","LifeStage", "QTR",noquote(jorja)))
Play_OriginalAgainstx[Play_OriginalAgainstx$Park!=4,-c(1:3)]<-0
    if(dim(Play_OriginalAgainstx)[2]==4){test<-Play_OriginalAgainstx[,4]}else{
    test<-apply(Play_OriginalAgainstx[,-c(1:3)], 1, sum)}
    Play_OriginalAgainstx1<-setNames(data.frame(test),unique(metadata$Category1[ metadata$Type== "Play"   &metadata$Park ==4 ])[zz])
Play_OriginalAgainst20<-data.frame(Play_OriginalAgainst20,Play_OriginalAgainstx1)

}

}
############################################################################################
# END OF WEIGHTING NOW WE HAVE TO COMBINE EVERYTHING TO MAKE THE STATISTIC
# From CountData we have "Original_RS","Original_RA" (i could have named these better but RS is runs scored RA is runs against)
# From SurveyData "wEE","wEEx"  These are the weighted EEs or wEE is runs scored and wEEx is runs against
############################################################################################

library(reshape)
Show_OriginalRuns<-rbind(Show_OriginalRuns20)
Show_OriginalAgainst<-rbind(Show_OriginalAgainst20)
Show_Runs<-rbind(Show_Runs20)
Show_Against<-rbind(Show_Against20)

one<-reshape2::melt(Show_OriginalRuns, id=(c("Park", "LifeStage","QTR")))
two<-reshape2::melt(Show_OriginalAgainst, id=(c("Park", "LifeStage","QTR")))
three<-reshape2::melt(Show_Runs, id=(c("Park", "LifeStage","QTR")))
four<-reshape2::melt(Show_Against, id=(c("Park", "LifeStage","QTR")))


Show<- cbind(one,two[,-c(1:4)],three[,-c(1:4)],four[,-c(1:4)])
Show<-Show[!is.na(Show$Park),]
Show<-unique(Show[apply(Show!=0, 1, all),])
names(Show)<- c("Park", "LifeStage","QTR","NAME","Original_RS","Original_RA","wEE","wEEx")

Play_OriginalRuns<-rbind(Play_OriginalRuns20)
Play_OriginalAgainst<-rbind(Play_OriginalAgainst20)
Play_Runs<-rbind(Play_Runs20)
Play_Against<-rbind(Play_Against20)


one<-reshape2::melt(Play_OriginalRuns, id=(c("Park", "LifeStage", "QTR")))
two<-reshape2::melt(Play_OriginalAgainst, id=(c("Park", "LifeStage", "QTR")))
three<-reshape2::melt(Play_Runs, id=(c("Park", "LifeStage", "QTR")))
four<-reshape2::melt(Play_Against, id=(c("Park", "LifeStage", "QTR")))


Play<- cbind(one,two[,-c(1:4)],three[,-c(1:4)],four[,-c(1:4)])
Play<-unique(Play[apply(Play!=0, 1, all),])
names(Play)<- c("Park", "LifeStage","QTR","NAME","Original_RS","Original_RA","wEE","wEEx")
Play<-Play[Play$NAME != "Park.1",]
Play<-Play[Play$NAME != "LifeStage.1",]
Play<-Play[Play$NAME != "QTR.1",]

Ride_OriginalRuns<-rbind(Ride_OriginalRuns20)
Ride_OriginalAgainst<-rbind(Ride_OriginalAgainst20)
Ride_Runs<-rbind(Ride_Runs20)
Ride_Against<-rbind(Ride_Against20)

one<-reshape2::melt(Ride_OriginalRuns, id=(c("Park", "LifeStage", "QTR")))
two<-reshape2::melt(Ride_OriginalAgainst, id=(c("Park", "LifeStage", "QTR")))
three<-reshape2::melt(Ride_Runs, id=(c("Park", "LifeStage", "QTR")))
four<-reshape2::melt(Ride_Against, id=(c("Park", "LifeStage", "QTR")))


Ride<- cbind(one,two[,-c(1:4)],three[,-c(1:4)],four[,-c(1:4)])
Ride<-unique(Ride[apply(Ride!=0, 1, all),])
names(Ride)<- c("Park", "LifeStage","QTR","NAME","Original_RS","Original_RA","wEE","wEEx")# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

wRAA_Table<-data.frame(Ride, Genre = "Ride")

Show$Genre <- "Show"
Play$Genre <- "Play"
wRAA_Table<-rbind(wRAA_Table,Show, Play)

library(dplyr)
library(sqldf)
twox<-wRAA_Table %>% group_by(NAME, Park, Genre, QTR, LifeStage) %>% mutate(Original_RS=sum(Original_RS), Original_RA=sum(Original_RA),wEE=sum(wEE), wEEx = sum(wEEx)) %>% distinct(.keep_all = FALSE)

onex<-twox %>% group_by(NAME,Park, QTR) %>% mutate(sum = sum(Original_RS+Original_RA) )
onex$Percent<- (onex$Original_RS+onex$Original_RA)/onex$sum
wRAA_Table<- sqldf('select a.*,b.Percent from twox a left join onex b on a.Park = b.Park and a.QTR=b.QTR and a.LifeStage = b.LifeStage and a.Name = b.Name')


wRAA_Table<-wRAA_Table[!is.na(wRAA_Table$Park), ]

#wRAA_Table$wEEx[wRAA_Table$wEEx>2000]<-wRAA_Table$Original_RA[wRAA_Table$wEEx>2000] I could probably change this to all parks scale maybe
wRAA_Table<-cbind(wRAA_Table, wOBA = wRAA_Table$wEE/ (wRAA_Table$Original_RS+ wRAA_Table$Original_RA) )

wRAA_Table<-merge(x=wRAA_Table, y =aggregate( wRAA_Table$Original_RS, by=list( QTR=wRAA_Table$QTR,Park = wRAA_Table$Park), FUN=sum), by = c("QTR", "Park"), all.x = TRUE)
names(wRAA_Table)[length(names(wRAA_Table))]<-"OBP1"
wRAA_Table<-merge(x=wRAA_Table, y =aggregate( wRAA_Table$Original_RA, by=list( QTR=wRAA_Table$QTR,Park = wRAA_Table$Park), FUN=sum), by = c("QTR", "Park"), all.x = TRUE)
names(wRAA_Table)[length(names(wRAA_Table))]<-"OBP2"
#Calculating an "On Base Average"  More Guest's on base means higher chance of scoring
    wRAA_Table$OBP<-wRAA_Table$OBP1/(wRAA_Table$OBP1+wRAA_Table$OBP2)

wRAA_Table<-merge(x = wRAA_Table, y = aggregate( wRAA_Table$wOBA, by=list( QTR=wRAA_Table$QTR,Park = wRAA_Table$Park), FUN=median), by = c("QTR", "Park"), all.x = TRUE)
names(wRAA_Table)[length(names(wRAA_Table))]<-"wOBA_Park"
#Scaling it to the park
    wRAA_Table<-data.frame(wRAA_Table,wOBA_Scale = wRAA_Table$wOBA_Park/wRAA_Table$OBP)

join_keys <- c("NAME", "Park", "Genre", "QTR", "LifeStage")
# Remove duplicate columns from table2 except join keys
table2_nodup <- EARS[, setdiff(names(EARS), setdiff(names(wRAA_Table), join_keys))]

# Merge, keeping only columns from table1 and non-duplicate columns from table2
EARSFinal_Final <- merge(wRAA_Table, table2_nodup, by = join_keys, all = FALSE)
#####################################################################################################
#Now that testing/parametrization is complete we will add in the guest carried and recalculate ears for the quarter
#This requires POG*Attendance for experiences without a Guest Carried.
#Guest Carried are collected from IE

#####################################################################################################
EARSx<-EARSFinal_Final
        EARSx$wRAA<-((EARSx$wOBA - EARSx$wOBA_Park)/EARSx$wOBA_Scale)*(EARSx$wRAA_Table.AnnualGuestsCarried)
EARSx$EARS<- EARSx$wRAA/ EARSx$RPW

EARSx$EARS<-    (EARSx$EARS+EARSx$replacement)*EARSx$p
    if (verbose) print(unique(EARSx$QTR))
EARSTotal<-rbind(EARSTotal,EARSx)
    
#EARSTotal<-sqldf('select a.*, b.EARS as Actual from EARSTotal a left join EARS b on a.NAME = b.NAME and
#a.Park = b.Park and  a.QTR = b.QTR and a.LifeStage=b.LifeStage')

FQ<-FQ+1
   }

EARS$Actual_EARS<-EARS$EARS


# Full join, then keep only id and var1
result <- full_join(EARSTotal, EARS[,c("NAME","Park","Genre","QTR","LifeStage", "Actual_EARS")], by = c("NAME","Park","Genre","QTR","LifeStage")) 
  #select(c("NAME","Park","QTR","LifeStage"), EARS,Actual_EARS)

result$EARS[is.na(result$EARS)]<-0
    colnames(result)[colnames(result) == "EARS"] <- "Simulation_EARS"

result$Incremental_EARS<-result$Simulation_EARS-result$Actual_EARS 
    EARSTotal2<-result
   EARSTotal2$sim_run <- run
  EARSTotal2
}
stopCluster(cl)
Simulation_Results<-EARSTotal_list
