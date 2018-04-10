library(data.table)
library(bit64)
library(sqldf)
library(dplyr)
library(timeDate)
library(ranger)
library(zoo)
library(TTR)
library(xts)
library(MASS)
library(pROC)
library(caret)
library(PresenceAbsence)
setwd("/data")
meta=fread("/data/meta/MetaStore2708_1.txt")
WeatherData=fread('Albertsons_Weather.csv')
ABS_UPC_Metadata=fread("ALLUPC.txt")
GoogleTrends=fread("GoogleTrends/GoogleTrends.csv")
news_sentiment = fread("/data/News_Sentiment.csv")
# Date Formatting
meta$TxnDateFixed=as.Date(meta$TxnDateFixed,format = '%Y-%m-%d')
WeatherData$DateTime=as.Date(as.character(WeatherData$DateTime),format = '%Y-%m-%d')
GoogleTrends$DateTime=as.Date(as.character(GoogleTrends$DateTime),format = '%Y-%m-%d')
news_sentiment$DateTime=as.Date(as.character(news_sentiment$DateTime),format = '%Y-%m-%d')

#weightSub=meta[meta$upc_group=="WEIGHT",]
itemSub=meta[meta$upc_group=="UNITS" | meta$upc_group=="WEIGHT",]
itemSub=itemSub[itemSub$V1<=5,]
rm(meta)
#> summary(weightSub$V1)
#Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
#1.00    1.00    3.00    9.61    8.00 2059.00 
#> summary(itemSub$V1)
#Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
#1.000    1.000    2.000    3.257    3.000 2361.000 
dates=seq.Date(as.Date("2013-01-01"), as.Date("2018-03-12"), by="days")
itemSubLV=itemSub %>% group_by(UPC_ID) %>% summarise(percentSales=n()/length(dates))
itemSubLV=itemSubLV[itemSubLV$percentSales<0.3,]
SalesByUPC = itemSub[itemSub$UPC_ID %in% itemSubLV$UPC_ID,]
SalesByUPC$V1[SalesByUPC$V1>0]=1
rm(itemSub)

#upc_id formatting
GoogleTrends$upc_id=as.integer64(GoogleTrends$upc_id)
#Impute NA, infinities and negatives for Coupon discount and online offline
SalesByUPC$CouponDisc[SalesByUPC$CouponDisc<0] <- 0
SalesByUPC$CouponDisc[is.infinite(SalesByUPC$CouponDisc)] <- 0
SalesByUPC$CouponDisc[is.na(SalesByUPC$CouponDisc)] <- 0
SalesByUPC$OnlineOffline[is.infinite(SalesByUPC$OnlineOffline)] <- 1
SalesByUPC$OnlineOffline[is.na(SalesByUPC$OnlineOffline)] <- 0


#12048 unique upc in the data
#get the groups to work on
SalesByUPC = merge(SalesByUPC,ABS_UPC_Metadata[,c("upc_id","category_nm","class_nm")],by.x="UPC_ID",by.y="upc_id")
CategoryList = unique(SalesByUPC$category_nm)
#itemSubDates = get_all_dates(itemSubLV)
AllFeatures = c("PrevDaySales1","PrevDaySales2","PrevDaySales3","PrevDaySales4",
                "CouponDisc","OnlineOffline","PromotionInd",
                "WeekDay","FedHoliday","PrevDayFedHoliday1","PrevDayFedHoliday2","NextDayFedHoliday1","NextDayFedHoliday2",
                "temp_avg_DFN","pressure_avg_DFN","wspd_avg_DFN","clds_avg_DFN","precip_DFN",
                "temp_min","temp_max","temp_avg","pressure_avg","wspd_avg","clds_avg","precip","hits","Score")

#CategoryList=CategoryList[1:2]
FeatureVector=c("V1","PrevDaySales1","PrevDaySales2","PrevDaySales3","PrevDaySales4",
                "PrevDaySales5","PrevDaySales6","PrevDaySales7","PrevDaySales8","CouponDisc","OnlineOffline","PromotionInd",
                "PriceLast7","PriceLast3")
ResultSummary = category_model_loop(SalesByUPC,CategoryList,FeatureVector,indexFlag=0)
fwrite(ResultSummary,"LowVolume/Categories_Sales2_0408.csv",row.names = F)

FeatureVector=c("V1","PrevDaySales1","PrevDaySales2","PrevDaySales3","PrevDaySales4",
                "PrevDaySales5","PrevDaySales6","PrevDaySales7","PrevDaySales8","CouponDisc","OnlineOffline","PromotionInd",
                "PriceLast7","PriceLast3",
                "WeekDay","DoM","Month","DoY","FedHoliday","PrevDayFedHoliday1","PrevDayFedHoliday2","NextDayFedHoliday1","NextDayFedHoliday2"
)
ResultSummary = category_model_loop(SalesByUPC,CategoryList,FeatureVector,indexFlag=0)
fwrite(ResultSummary,"LowVolume/Categories_SalesHolidays_0408.csv",row.names = F)

FeatureVector=c("V1","PrevDaySales1","PrevDaySales2","PrevDaySales3","PrevDaySales4","CouponDisc","OnlineOffline","PromotionInd",
                "WeekDay","FedHoliday","PrevDayFedHoliday1","PrevDayFedHoliday2","NextDayFedHoliday1","NextDayFedHoliday2",
                "temp_avg_DFN","pressure_avg_DFN","wspd_avg_DFN","clds_avg_DFN","precip_DFN",
                "temp_min","temp_max","temp_avg","pressure_avg","wspd_avg","clds_avg","precip")
ResultSummary = category_model_loop(SalesByUPC,CategoryList,FeatureVector,indexFlag=0)
fwrite(ResultSummary,"LowVolume/Categories_SalesWeather_0408.csv",row.names = F)

FeatureVector=c("V1","PrevDaySales1","PrevDaySales2","PrevDaySales3","PrevDaySales4",
                "CouponDisc","OnlineOffline","PromotionInd",
                "WeekDay","FedHoliday","PrevDayFedHoliday1","PrevDayFedHoliday2","NextDayFedHoliday1","NextDayFedHoliday2",
                "temp_avg_DFN","pressure_avg_DFN","wspd_avg_DFN","clds_avg_DFN","precip_DFN",
                "temp_min","temp_max","temp_avg","pressure_avg","wspd_avg","clds_avg","precip","hits","Score")
ResultSummary = category_model_loop(SalesByUPC,CategoryList,FeatureVector,indexFlag=0)
fwrite(ResultSummary,"LowVolume/Categories_SalesGT_0408.csv",row.names = F)



