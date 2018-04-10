##### Loop for categorical models (using group name instead of category name)
category_model_loop=function(SalesByUPC,CategoryList,FeatureVector,indexFlag){
  df1=c()
  i=0
  for (j in CategoryList) {
    i=i+1
    categoryUPC = ABS_UPC_Metadata[ABS_UPC_Metadata$category_nm==j,"upc_id"]
    SalesByUPCSubSet=SalesByUPC[SalesByUPC$UPC_ID%in%categoryUPC$upc_id,]
    #Exclude insignificant categories
    if (nrow(SalesByUPCSubSet)==0 || sum(SalesByUPCSubSet$V1)<1000) { next }
    #if(indexFlag==1){SalesByUPCSubSet=Sales_to_Indices(SalesByUPCSubSet,numberofdays=7) } else {SalesByUPCSubSet$SalesSMA=1}
    SalesByUPCSubSet$SalesSMA=1
    SalesByUPCSubSet=get_all_dates(SalesByUPCSubSet)
    PredictMat=create_predict_mat(SalesByUPCSubSet)
    #print(nrow(PredictMat))
    df2=model_predictions_cla(PredictMat,FeatureVector,j)
    df1 = rbind(df1,df2)
    print(paste(i,": ",j))
  }
  return(df1)
}
#Create Matrix for modeling - add features
create_predict_mat = function(SalesByUPCSubSet){
  
  ###Lags for pooled linear fit
  SalesByUPCSubSet=as.data.frame(SalesByUPCSubSet)
  SalesByUPCSubSet$PrevDate1=SalesByUPCSubSet$TxnDateFixed-1
  SalesByUPCSubSet$PrevDate2=SalesByUPCSubSet$TxnDateFixed-2
  SalesByUPCSubSet$PrevDate3=SalesByUPCSubSet$TxnDateFixed-3
  SalesByUPCSubSet$PrevDate4=SalesByUPCSubSet$TxnDateFixed-4
  SalesByUPCSubSet$PrevDate5=SalesByUPCSubSet$TxnDateFixed-5
  SalesByUPCSubSet$PrevDate6=SalesByUPCSubSet$TxnDateFixed-6
  SalesByUPCSubSet$PrevDate7=SalesByUPCSubSet$TxnDateFixed-7
  SalesByUPCSubSet$PrevDate8=SalesByUPCSubSet$TxnDateFixed-8
  SalesByUPCSubSet$NextDate1=SalesByUPCSubSet$TxnDateFixed+1
  SalesByUPCSubSet$NextDate2=SalesByUPCSubSet$TxnDateFixed+2
  
  ###Add weekday
  SalesByUPCSubSet$WeekDay = weekdays(SalesByUPCSubSet$TxnDateFixed)
  SalesByUPCSubSet$WeekDay = as.numeric(factor(SalesByUPCSubSet$WeekDay))
  SalesByUPCSubSet$DoM = format(SalesByUPCSubSet$TxnDateFixed, "%d") 
  SalesByUPCSubSet$Month = format(SalesByUPCSubSet$TxnDateFixed, "%m")
  SalesByUPCSubSet$DoY = round(as.numeric(strftime(SalesByUPCSubSet$TxnDateFixed, format = "%j"))/10)
  ###add general holidays, gen holidays - 1
  SalesByUPCSubSet$FedHoliday = (SalesByUPCSubSet$TxnDateFixed %in% as.Date(holidayNYSE(2013:2018),format="%Y-%m-%d"))
  SalesByUPCSubSet$PrevDayFedHoliday1 = (SalesByUPCSubSet$PrevDate1 %in% as.Date(holidayNYSE(2013:2018),format="%Y-%m-%d"))
  SalesByUPCSubSet$PrevDayFedHoliday2 = (SalesByUPCSubSet$PrevDate2 %in% as.Date(holidayNYSE(2013:2018),format="%Y-%m-%d"))
  SalesByUPCSubSet$NextDayFedHoliday1 = (SalesByUPCSubSet$NextDate1 %in% as.Date(holidayNYSE(2013:2018),format="%Y-%m-%d"))
  SalesByUPCSubSet$NextDayFedHoliday2 = (SalesByUPCSubSet$NextDate2 %in% as.Date(holidayNYSE(2013:2018),format="%Y-%m-%d"))
  ###Add weather data
  weatherPredictors = c("DateTime","temp_avg_DFN","temp_min_DFN","temp_max_DFN","pressure_avg_DFN","wspd_avg_DFN",
                        "clds_avg_DFN","precip_DFN","temp_avg","temp_min","temp_max",
                        "pressure_avg","wspd_avg","clds_avg","precip")
  SalesByUPCSubSet=left_join(SalesByUPCSubSet,WeatherData[,..weatherPredictors],by=c("TxnDateFixed"="DateTime"))
  SalesByUPCSubSet=left_join(SalesByUPCSubSet,GoogleTrends[,c("DateTime","upc_id","hits")],by=c("TxnDateFixed"="DateTime","UPC_ID"="upc_id"))
  SalesByUPCSubSet=left_join(SalesByUPCSubSet,news_sentiment[,c("DateTime","upc_id","Score")],by=c("TxnDateFixed"="DateTime","UPC_ID"="upc_id"))
  SalesByUPCSubSet=SalesByUPCSubSet[with(SalesByUPCSubSet,order(TxnDateFixed)),]
  ##Replace weather NA's by previous day
  for (i in weatherPredictors[-1]) {
    SalesByUPCSubSet[i] = na.locf(SalesByUPCSubSet[i])
  }
  SalesByUPCSubSet$UPC_ID=as.numeric(SalesByUPCSubSet$UPC_ID)
  PredictMat=sqldf('select a.*,b.V1 as PrevDaySales1,c.V1 as PrevDaySales2,d.V1 as PrevDaySales3,e.V1 as PrevDaySales4,
                   f.V1 as PrevDaySales5, g.V1 as PrevDaySales6, h.V1 as PrevDaySales7, i.V1 as PrevDaySales8, 
                   b.AvgPrice as AvgPrice1,c.AvgPrice as AvgPrice2,d.AvgPrice as AvgPrice3,e.AvgPrice as AvgPrice4,
                   f.AvgPrice as AvgPrice5, g.AvgPrice as AvgPrice6, h.AvgPrice as AvgPrice7, i.AvgPrice as AvgPrice8 from SalesByUPCSubSet a left join SalesByUPCSubSet b on b.TxnDateFixed=a.PrevDate1 and a.UPC_ID=b.UPC_ID   left join SalesByUPCSubSet c on c.TxnDateFixed=a.PrevDate2 and a.UPC_ID=c.UPC_ID left join SalesByUPCSubSet d on d.TxnDateFixed=a.PrevDate3 and a.UPC_ID=d.UPC_ID 
                   left join SalesByUPCSubSet e on e.TxnDateFixed=a.PrevDate4 and a.UPC_ID=e.UPC_ID
                   left join SalesByUPCSubSet f on f.TxnDateFixed=a.PrevDate5 and a.UPC_ID=f.UPC_ID
                   left join SalesByUPCSubSet g on g.TxnDateFixed=a.PrevDate6 and a.UPC_ID=g.UPC_ID
                   left join SalesByUPCSubSet h on h.TxnDateFixed=a.PrevDate7 and a.UPC_ID=h.UPC_ID
                   left join SalesByUPCSubSet i on i.TxnDateFixed=a.PrevDate8 and a.UPC_ID=i.UPC_ID' )
  PredictMat$PriceLast7=PredictMat$AvgPrice/rowMeans(subset(PredictMat,select=c(AvgPrice1,AvgPrice2,AvgPrice3,AvgPrice4,AvgPrice5,AvgPrice6,AvgPrice7),na.rm = T))
  PredictMat$PriceLast3=PredictMat$AvgPrice/rowMeans(subset(PredictMat,select=c(AvgPrice1,AvgPrice2,AvgPrice3),na.rm = T))
  PredictMat[is.na(PredictMat)]=0
  #PredictMat=data.frame(lapply(PredictMat, function(x) as.numeric(x)))
  PredictMat$V1=factor(PredictMat$V1)
  #PredictMat$meanLast8 = mean(c(PredictMat$PrevDaySales2,PredictMat$PrevDaySales1,PredictMat$PrevDaySales3,PredictMat$PrevDaySales4,PredictMat$PrevDaySales5,PredictMat$PrevDaySales6,PredictMat$PrevDaySales7,PredictMat$PrevDaySales8))
  #PredictMat$medianLast8 = median(c(PredictMat$PrevDaySales2,PredictMat$PrevDaySales1,PredictMat$PrevDaySales3,PredictMat$PrevDaySales4,PredictMat$PrevDaySales5,PredictMat$PrevDaySales6,PredictMat$PrevDaySales7,PredictMat$PrevDaySales8))
  
  ##Replace prev day sales NA with zeroes
  
  #print(nrow(PredictMat))
  return(PredictMat)
}

model_predictions_cla = function(PredictMat,FeatureVector,j){
  #Try catch to exclude the categories that throw unexpected errors - sparse test data set
  tryCatch({
    ###Linear Regression Train
    Traindata=PredictMat[PredictMat$TxnDateFixed<='2016-12-31',FeatureVector]
    #Traindata = SMOTE(V1~.,Traindata)
    # TrainFit=stepAIC(glm(V1~.,data=Traindata,family=binomial(link='logit')),direction = "both")
    #print(nrow(PredictMat))
    ###Linear Regression Test
    TestAll=PredictMat[PredictMat$TxnDateFixed>'2016-12-31',]
    TestData=PredictMat[PredictMat$TxnDateFixed>'2016-12-31',FeatureVector]
    #print(nrow(TestData))
    #TestPreds=predict(TrainFit,newdata = TestData,type="response")
    #aucLM <- roc(as.factor(TestAll$V1),TestPreds)
    #print(length(TestPreds))
    ###Ranger Train
    rfTrain=ranger(V1~.,data=Traindata,num.trees = 100,mtry = 4,probability = TRUE,classification = TRUE)
    #print(rfTrain$mtry)
    ###Ranger Test
    TestPredsRanger = predict(rfTrain,TestData,num.trees = rfTrain$num.trees)
    aucR <- roc(as.factor(TestAll$V1),TestPredsRanger$predictions[,2])
    df=data.frame(a=rownames(TestAll),b=as.numeric(as.character(TestAll$V1)),c=TestPredsRanger$predictions[,2])
    threshold=optimal.thresholds(df,opt.methods = c("Default","Sens=Spec","MaxKappa","MinROCdist"),FPC = 0.25,FNC = 0.75)
    #Predictions
    predictions = data.frame(Category=rep(j,nrow(TestAll)),
                             TxnDateFixed=TestAll$TxnDateFixed,
                             UPC_ID=TestAll$UPC_ID,
                             Actuals=TestData$V1,
                             PredictionsRangerThreshold=ifelse(TestPredsRanger$predictions[,2]>=coords(aucR, "best")["threshold"],1,0),
                             PredictionsRangerMaxKappa=ifelse(TestPredsRanger$predictions[,2]>=threshold[threshold$Method=="MaxKappa","c"],1,0),
                             PredictionsRangerDefault=ifelse(TestPredsRanger$predictions[,2]>0.5,1,0),
                             #sma=TestAll$SalesSMA,
                             PreviousDaySales=TestAll$PrevDaySales1)
    predictions=cbind(predictions,TestData[,-1])
    return(predictions)
  }, error=function(e){cat("ERROR :",j," : ",conditionMessage(e), "\n")})
  
}

####
#### Modeling Summary
model_predictions_reg = function(PredictMat,FeatureVector,j){
  #Try catch to exclude the categories that throw unexpected errors - sparse test data set
  tryCatch({
    ###Linear Regression Train
    Traindata=PredictMat[PredictMat$TxnDateFixed<'2016-12-31',FeatureVector]
    TrainFit=stepAIC(lm(V1~.,data=Traindata),direction = "both")
    #print(nrow(PredictMat))
    ###Linear Regression Test
    TestAll=PredictMat[PredictMat$TxnDateFixed>'2016-12-31',]
    TestData=PredictMat[PredictMat$TxnDateFixed>'2016-12-31',FeatureVector]
    #print(nrow(TestData))
    TestPreds=predict(TrainFit,newdata = TestData)
    
    #print(length(TestPreds))
    ###Ranger Train
    rfTrain=ranger(V1~.,data=Traindata,num.trees = 100,mtry = 4,min.node.size = 5)
    #print(rfTrain$mtry)
    ###Ranger Test
    TestPredsRanger = predict(rfTrain,TestData,num.trees = rfTrain$num.trees)
    #Predictions
    predictions = data.frame(Category=rep(j,nrow(TestAll)),
                             TxnDateFixed=TestAll$TxnDateFixed,
                             UPC_ID=TestAll$UPC_ID,
                             actuals=TestData$V1,
                             predictionsLM=TestPreds,
                             predictionsRanger=TestPredsRanger$predictions,
                             sma=TestAll$SalesSMA,
                             previousDaySales=TestAll$PrevDaySales1)
    return(predictions)
  }, error=function(e){cat("ERROR :",j," : ",conditionMessage(e), "\n")})
  
}

get_all_dates = function(meta){
  meta$TxnDateFixed=as.Date(meta$TxnDateFixed,format = '%Y-%m-%d')
  upcList = unique(meta$UPC_ID)
  alldates1=seq.Date(as.Date("2013-01-01"), as.Date("2018-03-12"), by="days")
  alldates=rep(alldates1,each=length(upcList))
  upcs=rep(upcList,times=length(alldates1))
  a=cbind.data.frame(UPC_ID=upcs,TxnDateFixed=alldates)
  rm(alldates,upcs,alldates1,upcList)
  meta1=merge(a,meta,by=c("UPC_ID","TxnDateFixed"),all=TRUE)
  meta1[is.na(meta1$V1),"V1"]=0
  return (meta1)
}
#fwrite(meta1,file="/data/meta/MetaStore_AllDates_2708.txt",row.names = F)
#summary = meta %>% group_by(PLU_CD) %>% summarise(sum=sum(V1))

