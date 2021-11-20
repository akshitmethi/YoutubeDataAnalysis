import numpy as np
import pandas as pd
import datetime
import os
from api import main as m
import xlsxwriter

def parsedDuration(duration):
    hour= "0"
    minute = "0"
    sec = "0"
    i=0
    while i < len(duration):
        if(duration[i].isnumeric()):
            break
        i+=1
    temp=""
    while i< len(duration):
        if(duration[i].isnumeric()):
            temp += duration[i]
        if(duration[i] == "H"):
            hour = temp
            temp = ""
        if(duration[i] == "M"):
            minute = temp
            temp = ""
        if(duration[i] == "S"):
            sec = temp
            temp = ""
        i+=1
    
    return 60*int(minute)+3600*int(hour)+int(sec)




def main():
    currDate = datetime.datetime.now().strftime("%d-%m-%y")
    #if records are processed for a day then don't process it again else fetch from the api
    if(not os.path.exists("../data/"+currDate+"/file.parquet")):
        m.fetchAndWrite("python",400)
    df = pd.read_parquet("../data/{}/file.parquet".format(currDate))
    tagClasses = set(['tutorial','basics','beginners','programming','development','improve','offer','opportunity','machine','learn',"course","pandas","numpy","developer","programmer","android","web"])
    df['classifiedTag'] = df.tags.apply(lambda x: [val for val in tagClasses if(x.lower().find(val) != -1)])
    #do free keyword analysis
    df['isFree'] = df.tags.str.lower().str.contains("free")
    df['durationSec'] = df.duration.apply(parsedDuration)

    #converting it into int format
    forDtypeConversion = ['views','dislikes', 'likes', 'favoriteCount', 'commentCount','durationSec']
    for col in forDtypeConversion:
        df[col] = df[col].astype(int)
    #For per tag analysis, exploding the tag list into rows
    df1 = df.explode(['classifiedTag'])
    #tag analysis
    tagGrouped = df1.groupby(['classifiedTag']).agg(videoCount = pd.NamedAgg('id',"count")
                                 ,avgDurationSec = pd.NamedAgg('durationSec','mean')
                                 ,maxDurationSec = pd.NamedAgg('durationSec','max')
                                 ,minDurationSec = pd.NamedAgg('durationSec','min')
                                 ,avgNumComments = pd.NamedAgg('commentCount','mean')
                                 ,avgLikes = pd.NamedAgg('likes','mean')
                                 ,avgDislikes = pd.NamedAgg('dislikes','mean')
                                 ,avgView = pd.NamedAgg('views','mean'))

    languageGrouped = df1.groupby(['language']).agg(avgViews = pd.NamedAgg('views','mean')
                             ,avgCommentCount = pd.NamedAgg('commentCount','mean')
                             ,avglikes = pd.NamedAgg('likes','mean')
                             ,maxViews = pd.NamedAgg('views','max')
                             ,maxCommentCount = pd.NamedAgg('commentCount','max')
                             ,maxlikes = pd.NamedAgg('likes','max'))

    #checking if adding free keyword in tags or title can affect the view rate
    isFreeGrouped = df1.groupby(['isFree']).agg(avgViews = pd.NamedAgg('views','mean')
                             ,avgCommentCount = pd.NamedAgg('commentCount','mean')
                             ,avglikes = pd.NamedAgg('likes','mean')
                             ,maxViews = pd.NamedAgg('views','max')
                             ,maxCommentCount = pd.NamedAgg('commentCount','max')
                             ,maxlikes = pd.NamedAgg('likes','max')
                             ,minViews = pd.NamedAgg('views','min')
                             ,minCommentCount = pd.NamedAgg('commentCount','min')
                             ,minlikes = pd.NamedAgg('likes','min'))

    #write to excel
    isFreeGrouped.head()
    excelWriter = pd.ExcelWriter("../data/{}/analysis_{}.xlsx".format(currDate,currDate),mode="w")
    tagGrouped.to_excel(excelWriter,engine=xlsxwriter,sheet_name="tagAnalysis")
    languageGrouped.to_excel(excelWriter,engine=xlsxwriter,sheet_name="languageAnalysis")
    isFreeGrouped.to_excel(excelWriter,engine=xlsxwriter,sheet_name="FreeTagAnalysis")


    excelWriter.close()


main()
