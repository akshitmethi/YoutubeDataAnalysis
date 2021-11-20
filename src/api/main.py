import requests
import json
import pandas as pd
import datetime
import os,errno
import yaml

vals = {}
with open("config.yml", "r") as stream:
    try:
        vals = yaml.safe_load(stream)
    except:
        raise "unable to load config file"

apiUrl = vals['google']['api']['video']
part = ["snippet","statistics","contentDetails"]
authKey = vals['google']['api']['authKey']
apiSearchUrl = vals['google']['api']['list']
samplesize = vals['sampleSize']

def fetchVideoDetails(id):
    iter=1
    if(len(id)>50):
        iter = len(id)/50 +1
    resultSet = []
    i=0
    while(i<iter):
        url = "{}?part={}&id={}&key={}".format(apiUrl,"&part=".join(part),"&id=".join(id[i*50:(i+1)*50]),authKey)
        response = requests.request("GET", url, headers={}, data={})
        resultSet.append(json.loads(response.text))
        i+=1
    return resultSet


def parseVideoDetails(jsonResponseList):
    rows = []
    rows.append(["id","publishedTimestamp","title","tags","language","isLiveBroadcast","channelId",
    "channelTitle","categoryId","views","dislikes","likes","favoriteCount","commentCount","description","duration","iscaptioned"])
    for jsonResponse in jsonResponseList:
        for item in jsonResponse['items']:
            id = item['id']
            publishedTimestamp = item['snippet']['publishedAt']
            channelId = item['snippet']['channelId']
            title = item['snippet']['title']
            tags = ''
            if('tags' in item['snippet']):
                tags = "||".join(item['snippet']['tags']) #double pipe seperated items
            language = item['snippet'].get('defaultAudioLanguage') or None
            isLiveBroadcast = item['snippet']['liveBroadcastContent']
            channelTitle =    item['snippet']['channelTitle']
            categoryId =  item['snippet']['categoryId']
            views = item['statistics']['viewCount']
            likes = item['statistics']['likeCount']
            dislikes = item['statistics']['dislikeCount']
            favoriteCount = item['statistics']['favoriteCount']
            commentCount = item['statistics']['commentCount']
            description = item['snippet']['description']
            duration = item['contentDetails']['duration']
            caption = item['contentDetails']['caption']

            rows.append([id,publishedTimestamp,title,tags,language,isLiveBroadcast,channelId,
            channelTitle,categoryId,views,dislikes,likes,favoriteCount,commentCount,description,duration,caption])
        
    return rows


#TODO
def descriptionAnalysis(desc):
    # this will parse desc text to find and classify the video type
    return {}


#maxResultSize above 50 will trucate to 50, as google only allows 50 items at max
def fetchVideoList(key,maxResultSize=50):
    resultSet = []
    nextPageToken=None
    iter = 1
    if(maxResultSize>50):
        iter = maxResultSize/50 + 1
    while(iter>0):
        url = ""
        if(nextPageToken == None):
            url = apiSearchUrl+"?part=snippet&maxResults={}&q={}&type=video&key={}".format(maxResultSize,key,authKey)
        else:
            url = apiSearchUrl+"?part=snippet&maxResults={}&q={}&type=video&nextPageToken={}&key={}".format(maxResultSize,key,nextPageToken,authKey)
        response = requests.request("GET", url, headers={}, data={})
        jsonResponse = json.loads(response.text)
        nextPageToken = jsonResponse['nextPageToken']
        resultSet.append(jsonResponse)
        iter -= 1
    return resultSet


def parseAndFetchVideoDescription(key,maxResultSize):
    videoListJson = fetchVideoList(key,maxResultSize)
    ids = [item['id']['videoId'] for videoJson in videoListJson for item in videoJson['items']]
    rows = parseVideoDetails(fetchVideoDetails(ids))
    return rows

def fetchAndWrite(key,maxResultSize=samplesize):
    rows = parseAndFetchVideoDescription(key,maxResultSize)
    df = pd.DataFrame(rows[1:],columns=rows[0])
    currDate = datetime.datetime.now().strftime("%d-%m-%y")
    if(not os.path.exists("../data/"+currDate)):
        try:
            os.makedirs("../data/"+currDate)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    
    df.to_parquet("../data/"+currDate+"/file.parquet")
    



