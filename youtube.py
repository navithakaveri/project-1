from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pymysql
import pandas as pd
import streamlit as st



#API CONNECTION

def api_connect():
    api_id="AIzaSyAAWf9VjADxH7nx_9CD_TK8C6KpSSLh4CQ"
    #api_id="AIzaSyBl0E4kWiD-oXL6fSrJtuu3cAFV1PJ6KuY"
    #api_id="AIzaSyBX1ptO4VmjXOzb-zzohxQInRYfzrY2-xU"
    #api_id="AIzaSyCNQO1GiETMxaZQa7WTduY_4_Drxq9bsrY"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=api_connect()


#GETTING CHANNEL INFORMATION
def get_channel_info(channel_id):
  
   request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id
   )
   response = request.execute()
   for i in response['items']:
      data=dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id=i["id"],
                  Subscribers=i["statistics"]["subscriberCount"],
                  Views=i["statistics"]["viewCount"],
                  Total_Videos=i['statistics']['videoCount'],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])

   return data





#GETTING VIDEO'S ID
def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
                                     
    playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    
    while True:
        response1 =youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")
        
        if next_page_token is None:
            break
    return video_ids 




#getting video information

def get_video_info(video_id):
    

    video_data=[]
    for video_ID in video_id:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ID
        )
        response=request.execute()
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
                        Channel_Id=i['snippet']['channelId'],
                        Video_Id=i['id'],
                        Title=i['snippet']['title'],
                        Thumbnail=i['snippet']['thumbnails']['default']['url'],
                        Comments=i['statistics'].get('commentCount'),
                        Like_Count=i['statistics'].get('likeCount'),
                        Dislike_Count=i['statistics'].get('dislikeCount'),
                        Faviorite=i['statistics']['favoriteCount'],
                        Description=i['snippet'].get('description'),
                        Published_Date=i['snippet']['publishedAt'],
                        Duration=i['contentDetails']['duration'],
                        Views=i['statistics'].get('viewCount'),
                        Definition=i['contentDetails']['definition'],
                        Caption_Status=i['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data
    





#GETTING COMMENTS INFORMATION
def get_comment_info(video_ids):
    
    Comment_data=[]
    try:
        for video_id in video_ids:
            
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for i in response['items']:
                data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                        Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                Comment_Published=i['snippet']['topLevelComment']['snippet']['publishedAt'])
            
            
            Comment_data.append(data)
            
    except:
        pass
    return Comment_data




#GETTING PLAYLIST INFORMATION
def get_playlist_details(channel_id):
    next_page_token=None
    all_data=[]
    while True:
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response=request.execute()
            for i in response['items']:
                data=dict(Playlist_Id=i['id'],
                          Title=i['snippet']['title'],
                          Channel_Id=i['snippet']['channelId'],
                          Channel_Name=i['snippet']['channelTitle'],
                          PublishedAt=i['snippet']['publishedAt'],
                          Video_Count=i['contentDetails']['itemCount']
                    
                    )
                all_data.append(data)
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                   break
    return all_data



#CONNECTION OF MONGODB

client=pymongo.MongoClient("mongodb://localhost:27017") # connection to mongo db


#CREATION OF DATABASE
db=client["youtube_project"] 


def chennal_detail(channel_id):# function creation
    channel_details=get_channel_info(channel_id)
    playlist_detail=get_playlist_details(channel_id)
    vi_id=get_video_ids(channel_id)
    video_detail=get_video_info(vi_id)
    comment_detail=get_comment_info(vi_id)
    
    #CREATION OF COLLECTION
    
    collection=db["chennal_details"] # channel_deatils name of the collection
    collection.insert_one({"channel_information":channel_details, 
                           "video_information":video_detail,
                           "playlist_information":playlist_detail,
                           "comment_information":comment_detail})
    
    
    return "upload completed"






#CHANNEL INFORMATION EXTRACT-TABLE CREATION

def channel_information():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='youtube_project')
    cursor=myconnection.cursor()

    check_table_query = "SHOW TABLES LIKE 'channels'"
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()
    
    if not table_exists:

        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(100) primary key,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total_Videos int,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        myconnection.commit()

        ch_list=[]
        db=client["youtube_project"]
        collection=db["chennal_details"]
        for ch_data in collection.find({},{"_id":0,"channel_information":1}):
            ch_list.append(ch_data["channel_information"])
            
        df=pd.DataFrame(ch_list)

        for index,row in df.iterrows():
                insert_query='''insert into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
                                                    
                values=(row["Channel_Name"],
                    row["Channel_Id"],
                    row["Subscribers"],
                    row["Views"],
                    row["Total_Videos"],
                    row["Channel_Description"],
                    row["Playlist_Id"])
                
                cursor.execute(insert_query,values)
                myconnection.commit()
            
            
#CONNECTION TO SQL
import pymysql
import mysql.connector
import datetime
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996')
cursor=myconnection.cursor()
            
            
            
#PLAYLIST DATA EXTRACT /TABLE CREATION

def playlist_information():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='youtube_project')
    cursor=myconnection.cursor()
    
    check_table_query = "SHOW TABLES LIKE 'play_list'"
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()
    
    if not table_exists:
        create_query='''create table if not exists play_list(Playlist_Id varchar(100) primary key,
                                                                Title varchar(100),
                                                                Channel_Id varchar(100),
                                                                Channel_Name varchar(100),
                                                                PublishedAt DATETIME,
                                                                Video_Count int)'''
                                                                
                                                                
                                                                
        cursor.execute(create_query)
        myconnection.commit()
        #extract data
        pl_list=[]
        db=client["youtube_project"]
        collection=db["chennal_details"]
        for pl_data in collection.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
            
        df1=pd.DataFrame(pl_list)
        #insering data into sql
        for index,row in df1.iterrows():
                insert_query='''insert into play_list(Playlist_Id,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    Video_Count)
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
                                                    
                                                    
                values=(row["Playlist_Id"],
                    row["Title"],
                    row["Channel_Id"],
                    row["Channel_Name"],
                    datetime.datetime.strptime(row["PublishedAt"],'%Y-%m-%dT%H:%M:%SZ'),
                    row["Video_Count"])
                
                
                
                cursor.execute(insert_query,values)
                myconnection.commit()
            
            
#CONNECTION TO SQL
import pymysql
import mysql.connector
import datetime
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996')
cursor=myconnection.cursor()

def comment_information():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='youtube_project')
    cursor=myconnection.cursor()

    check_table_query = "SHOW TABLES LIKE 'comments'"
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()
    
    if not table_exists:

        create_query='''create table if not exists comments(Comment_Id varchar(200),
                                                            Video_Id varchar(300),
                                                            Comment_Text text,
                                                            Comment_Author varchar(100),
                                                            Comment_Published DATETIME)'''
                                                        
        cursor.execute(create_query)
        myconnection.commit()

        cl_list=[]
        db=client["youtube_project"]
        collection=db["chennal_details"]
        for cl_data in collection.find({},{"_id":0,"comment_information":1}):
            for i in range(len(cl_data["comment_information"])):
               cl_list.append(cl_data["comment_information"][i])
            
        df4=pd.DataFrame(cl_list)

        for index,row in df4.iterrows():
            
            insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                values(%s,%s,%s,%s,%s)'''
            values=(row["Comment_Id"],
                row["Video_Id"],
                row["Comment_Text"],
                row["Comment_Author"],
                datetime.datetime.strptime(row["Comment_Published"],'%Y-%m-%dT%H:%M:%SZ')
                
                )
            cursor.execute(insert_query,values)
            myconnection.commit()
        
        
#VIDEO DATA EXTRACT /TABLE CREATION
db=client["youtube_project"] # db creation

import pymysql
import mysql.connector
import datetime

def video_information():
  myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='youtube_project')
  cursor=myconnection.cursor()

  check_table_query = "SHOW TABLES LIKE 'video_list'"
  cursor.execute(check_table_query)
  table_exists = cursor.fetchone()
  
  if not table_exists:
        create_query='''create table if not exists video_list(Channel_Name varchar(300),
                                                            Channel_Id varchar(300),
                                                            Video_Id varchar(300) primary key,
                                                            Title varchar(100),
                                                            Thumbnail varchar(250),
                                                            Comments int,
                                                            Like_Count int,
                                                            Dislike_Count int,
                                                            Faviorite int,
                                                            Description text,
                                                            Published_Date DATETIME,
                                                            Duration varchar(100),
                                                            Views int,
                                                            Definition varchar(100),
                                                            Caption_Status varchar(100))'''
                                                                                                                            
                                                        
        cursor.execute(create_query)
        myconnection.commit()

        vl_list=[]
        db=client["youtube_project"]
        collection=db["chennal_details"]
        for vl_data in collection.find({},{"_id":0,"video_information":1}):
            for i in range(len(vl_data["video_information"])):
                vl_list.append(vl_data["video_information"][i])
        
        df5=pd.DataFrame(vl_list)


    
        for index,row in df5.iterrows():
            insert_query='''insert into video_list(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Thumbnail,
                                                Comments,
                                                Like_Count,
                                                Dislike_Count,
                                                Faviorite,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Definition,
                                                Caption_Status)
                                                                                                                            
                                                        
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                
            values=(row["Channel_Name"],
                    row["Channel_Id"],
                    row["Video_Id"],
                    row["Title"],
                    row["Thumbnail"],
                    row["Comments"],
                    row["Like_Count"],
                    row["Dislike_Count"],
                    row["Faviorite"],
                    row["Description"],
                    datetime.datetime.strptime(row["Published_Date"],'%Y-%m-%dT%H:%M:%SZ'),
                    row["Duration"],
                    row["Views"],
                    row["Definition"],
                    row["Caption_Status"])
            
            cursor.execute(insert_query,values)
            myconnection.commit()
        
#FUNCTION OF ALL THE TABLE INFORMATION
def tables():
    channel_information()
    playlist_information()
    comment_information()
    video_information()
    
    return "created"



#STREAMLIT DATA

def show_channel_table():

    ch_list=[]
    db=client["youtube_project"]
    collection=db["chennal_details"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
        
    
    df=st.dataframe(ch_list)
    
    return df



def show_playlist_table():
    pl_list=[]
    db=client["youtube_project"]
    collection=db["chennal_details"]
    for pl_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
        
    df1=st.dataframe(pl_list)

    return df1




def show_video_table():
    
    vl_list=[]
    db=client["youtube_project"]
    collection=db["chennal_details"]
    for vl_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(vl_data["video_information"])):
           vl_list.append(vl_data["video_information"][i])
        
    df5=st.dataframe(vl_list)
    
    
def show_comment_table():
  cl_list=[]
  db=client["youtube_project"]
  collection=db["chennal_details"]
  for cl_data in collection.find({},{"_id":0,"comment_information":1}):
      for i in range(len(cl_data["comment_information"])):
        cl_list.append(cl_data["comment_information"][i])
      
  df4=st.dataframe(cl_list)
  
  return df4



#STREAM LIT
with st.sidebar:
    st.title(":black[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":red[SKILLS TAKE AWAY FROM THIS PROJECT]")
    st.caption(":black[API INTEGRATION]")
    st.caption(":black[DATA COLLECTION]")
    st.caption(":black[PYTHON SCRIPTING]")
    st.caption(":black[DATA MANAGEMENT USING MONGO AND SQL]")
    #styles={"backgroundColor": "#C80101"}
   
 
    
    
channel_id=st.text_input(":green[Enter the channel_Id]")
if st.button(":green[Collect and Store Data]"):
    ch_ids=[]
    db=client["youtube_project"]
    collection=db["channel_details"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exists")
    else:
        insert=chennal_detail(channel_id)
        st.success(insert)
        
if st.button(":green[Migrate of SQL]"):
    Table=tables()
    st.success(Table)
show_table=st.radio("select the table for view",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="PLAYLISTS":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()
    
#STREAMLIT QUESTIONS FRAMING AND QUERY
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996')
cursor=myconnection.cursor()
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='youtube_project')
cursor=myconnection.cursor()

st.write(":green[SELECT ANY QUESTION]")
questions = ['1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
choice_ques = st.selectbox('Questions : Click the question that you would like to query',questions)



if choice_ques == questions[0]:
    df = pd.read_sql_query('''select Title as videos,channel_Name as channelname from video_list''',myconnection)
    st.write(df)
    
elif choice_ques == questions[1]:
    df = pd.read_sql_query('''select Channel_Name as channel_name,Total_Videos as no_of_videos from channels order by Total_Videos desc''',myconnection)
    st.write(df)
  
    
elif choice_ques == questions[2]:
    df = pd.read_sql_query('''select Views as views,Channel_Name as channel_name,Title as video_title from video_list  where Views is not null order by Views desc limit 10''',myconnection)
    st.write(df)
    
elif choice_ques == questions[3]:
    df = pd.read_sql_query('''select Comments as no_of_comments,Title as video_title from video_list where Comments is not null''',myconnection)
    st.write(df)
        
elif choice_ques == questions[4]:
    df = pd.read_sql_query('''select Title as video_title,Channel_Name as channel_name, Like_Count as likes from video_list where Like_Count is not null order by Like_Count desc''',myconnection)
    st.write(df)
    
elif choice_ques == questions[5]:
    df = pd.read_sql_query('''select Like_Count as like_count,Dislike_Count as dislike_count,Title as video_title from video_list''',myconnection)
    st.write(df)
 
        
elif choice_ques == questions[6]:
    df = pd.read_sql_query('''select Views as views,Channel_Name as channel_name from video_list''',myconnection)
    st.write(df)
    
elif choice_ques == questions[7]:
    df = pd.read_sql_query('''select Title as video_title,Published_Date as published_date,Channel_Name as channel_name from video_list 
            where extract(year from Published_Date)=2022''',myconnection)
    st.write(df)

elif choice_ques == questions[8]:
    df = pd.read_sql_query("""SELECT Channel_Name, 
                    SUM(duration_sec) / COUNT(*) AS average_duration
                    FROM (
                        SELECT Channel_Name, 
                        CASE
                            WHEN Duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                            TIME_TO_SEC(CONCAT(
                            SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'H', 1), 'T', -1), ':',
                        SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'H', -1), ':',
                        SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1)
                        ))
                            WHEN Duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                            TIME_TO_SEC(CONCAT(
                            '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'S', 1), 'M', -1)
                        ))
                            WHEN Duration REGEXP '^PT[0-9]+S$' THEN 
                            TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                            END AS duration_sec
                    FROM video_list
                    ) AS subquery
                    GROUP BY Channel_Name""",myconnection)
    st.write(df)
   
elif choice_ques == questions[9]:
    df = pd.read_sql_query('''select Title as video_title,Comments as comments,Channel_Name as channel_name from video_list where Comments is not null order by Comments desc''',myconnection)
    st.write(df) 
    
    

   
