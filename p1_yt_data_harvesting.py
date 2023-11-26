import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
import pandas as pd
from googleapiclient.discovery import build
import mysql.connector
from bson import ObjectId
import re
import plotly.express as px


from googleapiclient.discovery import build
api_key=#your API Key
youtube = build('youtube', 'v3', developerKey=api_key)


con = pymongo.MongoClient("mongodb://localhost:27017/")
#Database for storing the data
db=con['Project1_Youtube_Final']
#Single Collection for storing the data
col=db['youtube_data_collection']


#Conecting to MySQL
mydb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='12345678'
                              )
#Creating Cursor 
mycursor = mydb.cursor()
#Switching to the DB Created
sql="USE Project1_Youtube"
mycursor.execute(sql)


# Data Extraction/Harvesting
def channel_details(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
    )
    response = request.execute()
    channel_data = dict(channel_name = response['items'][0]['snippet']['title'],
    channel_id = response['items'][0]['id'],                    
    channel_des = response['items'][0]['snippet']['description'],
    channel_pat = response['items'][0]['snippet']['publishedAt'],
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    channel_views = response['items'][0]['statistics']['viewCount'],
    subscription_count = response['items'][0]['statistics']['subscriberCount'],
    video_count = response['items'][0]['statistics']['videoCount'])

    
    return channel_data
#Function to return all video ids from the playlist id passed as argument
def video_id(p_id):
#playlistitems
#To pull video id from playlist items data
    pl_request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=p_id,
        maxResults=50
    )
    v_ids=[]
    while pl_request:   
        pl_response = pl_request.execute()
        #print(pl_response)

        for i in pl_response["items"]:
            vid=i['contentDetails']['videoId']
            v_ids.append(vid)

        if 'nextPageToken' in pl_response:
            pl_request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=p_id,
            maxResults=50,
            pageToken=pl_response['nextPageToken']
            )
        else:
            break

    return v_ids        
#now get video data using video id from Video reference
#joining the items in v_ids list by comma, which can be given as an IP to the following API request
def video_details(v_id):
    videos_data={}
    for i in range(0,len(v_id),50):
        #video_id_string = ",".join(video_ids)
        #print(video_id_string)
        v_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id= ",".join(v_id[i:i+50])
        )
        v_response = v_request.execute()
        #print(v_response)
        for item in v_response['items']:
            video_id=item['id']
            video_data=dict(
            video_id = item['id'],
            video_name = item['snippet']['title'],
            video_channel = item['snippet']['channelTitle'],
            video_channel_id = item['snippet']['channelId'],
            video_description = item['snippet'].get('description'),
            video_tags = item['snippet'].get('tags'),
            video_publishedat = item['snippet']['publishedAt'],
            video_viewcount = item['statistics'].get('viewCount'),
            video_likecount = item['statistics'].get('likeCount'),
            video_favcount = item['statistics'].get('favoriteCount'),
            video_commcount = item['statistics'].get('commentCount'),
            video_duration = item['contentDetails']['duration'],
            video_thumbnail = item['snippet']['thumbnails']['default']['url'],
            video_captsts = item['contentDetails']['caption']
            )
            videos_data[video_id]=video_data
    
    return videos_data
#to get the comment data
def comment_details(v_id):
    comments={}
    for i in v_id:
        c_request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=i,
        maxResults=100
        )
        try:
            c_response = c_request.execute()
        except Exception as e:
            print(e)
            continue
        
        for i in c_response["items"]:
            c_id=i['id']
            c_txt=i['snippet']['topLevelComment']['snippet']['textDisplay']
            c_aut=i['snippet']['topLevelComment']['snippet']['authorDisplayName']
            c_publ=i['snippet']['topLevelComment']['snippet']['publishedAt']
            c_vid=i['snippet']['videoId']
            comment_data = {
                'Comment_Id':c_id,
                'Comment_Text':c_txt,
                'Comment_Author':c_aut,
                'Comment_PublishedAt':c_publ,
                'Comment_Vd_ID':c_vid
            }
            comments[c_id]=comment_data
    return comments
#main fn to get the full data
def main(channel_id):
    channel_data=channel_details(channel_id)
    playlist_id=channel_data['playlist_id']
    video_ids=video_id(playlist_id)
    video_data = video_details(video_ids)
    comment_data = comment_details(video_ids)
    
    final_data={
        'Channel Details':channel_data,
        'Video Details':video_data,
        'Comment Details':comment_data
    }
    return final_data


# Data Warehousing
# To MongoDB
# Function to add data to MongoDB
def data_to_mongodb(data):
    try:
        con = pymongo.MongoClient("mongodb://localhost:27017/")
        #Database for storing the data
        db=con['Project1_Youtube_Final']
        #Single Collection for storing the data
        col=db['youtube_data_collection']
        col.insert_one(data)
    except Exception as e:
        print(f"Error is : {e}")
    finally:    
        con.close()


# Now fetching and inserting into MySQL
# 3 Tables - Channel, Comment, Video
# Fetching data from MongoDB and storing it into a df
#Function that takes the channel_name as input and then returns the 3 dataframes 
def get_df(channel_name):
    #Getting the object ID based on the channel name we provide
    for document in col.find({'Channel Details.channel_name':channel_name}):
        i = document['_id']
        #print(i)
        
    # Creating Channel DF by just providing the object id
    for document in col.find({'_id':i},{'Channel Details':1,'_id':0}):
        #print(document['Channel Details'])
        channel_details_list = [document['Channel Details']]
    channel_df=pd.DataFrame(channel_details_list)
    
    # Creating Video DF by just providing the object id
    video_data_list = []
    for document in col.find({'_id':i},{'Video Details':1,'_id':0}):
        video_details = (document['Video Details'])
        for v_id,v_data in video_details.items():
            video_data_list.append(v_data)
    video_df = pd.DataFrame(video_data_list)
    
    # Creating Comment DF by just providing the object id
    comment_data_list = []
    for document in col.find({'_id':i},{'Comment Details':1,'_id':0}):
        comment_details = (document['Comment Details'])
        for c_id,c_data in comment_details.items():
            comment_data_list.append(c_data)
    comment_df = pd.DataFrame(comment_data_list)
    
    
    return channel_df,video_df,comment_df


# Migrating Data To SQL from DataFrames
# Preprocessing Channel data and storing it in MySQL
# Need to convert the datatypes of data present in df to appropriate ones
# Function to preprocess and store the channel data in sql channel table
def ch_df_to_sql(channel_df):
    channel_df['channel_views'] = channel_df['channel_views'].astype(int)
    channel_data = channel_df[['channel_id','channel_name','channel_views','channel_des']]
    channel_data_to_insert = [tuple(row) for row in channel_data.values]
    #Inserting Channel Data to MySQL
    try:
        sql="INSERT INTO Channel(channel_id, channel_name, channel_views, channel_description) VALUES(%s,%s,%s,%s)"
        mycursor.executemany(sql,channel_data_to_insert)
        mydb.commit()
    except Exception as e:
        print("Data already Present : ",e)
# Preprocessing Video data and storing it in MySQL
# Function to preprocess and store the video data in sql video table
def v_df_to_sql(video_df):
    # Function to convert the duration to int
    def convert_to_seconds(duration_str):
        total_seconds = 0

        matches = re.findall(r'(\d+)([HMS])', duration_str)

        for value, unit in matches:
            value = int(value)
            if unit == 'H':
                total_seconds += value * 3600
            elif unit == 'M':
                total_seconds += value * 60
            elif unit == 'S':
                total_seconds += value

        return total_seconds

    video_df['video_publishedat'] = pd.to_datetime(video_df['video_publishedat'], format='%Y-%m-%dT%H:%M:%SZ')
    try:
        video_df['video_viewcount'] = pd.to_numeric(video_df['video_viewcount'],errors='coerce').fillna(0).astype(int)
        video_df['video_likecount'] = pd.to_numeric(video_df['video_likecount'],errors='coerce').fillna(0).astype(int)
        video_df['video_favcount'] = pd.to_numeric(video_df['video_favcount'],errors='coerce').fillna(0).astype(int)
        video_df['video_commcount'] = pd.to_numeric(video_df['video_commcount'],errors='coerce').fillna(0).astype(int)
    except Exception as e:
        print(e)
    video_df['video_duration'] = video_df['video_duration'].apply(convert_to_seconds)
    video_data = video_df[['video_id', 'video_name', 'video_description', 'video_publishedat', 'video_viewcount', 'video_likecount', 'video_favcount', 'video_commcount', 'video_duration', 'video_thumbnail', 'video_captsts','video_channel']]
    vdata_to_insert = [tuple(row) for row in video_data.values]

    #Inserting Video Data to MySQL
    try:
        sql="INSERT INTO Video(video_id, video_name, video_description, published_date, view_count, like_count, favorite_count, comment_count, duration, thumbnail, caption_status, video_channel) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        mycursor.executemany(sql,vdata_to_insert)
        mydb.commit()
    except Exception as e:
        print("Data already Present : ",e)
# Function to preprocess and store the comment data in sql comment table
def ct_df_to_sql(comment_df):
    comment_df['Comment_PublishedAt'] = pd.to_datetime(comment_df['Comment_PublishedAt'], format = '%Y-%m-%dT%H:%M:%SZ')
    comment_data = comment_df[['Comment_Id', 'Comment_Vd_ID', 'Comment_Text', 'Comment_Author', 'Comment_PublishedAt']]
    cdata_to_insert = [tuple(row) for row in comment_data.values]
    try:
        sql = 'INSERT INTO Comment(comment_id, video_id, comment_text, comment_author, comment_published_date) values(%s,%s,%s,%s,%s)'
        mycursor.executemany(sql,cdata_to_insert)
        mydb.commit()
    except Exception as e:
        print("Data already Present : ",e)


# Main Migration Function to Transform and Migrate data from MongoDB to MySQL
#fn to return the channel names from MongoDB
def get_cnames():
    channel_names=[]
    for document in col.find():
        cname = document['Channel Details']['channel_name']
        channel_names.append(cname)

    return channel_names
#Main Function to migrate data from MongoDB to MySQL when a channel_name is given as input
def main_migration(channel_name):
    #creating dfs for channel,comment and video data
    c_df,v_df,ct_df = get_df(channel_name)
    #preprocessing channel data and storing in MySQL channel table
    ch_df_to_sql(c_df)
    #preprocessing video data and storing in MySQL video table
    v_df_to_sql(v_df)
    #preprocessing comment data and storing in MySQL comment table
    ct_df_to_sql(ct_df)


# Final Part


st.set_page_config(
    page_title="Youtube Data Analysis by Devadath",
    page_icon="▶️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This app was developed by Devadath G Nair!"
    }
)

with st.sidebar:
    selected = option_menu("Main Menu", ["Home", 'About'], icons=['house', 'info'], menu_icon="cast", default_index=0)
    

if selected == "Home":
    st.title(':red[YouTube Data Analysis]')
    st.text('By Devadath G Nair')

    tab1, tab2, tab3 = st.tabs(["Extract", "Preprocess", "Analyze"])

    with tab1:
        st.header("Extract The Data")
        channel_id = st.text_input('Enter the youtube channel id for extracting data')
        if len(channel_id)>1:
            data1 = main(channel_id)
        if st.button('Extract'):
            with st.expander("View Extracted Data"):
                st.write(data1)
        if st.button('Store to MongoDB'):
            data_to_mongodb(data1)
            st.success('The data was successfully stored in MongoDB!', icon="✅")
    
    with tab2:
        st.header("Preprocess And Migrate The Data To SQL")
        channel_names = get_cnames()
        cname = st.selectbox(
            'Choose the channel for which you intend to transfer data from MongoDB to MySQL.',
            (channel_names))
        if st.button('Migrate'):
                main_migration(cname)
                st.success('The data was successfully Migrated to MySQL!', icon="✅")

    with tab3:
        st.header("Data Analysis and Insights")
        qs = ["1. What are the names of all the videos and their corresponding channels?",
                "2. Which channels have the most number of videos, and how many videos do they have?",
                "3. What are the top 10 most viewed videos and their respective channels?",
                "4. How many comments were made on each video, and what are their corresponding video names?",
                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                "6. What is the total number of likes for each video, and what are their corresponding video names?",
                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                "8. What are the names of all the channels that have published videos in the year 2022?",
                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]
        qn = st.selectbox(
            'Select any questions from below to gain insights',
            (qs))
        if qn==qs[0]:
            sql = "SELECT video_name, video_channel FROM video ORDER BY video_channel;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Video Name','Channel Name'])
            data_df.index = data_df.index+1
            st.title("List Of All Videos")
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[1]:
            sql = "SELECT video_channel, count(video_id) FROM video GROUP BY video_channel;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Channel Name','Total Number of Videos'])
            data_df.index = data_df.index+1
            st.title("Channels With Most Videos")
            with st.container():
                st.subheader("Channel vs Number Of Videos")
                fig = px.bar(data_df,
                             x='Channel Name',
                             y='Total Number of Videos',
                             color='Channel Name')
                st.plotly_chart(fig,use_container_width=True)
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[2]:
            sql = "SELECT video_name,video_channel,view_count FROM video ORDER BY view_count DESC LIMIT 10;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Video Name','Channel Name','Total Number of Views']).set_index(pd.Index(range(1, len(data) + 1)))
            #data_df.index = data_df.index+1
            #df = data_df
            #data_df.index = range(1, len(data_df) + 1)
            st.title("Top 10 Most Viewed Videos")
            with st.container():
                st.subheader("Channel vs Number Of Videos")
                #st.bar_chart(data_df,x='Video Name',y='Total Number of Views',color='Channel Name')
                fig = px.bar(data_df,
                     x='Video Name',
                     y='Total Number of Views',
                     color='Channel Name'
                    )
                st.plotly_chart(fig,use_container_width=True)
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[3]:
            sql = "SELECT video.video_name, count(comment.comment_id) FROM comment LEFT JOIN video ON comment.video_id = video.video_id GROUP BY comment.video_id;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Video Name','Comment Count'])
            data_df.index = data_df.index+1
            st.title("Total Comments On Each Videos")
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[4]:
            sql = "SELECT video_name,like_count,video_channel from video order by like_count DESC;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Video Name','Like Count','Channel Name'])
            data_df.index = data_df.index+1
            st.title("Most Liked Videos")
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[5]:
            sql = "SELECT video_name, like_count FROM video;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Video Name','Total Number of Likes'])
            data_df.index = data_df.index+1
            st.title("Total Number Of Likes For Each Videos")
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[6]:
            sql = "SELECT channel_name,channel_views FROM channel;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Channel Name','Total Number of Views'])
            data_df.index = data_df.index+1
            st.title("Total Number Of Views For Each Channels")
            with st.container():
                st.subheader("Channel vs Number Of Views")
                #st.bar_chart(data_df,x='Video Name',y='Total Number of Views',color='Channel Name')
                fig = px.bar(data_df,
                     x='Channel Name',
                     y='Total Number of Views',
                     color='Channel Name'
                    )
                st.plotly_chart(fig,use_container_width=True)
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[7]:
            sql = "SELECT DISTINCT(video_channel) FROM video WHERE published_date>'2022-01-01 00:00:00';"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Channels That Published Videos In The Year 2022'])
            data_df.index = data_df.index+1
            st.title("Channels That Published Videos In The Year 2022")
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[8]:
            sql = "SELECT DISTINCT(video_channel), AVG(duration) FROM video GROUP BY video_channel;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Channel Name','Average Video Duration(in Seconds)'])
            data_df.index = data_df.index+1
            st.title("Average Video Duration Of Each Channels")
            with st.container():
                st.subheader("Channel vs Video Duration")
                fig = px.bar(data_df,
                     x='Channel Name',
                     y='Average Video Duration(in Seconds)',
                     color='Channel Name'
                    )
                st.plotly_chart(fig,use_container_width=True)
            with st.container():
                st.subheader("Data Table")
                st.write(data_df)

        elif qn==qs[9]:
            sql = "SELECT video_name,comment_count,video_channel FROM video ORDER BY comment_count DESC;"
            mycursor.execute(sql)
            data = mycursor.fetchall()
            data_df = pd.DataFrame(data,columns=['Video Name','Number Of Comments','Channel Name'])
            data_df.index = data_df.index+1
            st.title('Videos With Highest Number Of Comments')
            with st.container():
                st.subheader('Data Table')
                st.write(data_df)   


elif selected == "About":
    st.title(':red[YouTube Data Analysis]')
    st.text('By Devadath G Nair')
    #st.markdown("**PROJECT TITLE** : *YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit*.")
    st.subheader("**PROJECT TITLE** : :gray[_YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit_]", divider='blue')
    st.subheader("**SKILLS TAKE AWAY FROM THIS PROJECT** : :gray[_Python scripting, Data Collection,MongoDB, Streamlit, API integration, Data Management using MongoDB (Compass) and SQL_ and Data Visualization using Plotly]", divider='blue')
    st.subheader("**DOMAIN** : :gray[_Social Media_]", divider='blue')
    #st.divider()
    st.subheader("**:blue[PROBLEM STATEMENT]**")
    multi = ''' The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
    1. Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
    2. Option to store the data in a MongoDB database as a data lake.
    3. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
    4. Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
    5. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

    '''
    st.markdown(multi)
