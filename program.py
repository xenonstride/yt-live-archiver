import requests
import json
import time
from streamlink import Streamlink
import pytz
from datetime import datetime

ACCESS_TOKEN=''
TOKEN_EXPIRE_TIME=0
FILE_SIZE=6*1000*1000*1000 
CHUNK_SIZE=256*4096*10 #2MB
STREAM_EXPIRE_TIME=0
PLAYLIST_ID='PLYrn8-Z25bGiA7qub18CRx8kkqgo2LdFi'

# NASASPACEFLIGHT STARBASE LIVE 24/7
STREAM_LINK='https://www.youtube.com/watch?v=mhJRzQsLZGg'
DESCRIPTION=f"""Credit: NASA Spaceflight\nOriginal Stream Link: {STREAM_LINK}\n\nArchive of NASA Spaceflight's Starbase LIVE 24/7\nOnly for archive purposes, no intent of monetizing off the content.\nAll rights reserved to NASA Spaceflight.\nPlease contact if you want the content to be removed."""

time_zone = pytz.timezone('US/Central')

def get_date_time_in_starbase():
    us_east_time = datetime.now(time_zone)
    return us_east_time.strftime('%Y-%m-%d %I:%M %p') 

def check_token():
    if time.time()>TOKEN_EXPIRE_TIME:
        token_refresh()
        with open('new_token.txt','w+') as t:
            t.write(ACCESS_TOKEN)

def check_stream_expiry():
    if time.time()>STREAM_EXPIRE_TIME:
        return True
    return False

def token_refresh():
    global ACCESS_TOKEN
    global TOKEN_EXPIRE_TIME

    #get OAuth Creds from a file
    with open('auth-2.json','r') as credentials:
        secrets = json.load(credentials)
        CLIENT_ID=secrets['CLIENT_ID']
        CLIENT_SECRET=secrets['CLIENT_SECRET']
        REFRESH_TOKEN=secrets["REFRESH_TOKEN"]

    REFRESH_TOKEN_URL=f"https://oauth2.googleapis.com/token?client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&grant_type=refresh_token&refresh_token={REFRESH_TOKEN}"

    res = requests.post(REFRESH_TOKEN_URL)
    res = res.json()
    
    #set the ACCESS_TOKEN and TOKEN_EXPIRE_TIME (100s less than the given time)
    ACCESS_TOKEN=res['access_token']
    TOKEN_EXPIRE_TIME=time.time()+res['expires_in']-100

def create_new_vid(title):
    check_token()

    req_headers={
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'X-Upload-Content-Length': f'{FILE_SIZE}',
        'x-upload-content-type': 'application/octet-stream',
        'Content-Type': 'application/json; charset=UTF-8'
    }

    #YT video properties
    # set title, description, privacy
    req_body={
        "snippet": {
            "title": title
        },
        "status": {
            "privacyStatus": "private"
        }
    }

    API_REQ_URL="https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status"

    #Get the video upload URL
    res=requests.post(API_REQ_URL,data=json.dumps(req_body),headers=req_headers)
    print("Creating video: ",res.status_code)
    
    try:
        loc = res.headers['Location']
    except Exception as e:
        print(e)
        print(res.json())
        loc="error"
    return loc


def add_vid_to_playlist(video_id):
    check_token()

    API_REQ_URL='https://www.googleapis.com/youtube/v3/playlistItems?part=snippet'
    
    headers={
        "Authorization": f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json; charset=UTF-8'
    }
    
    req_body={
        "snippet": {
            "playlistId": PLAYLIST_ID,
            "resourceId": {
                "kind": "youtube#video",
                "videoId": video_id
            }
        }
    } 

    res=requests.post(API_REQ_URL,data=json.dumps(req_body),headers=headers)
    print("Adding to Playlist",res.json())


def change_title(video_id,old_title,categoryId):
    check_token()

    API_REQ_URL='https://www.googleapis.com/youtube/v3/videos?part=snippet'
    
    headers={
        "Authorization": f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json; charset=UTF-8'
    }

    req_body={
        "id": video_id,
        "snippet": {
            "title": f'{old_title}-end-{get_date_time_in_starbase()}',
            "description": DESCRIPTION,
            "categoryId": categoryId
        }
    }

    res=requests.put(API_REQ_URL,data=json.dumps(req_body),headers=headers)
    print("Updating Title",res.json())


def upload_chunk(loc,data,prev_byte):
    check_token()

    # chunks starts from 0
    # if file size is 200000 then total bytes range from 0-199999
    start=prev_byte+1
    end=start+CHUNK_SIZE-1
    
    # check if last chunk
    if end>FILE_SIZE-1:
        end=FILE_SIZE-1

    req_headers = {
        "Authorization": f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/octet-stream',
        'Content-Length': f'{CHUNK_SIZE}',
        'Content-Range': f'bytes {start}-{end}/{FILE_SIZE}'
    }

    res=requests.put(loc,data=data,headers=req_headers)
    print(f'{start}-{end}   Status: {res.status_code}')
    # print(res.headers)

    # if file completed return -2 (any signal)
    if end==FILE_SIZE-1:
        try:
            complete_data=res.json()
            print(complete_data['id'])
            add_vid_to_playlist(complete_data['id'])
            change_title(complete_data['id'],complete_data['snippet']['title'],complete_data['snippet']['categoryId'])
        except Exception as e:
            print(e) 
        return -2
    else:
        return end

token_refresh()
print("Token: ",ACCESS_TOKEN)

def stream_thread(stream_name,stream_link):
    global STREAM_EXPIRE_TIME
    QUALITY='720p'

    session = Streamlink()
    session.set_option('hls-live-edge',9999)
    session.set_option('hls-segment-threads',5)
    #get raw stream link
    streams=session.streams(stream_link)
    STREAM_EXPIRE_TIME=int(streams[QUALITY].url.split('/')[7])-100
    stream = streams[QUALITY]
    stream_data = stream.open()

    # repeat until stopped forcefully
    while True:
        # set to -1 because the first chunk starts from 0 which is calculated in upload
        prev_byte=-1 
        vid_name=f'{stream_name}-start-{get_date_time_in_starbase()}'
        video_location=create_new_vid(vid_name)

        # video_location="https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status&upload_id=ADPycdt8LCUaaaiazR-VSjeLOVxE4gaVUfN_GhcUFlQg5tPt1jIr0EqEnfT_NRsh30nX1gop2nf7ekhVdcqMuPfk-nm8F4NIlg"
        print("Video Location: ",video_location)

        # store for the buffer bytes
        store = bytearray()

        #loop for each video file
        while True:
            if not check_stream_expiry():
                #print(f'0- {prev_byte}')

                # check if it is the last chunk
                # same as (prev_byte+1)+CHUNK_SIZE-1>FILE_SIZE-1
                if prev_byte+1+CHUNK_SIZE>FILE_SIZE:
                    stream_chunk=stream_data.read(FILE_SIZE-prev_byte-1)
                    check=FILE_SIZE-prev_byte-1
                
                #if not last chunk
                else:
                    stream_chunk=stream_data.read(CHUNK_SIZE)
                    check=CHUNK_SIZE
                
                #add the current chunk from the stream to the store
                store.extend(stream_chunk)

                # if length of the store is greater than the size required for upload chunk to youtube then get it from the store array and upload
                # here check is used so that it works for the last chunk also
                if len(store)>check:
                    #print(check)
                    up_chunk=store[:check]
                    prev_byte=upload_chunk(video_location, up_chunk, prev_byte)
                    store=store[check:]

                # check if the complete file has been uploaded
                if prev_byte==-2:
                    break 
                #print('--------------------------------')
            else:
                print("getting new raw stream link")
                stream_data.close()
                session=None
                session = Streamlink()
                session.set_option('hls-live-edge',9999)
                session.set_option('hls-segment-threads',5)
                streams=session.streams(stream_link)
                STREAM_EXPIRE_TIME=int(streams[QUALITY].url.split('/')[7])-100
                stream = streams[QUALITY]
                stream_data = stream.open()

        # remove this break when running for unlimited time
        break

try:
    stream_thread("NSF-Starbase-24/7",STREAM_LINK)
except Exception as e:
    print("Some exception")
    print(e)