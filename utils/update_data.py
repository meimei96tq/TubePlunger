import pymysql.cursors
# from TubeSage import query_data
# import query_data
# import pandas as pd
# from tqdm import tqdm
# import json

connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='kelab5136',
                             database='tube_sage',
                             cursorclass=pymysql.cursors.DictCursor)

connection.ping(reconnect=True)
# print('Connected to DB')
conn = connection.cursor()


# update bias of users

# update dashboard: videos_count, users_count, comments_count from channels
def get_videos_count(channel_id):
    sql_ = "SELECT count(distinct video_id) " \
           "as videos_count " \
           "FROM videos " \
           "where channel_id = %s; "
    conn.execute(sql_, channel_id)
    videos_count = conn.fetchall()[0]['videos_count']
    return videos_count


def get_users_count(channel_id):
    sql_ = "select count(distinct user_id) " \
           "as users_count " \
           "from comments inner join videos " \
           "on comments.video_id = videos.video_id " \
           "where videos.channel_id = %s; "
    conn.execute(sql_, channel_id)
    users_count = conn.fetchall()[0]['users_count']
    return users_count


def get_comments_count(channel_id):
    sql_ = "select count(distinct comment_id) " \
           "as comments_count " \
           "from comments inner join videos " \
           "on comments.video_id = videos.video_id " \
           "where videos.channel_id = %s; "
    conn.execute(sql_, channel_id)
    comments_count = conn.fetchall()[0]['comments_count']
    return comments_count


def update_dashboard(channel_id):
    # print("update_dashboard")
    sql_ = "Update channels " \
           "SET videos_count = %s, users_count= %s, comments_count = %s " \
           "where channel_id = %s; "
    conn.execute(sql_, (get_videos_count(channel_id), get_users_count(channel_id), get_comments_count(channel_id),
                        channel_id))
    connection.commit()


# print(get_videos_count('UCsF3Gj3ifEDLECWQWFT8twQ'))


