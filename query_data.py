import pandas as pd
import collections
from TubeSage.utils import datetime_utils
from datetime import datetime
import datetime as dtn
import requests
import pymysql


# from collections import ChainMap
# from itertools import groupby


def get_distribution(data_list, time_option):
    if time_option == 'second':  # minute
        pivot = 0
    elif time_option == 'minute':  # hour
        pivot = 3
    elif time_option == 'hour':  # day
        pivot = 6
    elif time_option == 'day':  # month
        pivot = 9
    else:
        pivot = 0
    counter = collections.Counter()
    if pivot == 0:
        for index, data in data_list.iterrows():
            time_str = data['comment_postdate']
            time_str = time_str[0:10] + " " + time_str[11:19]
            time_obj = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            time_kr = datetime_utils.utc_to_local(time_obj).strftime("%Y-%m-%d %H:%M:%S")
            counter.update([time_kr])
    else:
        for index, data in data_list.iterrows():
            time_str = data['comment_postdate']
            time_str = time_str[0:10] + " " + time_str[11:19]
            time_obj = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            time_kr = datetime_utils.utc_to_local(time_obj).strftime("%Y-%m-%d %H:%M:%S")

            time_kr = time_kr[:-pivot]
            time_short = datetime_utils.correct_time(time_kr)
            counter.update([time_short])

    counter_list = sorted(counter.items())
    x_data_date = [x for x, y in counter_list]
    y_data = [y for x, y in counter_list]
    return x_data_date, y_data


def get_data(channel_name, connection, conn):
    # limit_date = '2022-01-01T00:00:00Z'
    connection.ping(reconnect=True)
    sql = "SELECT DISTINCT comments.comment_id, comments.user_id, comments.video_id, videos.video_title, " \
          "videos.channel_title, comments.sentiment, comments.comment_postdate " \
          "FROM comments INNER JOIN videos " \
          "ON comments.video_id = videos.video_id " \
          "WHERE videos.channel_title = %s; "
    conn.execute(sql, channel_name)
    data = conn.fetchall()
    return data


def get_top_user(channel_title, connection, conn):
    connection.ping(reconnect=True)
    sql_ = "SELECT top_users.user_id, top_users.comments_count " \
           "FROM top_users WHERE top_users.channel_title = %s LIMIT 15; "
    conn.execute(sql_, channel_title)
    data = conn.fetchall()
    return data


def get_channel_info(channel_title, connection, conn):
    connection.ping(reconnect=True)
    sql_ = "SELECT video_id, video_title, video_date_published, " \
           "neg_comments_count, pos_comments_count,und_comments_count, comments_count " \
           "FROM videos WHERE channel_title = %s ORDER BY video_date_published DESC; "
    conn.execute(sql_, channel_title)
    data = conn.fetchall()
    for row in data:
        # if len(row['video_date_published']) != 0:
        # print(row['video_date_published'])
        dt = datetime.strptime(row['video_date_published'], '%Y-%m-%d')
        row['video_date_published'] = dtn.date(dt.year, dt.month, dt.day)
    return data


def get_comment_by_user(user_id, connection, conn):
    # limit_date = '2022-01-01T00:00:00Z'
    connection.ping(reconnect=True)
    sql = "SELECT DISTINCT comments.comment_id, comments.video_id, videos.video_title, videos.channel_id, " \
          "comments.comment_content, comments.comment_postdate, comments.sentiment, videos.channel_title " \
          "FROM comments INNER JOIN videos " \
          "ON comments.video_id = videos.video_id " \
          "WHERE comments.user_id = %s " \
          "ORDER BY videos.channel_title; "
    conn.execute(sql, (user_id))
    data = conn.fetchall()
    df = pd.DataFrame(data)
    return df


def get_channels(channel_title, connection, conn):
    connection.ping(reconnect=True)
    if channel_title == "":
        sql_channels = "SELECT * FROM channels; "
        conn.execute(sql_channels)
    else:
        sql_channels = "SELECT * FROM channels where channel_title = %s; "
        conn.execute(sql_channels, (channel_title.lower()))
    channels = conn.fetchall()
    return channels


def insert_channel(channel_id, connection, conn):
    sql_check = "SELECT * FROM channels WHERE channels.channel_id = %s ; "
    conn.execute(sql_check, channel_id)
    result = conn.fetchall()
    if len(result) == 0:
        parameters = {'part': 'snippet', 'channelId': channel_id, 'key': 'AIzaSyAO9vuH7rcUf4JAiap0cJ9FQwRGyfz1b6Q',
                      'maxResults': '1'}
        r = requests.get('https://www.googleapis.com/youtube/v3/search', params=parameters)
        search_list = r.json()
        if 'error' in search_list:
            flag = 'Not_Exist'
            return flag
        else:
            channel_title = search_list["items"][0]["snippet"]["channelTitle"]
            connection.ping(reconnect=True)
            sql = "INSERT INTO channels (id, channel_id, channel_title, status, " \
                  "                     leftwing_bias, rightwing_bias, political_bias) " \
                  "VALUE (default, %s, %s, %s, %s, %s, %s); "
            conn.execute(sql, (channel_id, channel_title, 'collecting', 0, 0, 3))
            connection.commit()
            flag = 'Inserted'
            return flag

    flag = 'Duplicated'
    return flag


def get_comment_video(channel_title, connection, conn):
    # connection = pymysql.connect(host='localhost',
    #                              port=3307,
    #                              user='root',
    #                              password='kelab5136',
    #                              database='tube_sage',
    #                              cursorclass=pymysql.cursors.DictCursor)
    #
    connection.ping(reconnect=True)
    # conn = connection.cursor()
    sql = "Select video_id, comments_distribution_day, comments_day, video_title from videos where channel_title = %s; "
    conn.execute(sql, channel_title.lower())
    data = conn.fetchall()
    print(data)
    for row in data:
        if row["comments_distribution_day"] != None:
            tmp_comments_distribution_day = row["comments_distribution_day"].replace("[", "").replace("]", "")
            tmp_comments_distribution_day = tmp_comments_distribution_day.split(",")
            row["comments_distribution_day"] = list(map(int, tmp_comments_distribution_day))
            tmp_comments_day = row["comments_day"]
            tmp_comments_day = tmp_comments_day.replace("[", "").replace("]", "").replace('"', "")
            tmp_comments_day = tmp_comments_day.split(",")
            row["comments_day"] = tmp_comments_day
        else:
            row["comments_day"] = []
            row["comments_distribution_day"] = []
    # print(data)
    return data


# get_comment_video('사람사는세상노무현재단')


def get_comment_sentiment(channel_title, connection, conn):
    connection.ping(reconnect=True)
    sql = "Select neg_comments_count, neg_comments_day, pos_comments_count, pos_comments_day, " \
          "und_comments_count, und_comments_day from channels where channel_title = %s; "
    conn.execute(sql, channel_title.lower())
    data = conn.fetchall()
    for row in data:
        if row["neg_comments_count"] != None:
            tmp_neg_comments_count = row["neg_comments_count"].replace("[", "").replace("]", "")
            tmp_neg_comments_count = tmp_neg_comments_count.split(",")
            row["neg_comments_count"] = list(map(int, tmp_neg_comments_count))

            tmp_neg_comments_day = row["neg_comments_day"]
            tmp_neg_comments_day = tmp_neg_comments_day.replace("[", "").replace("]", "").replace('"', "")
            tmp_neg_comments_day = tmp_neg_comments_day.split(",")
            row["neg_comments_day"] = tmp_neg_comments_day

            tmp_pos_comments_count = row["pos_comments_count"].replace("[", "").replace("]", "")
            tmp_pos_comments_count = tmp_pos_comments_count.split(",")
            row["pos_comments_count"] = list(map(int, tmp_pos_comments_count))

            tmp_pos_comments_day = row["pos_comments_day"]
            tmp_pos_comments_day = tmp_pos_comments_day.replace("[", "").replace("]", "").replace('"', "")
            tmp_pos_comments_day = tmp_pos_comments_day.split(",")
            row["pos_comments_day"] = tmp_pos_comments_day

            tmp_und_comments_count = row["und_comments_count"].replace("[", "").replace("]", "")
            tmp_und_comments_count = tmp_und_comments_count.split(",")
            row["und_comments_count"] = list(map(int, tmp_und_comments_count))

            tmp_und_comments_day = row["und_comments_day"]
            tmp_und_comments_day = tmp_und_comments_day.replace("[", "").replace("]", "").replace('"', "")
            tmp_und_comments_day = tmp_und_comments_day.split(",")
            row["und_comments_day"] = tmp_und_comments_day

        else:
            row["neg_comments_day"] = []
            row["neg_comments_count"] = []
            row["pos_comments_day"] = []
            row["pos_comments_count"] = []
            row["und_comments_day"] = []
            row["und_comments_count"] = []
    return data[0]

# import pymysql.cursors
#
# connection = pymysql.connect(host='localhost',
#                              port=3307,
#                              user='root',
#                              password='kelab5136',
#                              database='tube_sage',
#                              cursorclass=pymysql.cursors.DictCursor)
#
# connection.ping(reconnect=True)
# conn = connection.cursor()
# data = get_comment_sentiment("사람사는세상노무현재단", connection, conn)
# print(data)
#
# df_neg = pd.DataFrame(columns=["comments_count", "comments_day"])
# df_neg["comments_count"] = data["neg_comments_count"]
# df_neg["comments_day"] = data["neg_comments_day"]
# print(df_neg)
# # for index, row in df_neg.iterrows():
# #     print(row["comments_count"], row["comments_day"])
