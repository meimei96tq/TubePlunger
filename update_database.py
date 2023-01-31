import pymysql.cursors
import query_data
import pandas as pd
from tqdm import tqdm
import json
from datetime import date

connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='kelab5136',
                             database='tube_sage',
                             cursorclass=pymysql.cursors.DictCursor)

connection.ping(reconnect=True)
print('Connected to DB')
conn = connection.cursor()
sql = "SELECT * FROM channels; "
conn.execute(sql)
channels = conn.fetchall()


# update bias of users

# update dashboard: videos_count, users_count, comments_count from channels
def get_videos_count(channel_id):
    print('get_videos_count')
    sql_ = "SELECT count(distinct video_id) " \
           "as videos_count " \
           "FROM videos " \
           "where channel_id = %s; "
    conn.execute(sql_, channel_id)
    videos_count = conn.fetchall()[0]['videos_count']
    return videos_count


def get_users_count(channel_id):
    print('get_users_count')
    sql_ = "select count(distinct user_id) " \
           "as users_count " \
           "from comments inner join videos " \
           "on comments.video_id = videos.video_id " \
           "where videos.channel_id = %s; "
    conn.execute(sql_, channel_id)
    users_count = conn.fetchall()[0]['users_count']
    return users_count


def get_comments_count(channel_id):
    print('get_comments_count')
    sql_ = "select count(distinct comment_id) " \
           "as comments_count " \
           "from comments inner join videos " \
           "on comments.video_id = videos.video_id " \
           "where videos.channel_id = %s; "
    conn.execute(sql_, channel_id)
    comments_count = conn.fetchall()[0]['comments_count']
    return comments_count


def update_dashboard():
    print("check update_dashboard")
    for channel in channels:
        channel_id = channel["channel_id"]
        sql_ = "Update channels " \
               "SET videos_count = %s, users_count= %s, comments_count = %s " \
               "where channel_id = %s; "
        conn.execute(sql_, (get_videos_count(channel_id), get_users_count(channel_id), get_comments_count(channel_id),
                            channel_id))
        connection.commit()


# update comments_count for videos
def update_comments_each_video():
    print("check get_comments_each_video")
    sql_ = "SELECT comments.comment_id, comments.video_id, comments.comment_postdate " \
           "FROM comments ; "
    conn.execute(sql_)
    data = conn.fetchall()
    df = pd.DataFrame(data)
    video_list = df.groupby(['video_id'])['video_id'].count().reset_index(name='count').sort_values(['count'],
                                                                                                    ascending=False)
    for video in tqdm(video_list['video_id']):
        df_by_channel_video = df[df["video_id"] == video]
        x_data_date, y_data = query_data.get_distribution(df_by_channel_video, "day")
        json_x_data_date = json.dumps(x_data_date)
        json_y_data = json.dumps(y_data)

        sql_ = "Update videos " \
               "SET comments_distribution_day = %s, comments_day= %s " \
               "where video_id = %s; "
        conn.execute(sql_, (json_y_data, json_x_data_date, video))
        connection.commit()

    return True


# update political_bias of channels
# update sentiment_comments_count for channels
def get_sentiment_comment_channel(df, sentiment):
    df_by_channel_sentiment = df[df["sentiment"] == sentiment]
    x_data_date, y_data = query_data.get_distribution(df_by_channel_sentiment, "day")
    return x_data_date, y_data


def update_sentiment():
    print("check update_sentiment")
    for channel in tqdm(channels):
        sql_ = "select comment_id, videos.video_id, comment_postdate, sentiment, channel_id " \
               "from comments inner join videos " \
               "on comments.video_id = videos.video_id " \
               "where channel_id = %s; "
        conn.execute(sql_, channel["channel_id"])
        data = conn.fetchall()
        if len(data) != 0:
            df = pd.DataFrame(data)
            x_data_date_pos, y_data_pos = get_sentiment_comment_channel(df, "Positive")
            json_x_data_date_pos = json.dumps(x_data_date_pos)
            json_y_data_pos = json.dumps(y_data_pos)
            print("done pos")
            x_data_date_neg, y_data_neg = get_sentiment_comment_channel(df, "Negative")
            json_x_data_date_neg = json.dumps(x_data_date_neg)
            json_y_data_neg = json.dumps(y_data_neg)
            print("done neg")
            x_data_date_und, y_data_und = get_sentiment_comment_channel(df, "Undefined")
            json_x_data_date_und = json.dumps(x_data_date_und)
            json_y_data_und = json.dumps(y_data_und)
            print("done und")
            sql_ = "Update channels " \
                   "SET neg_comments_count = %s, neg_comments_day = %s, " \
                   "    pos_comments_count = %s, pos_comments_day = %s, " \
                   "    und_comments_count = %s, und_comments_day = %s " \
                   "where channel_id = %s; "
            conn.execute(sql_, (json_y_data_neg, json_x_data_date_neg, json_y_data_pos, json_x_data_date_pos,
                                json_y_data_und, json_x_data_date_und, channel["channel_id"]))
            connection.commit()
    return True


def delete_top_users_table():
    connection.ping(reconnect=True)
    sql_ = "DELETE FROM top_users; "
    conn.execute(sql_)
    connection.commit()


def update_top_users():
    delete_top_users_table()
    print("check update_top_users")
    connection.ping(reconnect=True)

    for channel in tqdm(channels):
        sql_ = "SELECT comments.comment_id, comments.user_id, videos.channel_title " \
               "FROM comments INNER JOIN videos " \
               "ON comments.video_id = videos.video_id " \
               "WHERE videos.channel_title = %s; "
        conn.execute(sql_, channel['channel_title'])
        data = conn.fetchall()
        if len(data) != 0:
            df = pd.DataFrame(data)
            top_user = df.groupby(['user_id'])['user_id'].count().reset_index(name='comment_count').\
                sort_values(['comment_count'], ascending=False)
            top_users = top_user.head(20)

            for index, user in top_users.iterrows():
                sql_ = "INSERT INTO top_users VALUE (DEFAULT, %s, %s, %s); "
                conn.execute(sql_, (channel['channel_title'], user['user_id'], user['comment_count']))
                connection.commit()


def update_video_details():
    connection.ping(reconnect=True)

    for channel in tqdm(channels):
        sql_ = "SELECT comments.comment_id, comments.video_id, videos.video_title, " \
               "videos.channel_title, comments.sentiment " \
               "FROM comments INNER JOIN videos " \
               "ON comments.video_id = videos.video_id " \
               "WHERE videos.channel_title = %s AND videos.video_date_published LIKE %s; "
        conn.execute(sql_, (channel['channel_title'], '2022-%'))
        data = conn.fetchall()

        if len(data) != 0:
            df = pd.DataFrame(data)
            video_list = df.groupby(['video_id'])['video_id'].count().reset_index(name='count').\
                sort_values(['count'], ascending=False)
            for video in tqdm(video_list['video_id']):
                df_by_channel_video = df[df["video_id"] == video]
                pos_comments_count = df_by_channel_video[df_by_channel_video["sentiment"] == "Positive"].shape[0]
                neg_comments_count = df_by_channel_video[df_by_channel_video["sentiment"] == "Negative"].shape[0]
                und_comments_count = df_by_channel_video[df_by_channel_video["sentiment"] == "Undefined"].shape[0]
                comments_count = df_by_channel_video.shape[0]
                sql_ = "Update videos " \
                       "SET neg_comments_count = %s, pos_comments_count = %s, " \
                       "    und_comments_count = %s, comments_count = %s " \
                       "where video_id = %s; "
                conn.execute(sql_, (neg_comments_count, pos_comments_count, und_comments_count, comments_count, video))
                connection.commit()


# update_dashboard()
# print("done update_dashboard")
# update_comments_each_video()
# print("done update_comments_each_video")
# update_top_users()
# print("done update_top_users")
# update_sentiment()
# print("done update_sentiment")
# update_video_details()
# print("done update_video_details")
#
# f = open('log\Done updating database ' + str(date.today()), 'w')
# f.close()

