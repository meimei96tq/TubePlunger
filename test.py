import pymysql.cursors
import pandas as pd
import query_data
connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='kelab5136',
                             database='tube_sage',
                             cursorclass=pymysql.cursors.DictCursor)

connection.ping(reconnect=True)
conn = connection.cursor()


def get_list_users():
    left_users = []
    right_users = []

    connection.ping(reconnect=True)
    sql_ = "SELECT user_id FROM users WHERE political_bias = 1; "
    conn.execute(sql_)
    data = conn.fetchall()
    for user in data:
        left_users.append(user['user_id'])

    sql_ = "SELECT user_id FROM users WHERE political_bias = 2; "
    conn.execute(sql_)
    data = conn.fetchall()
    for user in data:
        right_users.append(user['user_id'])

    return left_users, right_users


# connection.ping(reconnect=True)
# sql_ = "SELECT leftwing_bias, rightwing_bias " \
#        "FROM channels " \
#        "WHERE channel_id = %s; "
# conn.execute(sql_, 'UC8d0ZgFEl4AUdyAKGE8O3yg')
# data = conn.fetchall()
# left = 0
# right = 0
# left += int(data[0]['leftwing_bias'])
# right += int(data[0]['rightwing_bias'])
# print(left, right)

# left_users, right_users = get_list_users()
# print(len(left_users))
# print(len(right_users))

# # file = open('files\\common.txt', 'r', encoding='utf8')
# # for i in file:
# #     user_id = i[:-1]
# #     print(user_id)
# #     connection.ping(reconnect=True)
# #     sql = "INSERT INTO users (id, user_id, political_bias) " \
# #           "VALUE (default, %s, %s); "
# #     conn.execute(sql, (user_id, 3))
# #     connection.commit()

# def get_data(channel_name):
#     connection.ping(reconnect=True)
#     sql = "SELECT DISTINCT comments.comment_id, comments.video_id, comments.comment_postdate " \
#           "FROM comments ; "
#     conn.execute(sql)
#     data = conn.fetchall()
#   return data


# data = get_data('사람사는세상노무현재단')
# print("done get data")
# df = pd.DataFrame(data)
# video_list = df.groupby(['video_id'])['video_id'].count().reset_index(name='count').sort_values(['count'],
#                                                                                                 ascending=False)
# print(video_list)

# for video in video_list['video_id']:
#     print(video)
#     df_by_channel_video = df[df["video_id"] == video]
#     x_data_date, y_data = query_data.get_distribution(df_by_channel_video, "day")
#     print(x_data_date)
#     print(y_data)
#     break
# print('test')
