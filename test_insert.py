import pymysql.cursors

connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='kelab5136',
                             database='tube_sage',
                             cursorclass=pymysql.cursors.DictCursor)

connection.ping(reconnect=True)
conn = connection.cursor()
channel_id = 'UCOqCunaF9qVN8bXwsK0HT3g'


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


# left_users, right_users = get_list_users()
# print(len(left_users))
# print(len(right_users))


def update_bias_channel_temp(users, channel_id, connection, conn):
    left_users, right_users = get_list_users(connection, conn)
    print('Got the users lists')
    print(len(left_users))
    print(len(right_users))
    left_score = 0
    right_score = 0
    for user in users:
        if user in left_users:
            left_score += 1
        elif user in right_users:
            right_score += 1

    connection.ping(reconnect=True)
    sql_ = "SELECT leftwing_bias, rightwing_bias " \
           "FROM channels " \
           "WHERE channel_id = %s; "
    conn.execute(sql_, channel_id)
    data = conn.fetchall()
    # print('Check data: ', len(data))
    if len(data) != 0:
        # print('Check data: ', type(data[0]['leftwing_bias']))
        # if str(type(data[0]['leftwing_bias'])) != "<class 'NoneType'>":
        left_score += int(data[0]['leftwing_bias'])
        right_score += int(data[0]['rightwing_bias'])

    sql_ = "Update channels " \
           "SET leftwing_bias = %s, rightwing_bias= %s " \
           "where channel_id = %s; "
    conn.execute(sql_, (left_score, right_score, channel_id))
    connection.commit()


# channel_id = 'UCfmicRK2-WdZMVQDrfcitLA'
# sql_ = "SELECT id FROM channels WHERE channel_id = %s; "
# conn.execute(sql_, channel_id)
# channel_FK = conn.fetchone()['id']
# print(channel_FK)
