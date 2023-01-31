import threading
from TubeSage import query_data
from pororo import Pororo
from TubeSage.utils import preprocessing
import requests

sa = Pororo(task='sentiment', model='brainbert.base.ko.nsmc', lang='ko')
api_key = ['AIzaSyAU5oG0wsWP6Jh6RO5OBtnQoKEdl9EjAtQ',
           'AIzaSyDl3UJq55mpxqf22lGRqyNgZS1C_rZBcAQ',
           'AIzaSyDzLvh6QTNqGpliQemb_IcBWlDAuYQm9EE',
           'AIzaSyAVhsgNZSTnvnzOz5t8YrQ5PS5oHXXQb7c',
           'AIzaSyBarBcMLhdWEIgxxThb02SNe_AR54kFmok',
           'AIzaSyBXsnyTuGA5Xm9Q3PrBA4yD5ow2AHUec5U',
           'AIzaSyD7OW-eq84Au97MTf5Ukjf45nuy2zj8zSo',
           'AIzaSyD5TnWLVjVoGOAif1wvvGdjCWRKjV3GiXg',
           'AIzaSyAgqC_yTUqbJER-swokoqfDlcWrD7V5VHo',
           'AIzaSyDjZUwWKwkq9awxo7GT9LCzRgio-jBVHGU',
           'AIzaSyDZSkkX-PE6Exyv734mSTbjhHCaGB84zGE']


class Crawl_data(threading.Thread):
    def __init__(self, channel_id, connection, conn):
        threading.Thread.__init__(self)
        self.channel_id = channel_id
        self.connection = connection
        self.conn = conn

    def run(self):
        print("Starting crawl " + self.channel_id)
        crawl_data(self.channel_id, self.connection, self.conn)


def get_list_users(connection, conn):
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


def update_bias_channel_temp(users, channel_id, connection, conn):
    left_users, right_users = get_list_users(connection, conn)
    print('Got the users lists')
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
    print('Got old bias: ', data[0]['leftwing_bias'], data[0]['rightwing_bias'])
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
    print('Updated')


def insert_videos(search_list, connection, conn, key, index_key):
    for item in search_list['items']:
        kind = item['id']['kind']
        if kind == 'youtube#video':
            channel_title = item['snippet']['channelTitle']
            channel_id = item['snippet']['channelId']
            video_id = item['id']['videoId']
            video_title = item['snippet']['title']
            video_date_published = str(item['snippet']['publishedAt'])[:-9]
            first_iterator_c = 1
            hashtags = ''
            url = ''
            while first_iterator_c == 1:
                first_iterator_c = 0
                parameters_c = {'part': 'snippet,contentDetails,statistics', 'id': video_id,
                                'key': key}
                r_c = requests.get('https://www.googleapis.com/youtube/v3/videos',
                                   params=parameters_c)
                search_list_c = r_c.json()
                if 'error' in search_list_c:
                    print('error')
                    key = api_key[index_key % len(api_key)]
                    index_key += 1
                    first_iterator_c = 1
                else:
                    for i in search_list_c['items']:
                        mark = False
                        for j in i['snippet']:
                            if j == 'thumbnails':
                                url = item['snippet']['thumbnails']['high']['url']
                            if j == 'tags':
                                mark = True
                                break
                        if mark:
                            tags_array = i['snippet']['tags']
                            for j in tags_array:
                                hashtags += j + ', '
                            hashtags = hashtags[:-2]

            connection.ping(reconnect=True)
            sql = "INSERT INTO videos (id, video_id, channel_id, video_title, video_date_published, " \
                  "channel_title, thumbnail, hashtags) " \
                  "VALUE (default, %s, %s, %s, %s, %s, %s, %s); "
            conn.execute(sql, (video_id, channel_id, video_title, video_date_published, channel_title,
                               url, hashtags))
            connection.commit()
            insert_comments(video_id, channel_id, connection, conn, key, index_key)


def insert_comments(video_id, channel_id, connection, conn, key, index_key):
    first_iterator = 1
    next_page_token = ''
    error_count = 0
    users = []
    while first_iterator == 1 or next_page_token != '':
        first_iterator = 0
        parameters = {'part': 'id,snippet', 'videoId': video_id, 'maxResults': '100', 'pageToken': next_page_token,
                      'key': key}
        r = requests.get('https://www.googleapis.com/youtube/v3/commentThreads', params=parameters)
        comment_threads = r.json()
        if 'error' in comment_threads:
            error_catcher = comment_threads['error']['message']
            if str(error_catcher).startswith('The video identified by the'):
                print('CommentsDisabled!')
                break
            else:
                key = api_key[int(index_key % len(api_key))]
                index_key += 1
                if next_page_token == '':
                    first_iterator = 1
                error_count += 1
                if error_count == 50:
                    print('dailyLimitExceeded')
                    break
        else:
            error_count = 0
            if 'nextPageToken' in comment_threads:
                next_page_token = comment_threads['nextPageToken']
            else:
                next_page_token = ''
            for item in comment_threads['items']:
                top_level_comment = item['snippet']['topLevelComment']
                comment_postdate = top_level_comment['snippet']['publishedAt']
                reply_count = item['snippet']['totalReplyCount']
                comment_id = item['id']
                user_id = top_level_comment['snippet']['authorDisplayName']
                comment_content = top_level_comment['snippet']['textDisplay']
                like_count = top_level_comment['snippet']['likeCount']

                cmt = preprocessing.preprocessing(comment_content)
                sent = 'Undefined'
                if len(cmt) < 512:
                    sent = str(sa(cmt))

                sql = "INSERT INTO comments (id, comment_id, video_id, user_id, comment_content, comment_postdate, " \
                      "reply_count, like_count, tag, sentiment) " \
                      "VALUES (default, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

                values = (comment_id, video_id, user_id, comment_content, comment_postdate, reply_count,
                          like_count, 'undefined', sent)
                conn.execute(sql, values)
                connection.commit()
                users.append(user_id)

                if reply_count > 0:
                    continue_running = True
                    while continue_running:
                        parameters = {'part': 'id,snippet', 'parentId': comment_id, 'maxResults': '100',
                                      'key': key}
                        r = requests.get('https://www.googleapis.com/youtube/v3/comments', params=parameters)
                        reply_items = r.json()
                        reply_for = comment_id
                        if 'error' in reply_items:
                            key = api_key[int(index_key % len(api_key))]
                            index_key += 1
                        else:
                            continue_running = False
                            for reply_items in reversed(reply_items['items']):
                                comment_id = reply_items['id']
                                user_id = reply_items['snippet']['authorDisplayName']
                                comment_content = reply_items['snippet']['textDisplay']
                                comment_postdate = reply_items['snippet']['publishedAt']
                                like_count = reply_items['snippet']['likeCount']

                                cmt = preprocessing.preprocessing(comment_content)
                                sent = 'Undefined'
                                if len(cmt) < 512:
                                    sent = str(sa(cmt))

                                sql_ = "INSERT INTO comments (id, comment_id, video_id, user_id, comment_content, " \
                                       "comment_postdate, reply_id, like_count, tag, sentiment) " \
                                       "VALUES (default, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

                                values = (comment_id, video_id, user_id, comment_content, comment_postdate,
                                          reply_for, like_count, 'undefined', sent)
                                conn.execute(sql_, values)
                                connection.commit()
                                users.append(user_id)
    update_bias_channel_temp(users, channel_id, connection, conn)


def crawl_data(channel_id, connection, conn):
    index_key = 1
    key = api_key[0]
    next_page_token = ''
    first_iterator = 1
    while first_iterator == 1 or next_page_token != '':
        first_iterator = 0
        parameters = {'part': 'id, snippet', 'channelId': channel_id, 'maxResults': '50', 'order': 'date',
                      'pageToken': next_page_token, 'key': key}
        r = requests.get('https://www.googleapis.com/youtube/v3/search', params=parameters)
        search_list = r.json()
        if 'error' in search_list:
            key = api_key[index_key % len(api_key)]
            index_key += 1
            if next_page_token == '':
                first_iterator = 1

        else:
            if 'nextPageToken' in search_list:
                next_page_token = search_list['nextPageToken']
            else:
                next_page_token = ''

            insert_videos(search_list, connection, conn, key, index_key)

    connection.ping(reconnect=True)
    sql_ = "SELECT leftwing_bias, rightwing_bias " \
           "FROM channels " \
           "WHERE channel_id = %s; "
    conn.execute(sql_, channel_id)
    data = conn.fetchall()
    left_score = int(float(data[0]['leftwing_bias']) / float(data[0]['rightwing_bias']))
    right_score = 100 - left_score
    sql_ = "Update channels " \
           "SET leftwing_bias = %s, rightwing_bias= %s " \
           "where channel_id = %s; "
    conn.execute(sql_, (left_score, right_score, channel_id))
    connection.commit()
    return True


# def stop(self):
#     if query_data.crawl_data(self.channel_id, self.connection, self.conn):
#         print("stop crawl")
#         self.connection.ping(reconnect=True)
#         sql = "update channels set status = 'collected' where channel_id = %s; "
#         self.conn.execute(sql, self.channel_id)
#         self.connection.commit()
#     #     con phan xu ly khi an vao button stop
#     return True

