from flask import Blueprint, render_template, request
from flask_login import login_required, current_user

from TubeSage.__init__ import create_app, db

from TubeSage import query_data
from TubeSage import plot_chart
import pymysql.cursors
from TubeSage.utils import update_data
from TubeSage.crawl_data_listen import Crawl_data
from word2word import methods

main = Blueprint('main', __name__)


connection = pymysql.connect(host='localhost',
                             port=3307,
                             user='root',
                             password='kelab5136',
                             database='tube_sage',
                             cursorclass=pymysql.cursors.DictCursor)

connection.ping(reconnect=True)
conn = connection.cursor()
sql = "SELECT * FROM channels WHERE political_bias = 3; "
conn.execute(sql)
collected_channels = conn.fetchall()

main_channels = [
    "사람사는세상노무현재단",
    "김용민tv",
    "tbs 시민의방송",
    "신의한수",
    "펜앤드마이크tv",
    "tv홍카콜라",
]
left_wing_channels = [
    "사람사는세상노무현재단",
    "김용민tv",
    "tbs 시민의방송"]
right_wing_channels = [
    "신의한수",
    "펜앤드마이크tv",
    "tv홍카콜라",
]

# app = Flask(__name__)


@main.route('/')
def home():
    connection.ping(reconnect=True)
    sql = "SELECT * FROM channels WHERE political_bias = 3; "
    conn.execute(sql)
    collected_channels = conn.fetchall()
    # collected_channels = []
    return render_template('home.html', collected_channels=collected_channels)


@main.route('/noti_for_signup')
def noti_for_signup():
    return render_template('noti_for_signup.html', collected_channels=collected_channels)


@main.route('/channel_statistics', methods=['POST'])
@login_required
def channel_statistics():
    print("check post")
    sql = "SELECT * FROM channels WHERE political_bias = 3; "
    conn.execute(sql)
    collected_channels = conn.fetchall()
    return render_template('channel_statistics.html', collected_channels=collected_channels)


@main.route('/channel_statistics', methods=['GET'])
@login_required
def channel():
    # try:
        channel_name = request.args["channel_name"]
        print("check get", channel_name)
        # data = query_data.get_data(channel_name, connection, conn)
        top_user = query_data.get_top_user(channel_name, connection, conn)
        comment_video = query_data.get_comment_video(channel_name, connection, conn)
        comment_sentiment = query_data.get_comment_sentiment(channel_name, connection, conn)

        plot_div_comment_video = plot_chart.plot_comment_video(comment_video)
        plot_div_sentiment = plot_chart.plot_sentiment(comment_sentiment)

        channel_info = query_data.get_channels(channel_name, connection, conn)[0]
        channel_detail = query_data.get_channel_info(channel_name, connection, conn)


        sql = "SELECT * FROM channels WHERE political_bias = 3; "
        conn.execute(sql)
        collected_channels = conn.fetchall()
        return render_template('channel_statistics.html', channel_info=channel_info, top_user=top_user,
                               plot_div_comment_video=plot_div_comment_video,
                               plot_div_sentiment=plot_div_sentiment, channel_detail=channel_detail,
                               collected_channels=collected_channels)
    # except:
    #     return render_template('error.html', collected_channels=collected_channels)


@main.route('/user_tracking', methods=['POST', 'GET'])
@login_required
def user_tracking():
    if request.method == 'POST':
        sql = "SELECT * FROM channels WHERE political_bias = 3; "
        conn.execute(sql)
        collected_channels = conn.fetchall()
        user_id = request.form['input']
        print(user_id)
        comment_list = query_data.get_comment_by_user(user_id, connection, conn)
        left_wing_comments = []
        right_wing_comments = []
        left_pos = 0
        left_neg = 0
        left_und = 0
        right_pos = 0
        right_neg = 0
        right_und = 0
        for index, row in comment_list.iterrows():
            if row["channel_title"].lower() in left_wing_channels:
                left_wing_comments.append({
                    "video_id": row["video_id"],
                    "video_title": row["video_title"],
                    "comment_content": row["comment_content"],
                    "comment_postdate": row["comment_postdate"],
                    "sentiment": row["sentiment"],
                    "channel_title": row["channel_title"],
                })
                if row["sentiment"] == "Positive":
                    left_pos += 1
                elif row["sentiment"] == "Negative":
                    left_neg += 1
                else:
                    left_und += 1
            else:
                right_wing_comments.append({
                    "video_id": row["video_id"],
                    "video_title": row["video_title"],
                    "comment_content": row["comment_content"],
                    "comment_postdate": row["comment_postdate"],
                    "sentiment": row["sentiment"],
                    "channel_title": row["channel_title"],
                })
                if row["sentiment"] == "Positive":
                    right_pos += 1
                elif row["sentiment"] == "Negative":
                    right_neg += 1
                else:
                    right_und += 1
        left_sum = left_pos + left_neg + left_und
        right_sum = right_pos + right_neg + right_und
        left_count = [left_pos, left_neg, left_und, left_sum]
        right_count = [right_pos, right_neg, right_und, right_sum]

        plot_left_channel = plot_chart.plot_comment_channel(left_wing_comments)
        plot_right_channel = plot_chart.plot_comment_channel(right_wing_comments)
        return render_template("user_tracking.html", user_id=user_id, left_wing_comments=left_wing_comments,
                               right_wing_comments=right_wing_comments, left_count=left_count, right_count=right_count,
                               plot_left_channel=plot_left_channel, plot_right_channel=plot_right_channel,
                               collected_channels=collected_channels)

    if request.method == 'GET':
        sql = "SELECT * FROM channels WHERE political_bias = 3; "
        conn.execute(sql)
        collected_channels = conn.fetchall()
        try:
            user_id = request.args["user_id"]
            print(user_id)
            comment_list = query_data.get_comment_by_user(user_id, connection, conn)
            left_wing_comments = []
            right_wing_comments = []
            left_pos = 0
            left_neg = 0
            left_und = 0
            right_pos = 0
            right_neg = 0
            right_und = 0
            for index, row in comment_list.iterrows():
                if row["channel_title"].lower() in left_wing_channels:
                    left_wing_comments.append({
                        "video_id": row["video_id"],
                        "video_title": row["video_title"],
                        "comment_content": row["comment_content"],
                        "comment_postdate": row["comment_postdate"],
                        "sentiment": row["sentiment"],
                        "channel_title": row["channel_title"],
                    })
                    if row["sentiment"] == "Positive":
                        left_pos += 1
                    elif row["sentiment"] == "Negative":
                        left_neg += 1
                    else:
                        left_und += 1
                else:
                    right_wing_comments.append({
                        "video_id": row["video_id"],
                        "video_title": row["video_title"],
                        "comment_content": row["comment_content"],
                        "comment_postdate": row["comment_postdate"],
                        "sentiment": row["sentiment"],
                        "channel_title": row["channel_title"],
                    })
                    if row["sentiment"] == "Positive":
                        right_pos += 1
                    elif row["sentiment"] == "Negative":
                        right_neg += 1
                    else:
                        right_und += 1
            left_sum = left_pos + left_neg + left_und
            right_sum = right_pos + right_neg + right_und
            left_count = [left_pos, left_neg, left_und, left_sum]
            right_count = [right_pos, right_neg, right_und, right_sum]
            plot_left_channel = plot_chart.plot_comment_channel(left_wing_comments)
            plot_right_channel = plot_chart.plot_comment_channel(right_wing_comments)
            return render_template("user_tracking.html", user_id=user_id, left_wing_comments=left_wing_comments,
                                   right_wing_comments=right_wing_comments, left_count=left_count,
                                   right_count=right_count,
                                   plot_left_channel=plot_left_channel, plot_right_channel=plot_right_channel,
                                   collected_channels=collected_channels)
        except:
            print("check except")
            return render_template("user_tracking.html", user_id="", collected_channels=collected_channels)


@main.route('/social_event_analysis', methods=['POST'])
@login_required
def social_event():
    try:
        return render_template('social_event_analysis.html', collected_channels=collected_channels)
    except:
        return render_template('error.html', collected_channels=collected_channels)


@main.route('/social_event_analysis', methods=['GET'])
@login_required
def social_event_analysis():
    sql = "SELECT * FROM channels WHERE political_bias = 3; "
    conn.execute(sql)
    collected_channels = conn.fetchall()
    try:

        return render_template("social_event_analysis.html", collected_channels=collected_channels)
    except:
        return render_template('error.html', collected_channels=collected_channels)


@main.route('/collect_data', methods=['POST', 'GET'])
@login_required
def collect_data():
    if request.method == 'POST':
        channel_id = request.form['input']
        print(channel_id)
        flag = query_data.insert_channel(channel_id, connection, conn)
        sql = "SELECT * FROM channels WHERE political_bias = 3; "
        conn.execute(sql)
        collected_channels = conn.fetchall()

        if flag == "Inserted":
            channels = query_data.get_channels("", connection, conn)
            streamListener = Crawl_data(channel_id, connection, conn)
            streamListener.start()
            # sql = "SELECT * FROM channels WHERE political_bias = 3; "
            # conn.execute(sql)
            # collected_channels = conn.fetchall()
            return render_template("collect_data.html", flag=flag, channels=channels, collected_channels=collected_channels,
                                   main_channels=main_channels)
        channels = query_data.get_channels("", connection, conn)

        for channel in channels:
            if channel['videos_count'] == None:
                print(channel['channel_id'], 'POST')
                channel['videos_count'] = update_data.get_videos_count(channel['channel_id'])
                channel['comments_count'] = update_data.get_comments_count(channel['channel_id'])
                channel['users_count'] = update_data.get_users_count(channel['channel_id'])
        return render_template("collect_data.html", flag=flag, channels=channels, collected_channels=collected_channels,
                           main_channels=main_channels)

    if request.method == 'GET':
        sql = "SELECT * FROM channels WHERE political_bias = 3; "
        conn.execute(sql)
        collected_channels = conn.fetchall()
        # Doc data tu db
        channels = query_data.get_channels("", connection, conn)

        for channel in channels:
            if channel['videos_count'] == None:
                print('GET', channel['channel_id'])
                channel['videos_count'] = update_data.get_videos_count(channel['channel_id'])
                channel['comments_count'] = update_data.get_comments_count(channel['channel_id'])
                channel['users_count'] = update_data.get_users_count(channel['channel_id'])
        return render_template("collect_data.html", channels=channels, collected_channels=collected_channels,
                           main_channels=main_channels)


app = create_app()  # we initialize our flask app using the __init__.py function
if __name__ == '__main__':
    db.create_all(app=create_app())  # create the SQLite database
    app.run(debug=True)


# @main.route('/collect_data', methods=['GET'])
# @login_required
# def collect_data():
#     sql = "SELECT * FROM channels WHERE political_bias = 3; "
#     conn.execute(sql)
#     collected_channels = conn.fetchall()
#     # Doc data tu db
#     channels = query_data.get_channels("", connection, conn)
#     # print(channels)
#
#     for channel in channels:
#         if channel['videos_count'] == None:
#             channel['videos_count'] = update_data.get_videos_count(channel['channel_id'])
#             channel['comments_count'] = update_data.get_comments_count(channel['channel_id'])
#             channel['users_count'] = update_data.get_users_count(channel['channel_id'])
#     return render_template("collect_data.html", channels=channels, collected_channels=collected_channels,
#                            main_channels=main_channels)


# @main.route('/user_tracking', methods=['GET'])
# @login_required
# def user_tracking():
#     sql = "SELECT * FROM channels WHERE political_bias = 3; "
#     conn.execute(sql)
#     collected_channels = conn.fetchall()
#     try:
#         user_id = request.args["user_id"]
#         print(user_id)
#         comment_list = query_data.get_comment_by_user(user_id, connection, conn)
#         left_wing_comments = []
#         right_wing_comments = []
#         left_pos = 0
#         left_neg = 0
#         left_und = 0
#         right_pos = 0
#         right_neg = 0
#         right_und = 0
#         for index, row in comment_list.iterrows():
#             if row["channel_title"].lower() in left_wing_channels:
#                 left_wing_comments.append({
#                     "video_id": row["video_id"],
#                     "video_title": row["video_title"],
#                     "comment_content": row["comment_content"],
#                     "comment_postdate": row["comment_postdate"],
#                     "sentiment": row["sentiment"],
#                     "channel_title": row["channel_title"],
#                 })
#                 if row["sentiment"] == "Positive":
#                     left_pos += 1
#                 elif row["sentiment"] == "Negative":
#                     left_neg += 1
#                 else:
#                     left_und += 1
#             else:
#                 right_wing_comments.append({
#                     "video_id": row["video_id"],
#                     "video_title": row["video_title"],
#                     "comment_content": row["comment_content"],
#                     "comment_postdate": row["comment_postdate"],
#                     "sentiment": row["sentiment"],
#                     "channel_title": row["channel_title"],
#                 })
#                 if row["sentiment"] == "Positive":
#                     right_pos += 1
#                 elif row["sentiment"] == "Negative":
#                     right_neg += 1
#                 else:
#                     right_und += 1
#         left_sum = left_pos + left_neg + left_und
#         right_sum = right_pos + right_neg + right_und
#         left_count = [left_pos, left_neg, left_und, left_sum]
#         right_count = [right_pos, right_neg, right_und, right_sum]
#         plot_left_channel = plot_chart.plot_comment_channel(left_wing_comments)
#         plot_right_channel = plot_chart.plot_comment_channel(right_wing_comments)
#         return render_template("user_tracking.html", user_id=user_id, left_wing_comments=left_wing_comments,
#                                right_wing_comments=right_wing_comments, left_count=left_count, right_count=right_count,
#                                plot_left_channel=plot_left_channel, plot_right_channel=plot_right_channel,
#                                collected_channels=collected_channels)
#     except:
#         print("check except")
#         return render_template("user_tracking.html", user_id="", collected_channels=collected_channels)






