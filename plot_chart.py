import pandas as pd
import json
import plotly
import plotly.graph_objects as go
from TubeSage import query_data



def plot_comment_video(data):
    df = pd.DataFrame(data)
    fig = go.Figure()
    fig.update_layout(xaxis=dict(
        title='Time',
        type='date'
    ),
        yaxis=dict(
            title='#Comments'
        ),
        title='Number of comments over day',
        legend_title="Video:")
    for index, row in df.iterrows():
        fig.add_trace(go.Scatter(x=row["comments_day"], y=row["comments_distribution_day"],
                                 mode='lines+markers',
                                 name=row["video_title"][0:15]+'...'))
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON


def plot_sentiment(data):
    fig = go.Figure()
    fig.update_layout(xaxis=dict(
        title='Time',
        type='date'
    ),
        yaxis=dict(
            title='#Comments'
        ),
        title='Sentiment distribution in comments',
        legend_title="Sentiment:")
    fig.add_trace(go.Scatter(x=data["pos_comments_day"], y=data["pos_comments_count"],
                             mode='lines+markers',
                             line=dict(color="#1cc88a"),
                             name="Positive"))

    fig.add_trace(go.Scatter(x=data["neg_comments_day"], y=data["neg_comments_count"],
                             mode='lines+markers',
                             line=dict(color="#e74a3b"),
                             name="Negative"))

    fig.add_trace(go.Scatter(x=data["und_comments_day"], y=data["und_comments_count"],
                             mode='lines+markers',
                             line=dict(color="#f6c23e"),
                             name="Undefined"))


    # fig.show()
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON


def plot_comment_channel(data):
    df = pd.DataFrame(data, columns=["video_id", "video_title", "comment_content", "comment_postdate", "sentiment",
                                     "channel_title"])
    channel_list = df.groupby(['channel_title'])['channel_title'].count().reset_index(name='count').sort_values(
        ['count'],
        ascending=False)

    fig = go.Figure()
    fig.update_layout(xaxis=dict(
        title='Time',
        type='date'
    ),
        yaxis=dict(
            title='#Comments'
        ),
        autosize=False,
        width=1589,
        height=450,
        title='Distribution of comments by channels',
        legend_title="Channel:")
    for channel in channel_list['channel_title']:
        df_by_channel = df[df["channel_title"] == channel]
        x_data_date, y_data = query_data.get_distribution(df_by_channel, "day")
        fig.add_trace(go.Scatter(x=x_data_date, y=y_data,
                                 mode='lines+markers',
                                 name=channel))
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON
