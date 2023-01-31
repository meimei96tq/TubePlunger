from flask_login import UserMixin
from TubeSage.__init__ import db
from sqlalchemy.dialects.mysql import JSON
from datetime import datetime


class User(UserMixin, db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(100), nullable=False)


class Channels(db.Model):
    __tablename__ = 'channels'
    id = db.Column(db.Integer, primary_key=True)
    channel_id = db.Column(db.String(100), unique=True, nullable=False)
    channel_title = db.Column(db.String(100), nullable=False)
    channel_img = db.Column(db.String(100))
    status = db.Column(db.String(100))
    leftwing_bias = db.Column(db.String(100))
    rightwing_bias = db.Column(db.String(100))
    political_bias = db.Column(db.String(100), nullable=False)
    videos_count = db.Column(db.String(100))
    users_count = db.Column(db.String(100))
    comments_count = db.Column(db.String(100))
    neg_comments_count = db.Column(db.String(100))
    neg_comments_day = db.Column(db.String(100))
    pos_comments_count = db.Column(db.String(100))
    pos_comments_day = db.Column(db.String(100))
    und_comments_count = db.Column(db.String(100))
    und_comments_day = db.Column(db.String(100))
    user_FK = db.Column(db.Integer, nullable=False)

