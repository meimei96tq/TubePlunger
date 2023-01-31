from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

db = SQLAlchemy()


def create_app():
    app_main = Flask(__name__)
    app_main.config['SECRET_KEY'] = 'secret-key-goes-here'  # it is used by Flask and extensions to keep data safe
    app_main.config[
        'SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:kelab5136@localhost:3307/tube_sage'  # it is the path where the SQLite database file will be saved
    app_main.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # deactivate Flask-SQLAlchemy track modifications
    db.init_app(app_main)

    # The login manager contains the code that lets your application and Flask-Login work together
    login_manager = LoginManager()  # Create a Login Manager instance
    login_manager.login_view = 'auth.login'  # define the redirection path when login required and we attempt to access without being logged in
    login_manager.init_app(app_main)  # configure it for login
    from TubeSage.models import User
    @login_manager.user_loader
    def load_user(user_id):  # reload user object from the user ID stored in the session
        # since the user_id is just the primary key of our user table, use it in the query for the user
        return User.query.get(int(user_id))

    # blueprint for auth routes in our app
    # blueprint allow you to orgnize your flask app
    from TubeSage.auth import auth as auth_blueprint
    app_main.register_blueprint(auth_blueprint)
    # blueprint for non-auth parts of app
    from TubeSage.main import main as main_blueprint
    app_main.register_blueprint(main_blueprint)
    return app_main
