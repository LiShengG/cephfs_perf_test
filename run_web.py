from server.app import create_app
from server.utils.paths import read_app_config


config = read_app_config()
app = create_app()


if __name__ == "__main__":
    app.run(host=config["host"], port=config["port"], debug=False)
