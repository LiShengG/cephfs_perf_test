from __future__ import annotations

from http import HTTPStatus

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import RequestEntityTooLarge

from server.routes.config_routes import config_bp
from server.routes.import_routes import import_bp
from server.routes.metrics_routes import metrics_bp
from server.routes.run_routes import run_bp
from server.utils.paths import REPO_ROOT, read_app_config


def create_app() -> Flask:
    config = read_app_config()
    app = Flask(
        __name__,
        template_folder=str((REPO_ROOT / "server" / "templates").resolve()),
        static_folder=str((REPO_ROOT / "server" / "static").resolve()),
    )
    max_upload_mb = int(config.get("max_upload_mb", 256))
    app.config["MAX_CONTENT_LENGTH"] = max_upload_mb * 1024 * 1024
    app.register_blueprint(config_bp)
    app.register_blueprint(import_bp)
    app.register_blueprint(metrics_bp)
    app.register_blueprint(run_bp)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.errorhandler(RequestEntityTooLarge)
    def handle_file_too_large(_error: RequestEntityTooLarge):
        message = f"uploaded payload is too large; max upload size is {max_upload_mb} MB"
        if request.path.startswith("/api/"):
            response = jsonify({"ok": False, "error": message})
            response.status_code = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
            return response
        return message, HTTPStatus.REQUEST_ENTITY_TOO_LARGE

    return app


if __name__ == "__main__":
    app = create_app()
    config = read_app_config()
    app.run(host=config["host"], port=config["port"], debug=False)
