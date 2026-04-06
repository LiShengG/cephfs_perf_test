from __future__ import annotations

from flask import Flask, render_template

from server.routes.config_routes import config_bp
from server.routes.import_routes import import_bp
from server.routes.metrics_routes import metrics_bp
from server.routes.run_routes import run_bp
from server.utils.paths import REPO_ROOT, read_app_config


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str((REPO_ROOT / "server" / "templates").resolve()),
        static_folder=str((REPO_ROOT / "server" / "static").resolve()),
    )
    app.register_blueprint(config_bp)
    app.register_blueprint(import_bp)
    app.register_blueprint(metrics_bp)
    app.register_blueprint(run_bp)

    @app.get("/")
    def index():
        return render_template("index.html")

    return app


if __name__ == "__main__":
    config = read_app_config()
    app = create_app()
    app.run(host=config["host"], port=config["port"], debug=False)
