from __future__ import annotations

from http import HTTPStatus

from flask import Blueprint, jsonify, request

from server.services.config_store import ConfigStore, ConfigValidationError


config_bp = Blueprint("config_bp", __name__)
store = ConfigStore()


def _json_error(message: str, status: HTTPStatus):
    response = jsonify({"ok": False, "error": message})
    response.status_code = status
    return response


@config_bp.get("/api/configs")
def list_configs():
    return jsonify(
        {
            "ok": True,
            "configs": [item.__dict__ for item in store.list_configs()],
            "tree": store.experiment_tree(),
            "default_config": store.default_config(),
            "metrics_schema": store.metrics_schema(),
        }
    )


@config_bp.post("/api/configs")
def create_config():
    body = request.get_json(silent=True) or {}
    name = body.get("name", "").strip()
    payload = body.get("config", {})
    try:
        record = store.create_config(name, payload)
    except FileExistsError:
        return _json_error("config name already exists", HTTPStatus.CONFLICT)
    except ConfigValidationError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
    return jsonify({"ok": True, "config": record})


@config_bp.get("/api/configs/<name>")
def get_config(name: str):
    try:
        return jsonify({"ok": True, "config": store.get_config(name)})
    except FileNotFoundError:
        return _json_error("config not found", HTTPStatus.NOT_FOUND)
    except ConfigValidationError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)


@config_bp.put("/api/configs/<name>")
def update_config(name: str):
    body = request.get_json(silent=True) or {}
    try:
        record = store.update_config(name, body.get("config", {}))
    except FileNotFoundError:
        return _json_error("config not found", HTTPStatus.NOT_FOUND)
    except ConfigValidationError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
    return jsonify({"ok": True, "config": record})


@config_bp.post("/api/configs/<name>/rename")
def rename_config(name: str):
    body = request.get_json(silent=True) or {}
    new_name = str(body.get("new_name", "")).strip()
    try:
        store.rename_config(name, new_name)
    except FileNotFoundError:
        return _json_error("config not found", HTTPStatus.NOT_FOUND)
    except FileExistsError:
        return _json_error("new config name already exists", HTTPStatus.CONFLICT)
    except ConfigValidationError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)
    return jsonify({"ok": True, "old_name": name, "new_name": new_name})


@config_bp.delete("/api/configs/<name>")
def delete_config(name: str):
    force = str(request.args.get("force", "")).strip().lower() in {"1", "true", "yes"}
    try:
        store.delete_config(name, force=force)
    except FileNotFoundError:
        return _json_error("config not found", HTTPStatus.NOT_FOUND)
    except ConfigValidationError as exc:
        return _json_error(str(exc), HTTPStatus.CONFLICT)
    return jsonify({"ok": True})
