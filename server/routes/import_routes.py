from __future__ import annotations

import json
from http import HTTPStatus

from flask import Blueprint, jsonify, request

from server.services.import_service import ImportService, ImportValidationError
from server.services.run_store import RunStore


import_bp = Blueprint("import_bp", __name__)
import_store = ImportService()
run_store = RunStore()


def _json_error(message: str, status: HTTPStatus):
    response = jsonify({"ok": False, "error": message})
    response.status_code = status
    return response


@import_bp.post("/api/imports/experiment")
def import_experiment():
    manifest_raw = str(request.form.get("manifest", "")).strip()
    if not manifest_raw:
        return _json_error("manifest is required", HTTPStatus.BAD_REQUEST)
    try:
        manifest = json.loads(manifest_raw)
    except json.JSONDecodeError:
        return _json_error("manifest must be valid JSON", HTTPStatus.BAD_REQUEST)
    if not isinstance(manifest, list):
        return _json_error("manifest must be a JSON array", HTTPStatus.BAD_REQUEST)

    files = request.files.getlist("files")
    try:
        imported = import_store.import_experiment(manifest, files)
        run = run_store.get_run_detail(imported["config_name"], imported["run_id"])
    except ImportValidationError as exc:
        return _json_error(str(exc), HTTPStatus.BAD_REQUEST)

    return jsonify(
        {
            "ok": True,
            "config_name": imported["config_name"],
            "run_id": imported["run_id"],
            "source_dir_name": imported["source_dir_name"],
            "run": run,
            "summary": imported["summary"],
        }
    )
