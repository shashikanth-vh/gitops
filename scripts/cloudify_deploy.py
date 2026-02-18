#!/usr/bin/env python3
"""
cloudify_deploy.py - Generic Cloudify deploy runner (REST API only)

Features:
- Cloudify API version configurable (default: v3.1)
- Token auth via POST /tokens (supports Basic Auth header)
- Blueprint upload via ZIP archive (built from --blueprint-dir)
- Create deployment if missing
- Run workflow (install/update) and wait for completion
- Retries/backoff for transient failures
"""

import argparse
import base64
import json
import os
import sys
import time
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests
import yaml


# -------------------------
# Utilities
# -------------------------

class CloudifyAPIError(RuntimeError):
    pass


def log(msg: str) -> None:
    print(msg, flush=True)


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr, flush=True)
    sys.exit(code)


def str_to_bool(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "yes", "y", "on")


def load_yaml_file(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    if not os.path.exists(path):
        die(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        die(f"YAML top-level must be a mapping/object: {path}")
    return data


def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    out.update(override)
    return out


def build_zip_from_dir(src_dir: str, out_zip: str, root_dir_name: str) -> None:
    """
    Cloudify (some versions/configs) require the blueprint archive to contain exactly
    one top-level directory. This function zips src_dir into out_zip under root_dir_name/.
    """
    if not os.path.isdir(src_dir):
        die(f"Blueprint directory not found: {src_dir}")

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, src_dir)
                arcname = os.path.join(root_dir_name, rel)  # <- one top-level dir
                z.write(full, arcname)



# -------------------------
# Cloudify API client
# -------------------------

@dataclass
class CfyConfig:
    manager_url: str
    username: str
    password: str
    tenant: Optional[str]
    api_version: str          # e.g. "v3.1" or "v3"
    insecure: bool            # disable TLS verify
    request_timeout_sec: int  # per-request timeout
    exec_timeout_sec: int     # overall execution wait timeout
    poll_interval_sec: int    # poll interval


def api_url(cfg: CfyConfig, path: str) -> str:
    base = cfg.manager_url.rstrip("/")
    ver = cfg.api_version.strip("/")

    # allow user to pass "v3.1" or "api/v3.1"
    if ver.startswith("api/"):
        ver = ver[4:]

    return f"{base}/api/{ver}/{path.lstrip('/')}"


def _request(
    cfg: CfyConfig,
    method: str,
    url: str,
    token: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Any] = None,
    data_body: Optional[Any] = None,
    content_type: Optional[str] = None,
    retries: int = 5,
    backoff_sec: float = 1.0,
) -> Tuple[int, str, Dict[str, Any]]:
    """
    Returns: (status_code, raw_text, json_dict_or_empty)
    Retries on transient errors (5xx, timeouts, connection issues).
    """
    h = dict(headers or {})

    # Always include tenant if provided (Cloudify may require it for authorization)
    if cfg.tenant and "Tenant" not in h:
        h["Tenant"] = cfg.tenant

    if token:
        h["Authentication-Token"] = token

    if content_type:
        h["Content-Type"] = content_type

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=h,
                params=params,
                json=json_body,
                data=data_body,
                verify=not cfg.insecure,
                timeout=cfg.request_timeout_sec,
            )
            text = resp.text or ""
            parsed: Dict[str, Any] = {}
            if text.strip():
                try:
                    parsed = resp.json()
                except ValueError:
                    parsed = {}
            # Retry on 5xx
            if resp.status_code >= 500 and attempt < retries:
                log(f"HTTP {resp.status_code} (attempt {attempt}/{retries}) for {method} {url} — retrying...")
                time.sleep(backoff_sec * attempt)
                continue
            return resp.status_code, text, parsed
        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as e:
            last_exc = e
            if attempt >= retries:
                break
            log(f"Request error (attempt {attempt}/{retries}) for {method} {url}: {e} — retrying...")
            time.sleep(backoff_sec * attempt)

    raise CloudifyAPIError(f"Request failed after retries: {method} {url}: {last_exc}")


def login_get_token(cfg: CfyConfig) -> str:
    """
    POST /tokens. Your curl works with:
    - Authorization: Basic base64(user:pass)
    - body: {"username": "...", "password": "..."}
    We'll do the same.
    """
    url = api_url(cfg, "tokens")
    basic = base64.b64encode(f"{cfg.username}:{cfg.password}".encode("utf-8")).decode("utf-8")

    payload: Dict[str, Any] = {"username": cfg.username, "password": cfg.password}
    if cfg.tenant:
        payload["tenant_name"] = cfg.tenant

    log(f"Authenticating to Cloudify Manager ({cfg.api_version})...")
    status, text, data = _request(
        cfg,
        "POST",
        url,
        headers={
            "Authorization": f"Basic {basic}",
        },
        json_body=payload,
        content_type="application/json",
    )

    if status >= 400:
        raise CloudifyAPIError(f"HTTP {status} for POST {url}: {text}")

    token = data.get("value")
    if not token:
        # Some environments might return raw text or different fields
        raise CloudifyAPIError(f"Token not found in response: {text}")
    return token


def upload_blueprint_zip(
    cfg: CfyConfig,
    token: str,
    blueprint_id: str,
    blueprint_zip: str,
    application_file: str,
) -> None:
    url = api_url(cfg, f"blueprints/{blueprint_id}")
    params = {"application_file": application_file}

    log(f"Uploading blueprint '{blueprint_id}' (zip={blueprint_zip}, app={application_file})...")
    with open(blueprint_zip, "rb") as f:
        status, text, _ = _request(
            cfg,
            "PUT",
            url,
            token=token,
            params=params,
            data_body=f,
            content_type="application/zip",
        )
    if status >= 400:
        raise CloudifyAPIError(f"Blueprint upload failed (HTTP {status}): {text}")
    log("Blueprint upload OK.")


def deployment_exists(cfg: CfyConfig, token: str, deployment_id: str) -> bool:
    url = api_url(cfg, f"deployments/{deployment_id}")
    status, _, _ = _request(cfg, "GET", url, token=token)
    if status == 404:
        return False
    if status >= 400:
        raise CloudifyAPIError(f"Failed to check deployment (HTTP {status}) {deployment_id}")
    return True


def create_deployment(
    cfg: CfyConfig,
    token: str,
    deployment_id: str,
    blueprint_id: str,
    inputs: Dict[str, Any],
) -> None:
    url = api_url(cfg, f"deployments/{deployment_id}")
    payload = {"blueprint_id": blueprint_id, "inputs": inputs}

    log(f"Creating deployment '{deployment_id}' from blueprint '{blueprint_id}'...")
    status, text, _ = _request(
        cfg,
        "PUT",
        url,
        token=token,
        json_body=payload,
        content_type="application/json",
    )
    if status >= 400:
        raise CloudifyAPIError(f"Deployment create failed (HTTP {status}): {text}")
    log("Deployment create OK.")


def start_execution(
    cfg: CfyConfig,
    token: str,
    deployment_id: str,
    workflow_id: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> str:
    url = api_url(cfg, "executions")
    payload: Dict[str, Any] = {"deployment_id": deployment_id, "workflow_id": workflow_id}
    if parameters:
        payload["parameters"] = parameters

    log(f"Starting workflow '{workflow_id}' on deployment '{deployment_id}'...")
    status, text, data = _request(
        cfg,
        "POST",
        url,
        token=token,
        json_body=payload,
        content_type="application/json",
    )
    if status >= 400:
        raise CloudifyAPIError(f"Start execution failed (HTTP {status}): {text}")

    exec_id = data.get("id")
    if not exec_id:
        raise CloudifyAPIError(f"Execution id missing in response: {text}")
    log(f"Execution started: {exec_id}")
    return exec_id


def wait_execution(cfg: CfyConfig, token: str, exec_id: str) -> None:
    url = api_url(cfg, f"executions/{exec_id}")
    deadline = time.time() + cfg.exec_timeout_sec

    while True:
        status, text, data = _request(cfg, "GET", url, token=token)
        if status >= 400:
            raise CloudifyAPIError(f"Failed to read execution (HTTP {status}): {text}")

        st = data.get("status", "unknown")
        log(f"Execution {exec_id} status: {st}")

        if st == "terminated":
            log("Execution succeeded.")
            return
        if st in ("failed", "cancelled"):
            raise CloudifyAPIError(f"Execution ended with status '{st}': {text}")

        if time.time() > deadline:
            raise CloudifyAPIError(f"Timed out waiting for execution {exec_id}. Last status: {st}")

        time.sleep(cfg.poll_interval_sec)


# -------------------------
# Main runner
# -------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generic Cloudify deploy runner (REST API only)")

    # Manager/Auth
    p.add_argument("--manager", default=os.getenv("CFY_MANAGER_URL"), help="Cloudify Manager URL (e.g., http://localhost:7081)")
    p.add_argument("--username", default=os.getenv("CFY_USERNAME"), help="Cloudify username")
    p.add_argument("--password", default=os.getenv("CFY_PASSWORD"), help="Cloudify password")
    p.add_argument("--tenant", default=os.getenv("CFY_TENANT"), help="Cloudify tenant (optional)")
    p.add_argument("--api-version", default=os.getenv("CFY_API_VERSION", "v3.1"), help="API version (v3.1 or v3). Default v3.1")
    p.add_argument("--insecure", action="store_true",
                   default=str_to_bool(os.getenv("CFY_INSECURE", "false")),
                   help="Disable TLS verification (self-signed certs). Can also set CFY_INSECURE=true")

    # Blueprint/deployment
    p.add_argument("--blueprint-id", required=True)
    p.add_argument("--blueprint-dir", required=True, help="Directory containing blueprint.yaml (will be zipped)")
    p.add_argument("--application-file", default="blueprint.yaml", help="Entry blueprint file inside zip")
    p.add_argument("--deployment-id", required=True)
    p.add_argument("--inputs-file", action="append", default=[], help="Inputs YAML file(s). Later files override earlier ones")

    # Execution behavior
    p.add_argument("--workflow", choices=["install", "update"], default="install")
    p.add_argument("--create-if-missing", action="store_true", default=True)
    p.add_argument("--wait", action="store_true", default=True)
    p.add_argument("--request-timeout-sec", type=int, default=int(os.getenv("CFY_REQUEST_TIMEOUT_SEC", "60")))
    p.add_argument("--exec-timeout-sec", type=int, default=int(os.getenv("CFY_EXEC_TIMEOUT_SEC", "3600")))
    p.add_argument("--poll-interval-sec", type=int, default=int(os.getenv("CFY_POLL_INTERVAL_SEC", "10")))

    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.manager:
        die("Missing manager URL. Set --manager or CFY_MANAGER_URL")
    if not args.username or not args.password:
        die("Missing username/password. Set --username/--password or CFY_USERNAME/CFY_PASSWORD")

    cfg = CfyConfig(
        manager_url=args.manager,
        username=args.username,
        password=args.password,
        tenant=args.tenant,
        api_version=args.api_version,
        insecure=args.insecure,
        request_timeout_sec=args.request_timeout_sec,
        exec_timeout_sec=args.exec_timeout_sec,
        poll_interval_sec=args.poll_interval_sec,
    )

    # Merge inputs
    merged_inputs: Dict[str, Any] = {}
    for fpath in args.inputs_file:
        merged_inputs = merge_dicts(merged_inputs, load_yaml_file(fpath))

    # Build zip
    tmp_zip = f"/tmp/{args.blueprint_id}.zip"
    build_zip_from_dir(args.blueprint_dir, tmp_zip, root_dir_name=args.blueprint_id)
    token = login_get_token(cfg)

    upload_blueprint_zip(cfg, token, args.blueprint_id, tmp_zip, args.application_file)

    if not deployment_exists(cfg, token, args.deployment_id):
        if not args.create_if_missing:
            die(f"Deployment missing and create disabled: {args.deployment_id}")
        create_deployment(cfg, token, args.deployment_id, args.blueprint_id, merged_inputs)

    exec_id = start_execution(cfg, token, args.deployment_id, args.workflow)

    if args.wait:
        wait_execution(cfg, token, exec_id)

    # cleanup
    try:
        os.remove(tmp_zip)
    except OSError:
        pass


if __name__ == "__main__":
    try:
        main()
    except CloudifyAPIError as e:
        die(f"Cloudify API error: {e}")
    except KeyboardInterrupt:
        die("Interrupted", code=130)
