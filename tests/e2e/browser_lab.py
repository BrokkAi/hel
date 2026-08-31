#!/usr/bin/env python3
"""Drive the real Hel web viewer concurrently with a terminal dashboard."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import pathlib
import subprocess
import sys
import time
import traceback

from reliability_lab import Lab, ScenarioFailure, TIMEOUT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--hel", required=True, type=pathlib.Path)
    return parser.parse_args()


def start_dashboard(lab: Lab):
    client = lab.start_tui("tui-1")
    client.wait_for("Workspaces")
    client.send(b"\r\r")
    client.wait_for("Active")
    return client


def qr_url(lab: Lab) -> str:
    reply = lab.daemon_request({"action": "status"})
    if not isinstance(reply, dict) or reply.get("reply") != "status":
        raise ScenarioFailure(f"unexpected daemon status reply: {reply!r}")
    phone = reply.get("value", {}).get("phone_status", {})
    url = phone.get("qr_login_url") if isinstance(phone, dict) else None
    if not isinstance(url, str) or not url.startswith("https://"):
        raise ScenarioFailure(f"daemon did not publish an HTTPS QR login URL: {phone!r}")
    return url


def wait_marker_or_exit(marker: pathlib.Path, browser: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if marker.exists():
            return
        code = browser.poll()
        if code is not None:
            raise ScenarioFailure(f"Playwright exited before browser/TUI synchronization ({code})")
        time.sleep(0.05)
    raise ScenarioFailure("Playwright did not reach its offline synchronization point")


def finish_from_dashboard(client) -> None:
    client.send(b"\x06")
    client.wait_for("Finish session?")
    client.send(b"\r")


def run(lab: Lab) -> None:
    port = lab.prepare(phone_tls=True)
    dashboard = start_dashboard(lab)
    code, _ = lab.wait_daemon_status(port)
    lab.base_url = f"https://127.0.0.1:{port}"
    status, _ = lab.request("POST", "/auth/session", {"code": code})
    if status != 204:
        raise ScenarioFailure(f"Python observer login returned {status}")
    login_url = qr_url(lab)
    title = f"browser-reliability-{lab.seed}"
    ready_marker = lab.runtime_root / "browser-ready"
    changed_marker = lab.runtime_root / "tui-changed"
    web_root = pathlib.Path(__file__).resolve().parent / "web"
    browser_log = (lab.root / "browser.log").open("wb")
    environment = lab.environment()
    environment.update(
        {
            "HEL_BROWSER_BASE_URL": lab.base_url,
            "HEL_BROWSER_CODE": code,
            "HEL_BROWSER_QR_URL": login_url,
            "HEL_BROWSER_TITLE": title,
            "HEL_BROWSER_PROJECT_DIRECTORY": str(lab.project),
            "HEL_BROWSER_READY_MARKER": str(ready_marker),
            "HEL_TUI_CHANGED_MARKER": str(changed_marker),
            "HEL_BROWSER_TRACE": str(lab.root / "browser-trace.zip"),
            "HEL_BROWSER_SCREENSHOT": str(lab.root / "browser-failure.png"),
        }
    )
    browser = subprocess.Popen(
        [str(web_root / "node_modules/.bin/playwright"), "test"],
        cwd=web_root,
        env=environment,
        stdout=browser_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    lab.record_process("started", "playwright", browser.pid)
    try:
        wait_marker_or_exit(ready_marker, browser)
        dashboard.wait_for(title)
        dashboard.resize(18, 72)
        time.sleep(0.2)
        dashboard.resize(40, 150)
        time.sleep(0.5)
        dashboard.wait_for(title)
        lab.record_action("dashboard-resized", rows=40, columns=150)
        finish_from_dashboard(dashboard)
        snapshot = lab.wait_snapshot(
            lambda value: any(
                item.get("title") == title and item.get("state") == "saved"
                for item in value.get("sessions", [])
            ),
            "TUI-finished browser session",
        )
        session = next(item for item in snapshot["sessions"] if item["title"] == title)
        changed_marker.write_text("TUI Finish reached durable state\n")
        lab.record_action("tui-finished-session", session_id=session["id"])
        try:
            return_code = browser.wait(timeout=TIMEOUT * 2)
        except subprocess.TimeoutExpired as error:
            raise ScenarioFailure("Playwright did not finish after SSE reconnection") from error
        if return_code != 0:
            raise ScenarioFailure(f"Playwright failed with exit code {return_code}")
        lab.record_process("stopped", "playwright", browser.pid)
        (lab.root / "browser-transcript.json").write_text(
            json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
        )
        quit_elapsed = dashboard.quit()
        lab.record_process("stopped", "tui-1", dashboard.process.pid)
        if quit_elapsed >= 2:
            raise ScenarioFailure(f"dashboard quit took {quit_elapsed:.3f}s")
        lab.stop_daemon()
        lab.integrity()
        leaks = lab.owned_pids()
        if leaks:
            raise ScenarioFailure(f"owned processes remained after cleanup: {leaks}")
        lab.trace["finished_at"] = lab.timestamp()
        lab.trace["outcome"] = "passed"
        lab.write_trace()
    finally:
        if browser.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(browser.pid, 15)
            with contextlib.suppress(subprocess.TimeoutExpired):
                browser.wait(timeout=2)
        browser_log.close()


def main() -> int:
    args = parse_args()
    lab = Lab(args.hel, "browser-tui-convergence", args.seed)
    print(f"browser reliability: artifacts={lab.root}", flush=True)
    try:
        run(lab)
    except BaseException as error:
        (lab.root / "failure-traceback.txt").write_text(traceback.format_exc())
        lab.trace["finished_at"] = lab.timestamp()
        lab.trace["outcome"] = "failed"
        lab.trace["failure"] = str(error)
        lab.write_trace()
        lab.cleanup_owned()
        with contextlib.suppress(Exception):
            lab.integrity()
        lab.capture_process_tree()
        lab.preserve_runtime()
        lab.remove_runtime()
        print(f"browser reliability: failed: {error}", file=sys.stderr)
        return 1
    lab.capture_process_tree()
    lab.preserve_runtime()
    lab.remove_runtime()
    print("browser reliability: passed clients=2 sse_reconnect=1 leaks=0", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
