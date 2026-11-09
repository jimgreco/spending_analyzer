#!/usr/bin/env python3
"""Rebuild and replace only the existing Spending service, keeping its live runtime config.

Runs on the production host (Amazon Linux python3, so keep Python 3.9 syntax).
The running container's Compose labels decide what to rebuild: the same project,
working directory and ordered Compose files that created it. Only the `spending`
service's image changes; its effective environment (including whichever
DATABASE_URL / runtime-role state the DB cutover left in place) must already
match the running container exactly or the release refuses.

Never print subprocess output: Compose config and docker inspect contain credentials.
"""

import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

PROJECT = "deploy"
SERVICE = "spending"
COMPOSE = "docker-compose"
SHARED_HOST_LOCK = Path("/tmp/codex-shared-host-release.lock")
LOCK_WAIT_SECONDS = 15 * 60
MIN_FREE_BYTES = 1024 ** 3
CUTOVER_WINDOW_SECONDS = 24 * 60 * 60
OWN_OVERRIDE = re.compile(r"spending-image-[0-9a-f]{40}\.json")
CUTOVER_OVERRIDE = re.compile(r"db-runtime-spending-[0-9a-f]{16}\.json")
RELEASE = re.compile(r"[0-9a-f]{40}")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")


class ReleaseError(Exception):
    pass


def run(*args, json_output=False, input_text=None):
    # Keep all output in memory; never put subprocess output in an exception or record.
    result = subprocess.run(args, input=input_text, text=True, capture_output=True)
    if result.returncode:
        raise ReleaseError("Command failed: {} {}; output withheld".format(args[0], args[1] if len(args) > 1 else ""))
    try:
        return json.loads(result.stdout) if json_output else result.stdout.strip()
    except ValueError:
        raise ReleaseError("Command returned invalid JSON; output withheld") from None


def environment(values):
    return dict(value.split("=", 1) for value in values or [] if "=" in value)


def configured_environment(service):
    configured = service.get("environment") or {}
    if isinstance(configured, list):
        configured = environment(configured)
    return {key: str(value) if value is not None else None for key, value in configured.items()}


def validate_environment(live, configured, image_defaults):
    """The running env must equal image defaults overlaid by the Compose environment."""
    resolved = dict(image_defaults)
    resolved.update(configured)
    if live != resolved:
        # Name only the keys; values may be credentials.
        changed = sorted(key for key in set(live) | set(resolved) if live.get(key) != resolved.get(key))
        raise ReleaseError("Effective Spending environment differs from the running container ({}); "
                           "reconcile before release".format(", ".join(changed)))
    if not (live.get("DATABASE_URL") or "").startswith(("postgres://", "postgresql://")):
        raise ReleaseError("Existing Spending DATABASE_URL is missing or not PostgreSQL")
    if "MIGRATION_DATABASE_URL" in live:
        raise ReleaseError("Running Spending environment contains MIGRATION_DATABASE_URL; reconcile before release")


def compose_for(container, directory):
    """Rebuild the exact Compose invocation recorded on the running container."""
    labels = container["Config"].get("Labels") or {}
    if labels.get("com.docker.compose.project") != PROJECT or labels.get("com.docker.compose.service") != SERVICE:
        raise ReleaseError("Container is not the existing deploy/spending service")
    if Path(labels.get("com.docker.compose.project.working_dir", "")).resolve() != directory:
        raise ReleaseError("Running Spending Compose working directory differs from the deploy directory")
    files = labels.get("com.docker.compose.project.config_files", "").split(",")
    if not files or any(not name for name in files):
        raise ReleaseError("Running Spending container has no recorded Compose files")
    command = [COMPOSE, "--project-name", PROJECT, "--project-directory", str(directory)]
    paths = []
    for name in files:
        path = Path(name).resolve()
        if path.parent != directory or not path.is_file():
            raise ReleaseError("Recorded Spending Compose file is missing or outside the deploy directory")
        paths.append(path)
        command.extend(["-f", str(path)])
    return command, paths


def validate_compose_version(container):
    recorded = (container["Config"].get("Labels") or {}).get("com.docker.compose.version")
    installed = run(COMPOSE, "version", "--short").lstrip("v")
    if not recorded or installed != recorded:
        raise ReleaseError("Installed Compose differs from the one that created Spending; reconcile before release")


def service_hash(output):
    fields = output.split()
    if len(fields) != 2 or fields[0] != SERVICE or not re.fullmatch(r"[0-9a-f]{64}", fields[1]):
        raise ReleaseError("Compose returned an invalid Spending configuration hash")
    return fields[1]


def stdin_compose(compose):
    command = []
    parts = iter(compose)
    for part in parts:
        if part == "-f":
            next(parts)
        else:
            command.append(part)
    return command + ["-f", "-"]


def validate_service_hash(live, compose, config):
    labels = live["Config"].get("Labels") or {}
    live_hash = labels.get("com.docker.compose.config-hash")
    configured_hash = service_hash(run(*compose, "config", "--hash", SERVICE))
    if configured_hash == live_hash:
        return
    # Compose 2.26.1 `up --no-deps` drops depends_on before hashing. Accept exactly
    # that case (same as Ritual Cue's helper); never waive unrelated drift.
    service = config.get("services", {}).get(SERVICE, {})
    if (not service.get("depends_on")
            or labels.get("com.docker.compose.depends_on") != ""
            or labels.get("com.docker.compose.version") != "2.26.1"):
        raise ReleaseError("Effective Spending Compose configuration differs from the running container")
    # The resolved model goes through stdin, never a file or argv.
    command = stdin_compose(compose) + ["config", "--hash", SERVICE]
    if service_hash(run(*command, input_text=json.dumps(config))) != configured_hash:
        raise ReleaseError("Cannot verify the unchanged Spending Compose model through stdin")
    without_dependencies = copy.deepcopy(config)
    without_dependencies["services"][SERVICE].pop("depends_on")
    if service_hash(run(*command, input_text=json.dumps(without_dependencies))) != live_hash:
        raise ReleaseError("Effective Spending Compose configuration differs from the running container")


def validate_build_source(service, webapp):
    """Only a source-built service can be released from a commit."""
    build = service.get("build")
    if not isinstance(build, dict) or not build.get("context"):
        raise ReleaseError("Spending has no Compose build section; it runs a prebuilt image only. "
                           "Automatic release refuses to switch artifacts; release it manually")
    if Path(build["context"]).resolve() != webapp.resolve():
        raise ReleaseError("Spending build context differs from the transferred webapp directory")


def check_cutover_window(paths, now):
    """Do not replace the DB-cutover image while its observation/recovery window is open."""
    for path in paths:
        if CUTOVER_OVERRIDE.fullmatch(path.name) and now - path.stat().st_mtime < CUTOVER_WINDOW_SECONDS:
            raise ReleaseError("Spending DB runtime cutover is within its 24-hour recovery window; "
                               "automatic release waits until it closes")


def retained_files(compose, paths):
    """Drop this helper's earlier image-only overrides; the new one supersedes them."""
    base = compose[:5]
    kept = []
    for path in paths:
        if OWN_OVERRIDE.fullmatch(path.name):
            try:
                content = json.loads(path.read_text())
            except ValueError:
                content = None
            image = ((content or {}).get("services") or {}).get(SERVICE) or {}
            if (set(content or {}) == {"services"} and set(content["services"]) == {SERVICE}
                    and set(image) == {"image"}):
                continue
        kept.append(path)
    command = list(base)
    for path in kept:
        command.extend(["-f", str(path)])
    return command


def image_pinned(service):
    return bool(service.get("image"))


def release_metadata(webapp, release):
    try:
        data = json.loads((webapp / "version.json").read_text())
        version = (webapp / "VERSION").read_text().strip()
    except (OSError, ValueError):
        raise ReleaseError("Transferred version.json/VERSION is missing or invalid") from None
    if not isinstance(data, dict) or data.get("sha") != release[:7] or not isinstance(data.get("timestamp"), str):
        raise ReleaseError("Transferred version.json does not match the requested commit")
    if data.get("version") != version:
        raise ReleaseError("Transferred version.json does not match webapp/VERSION")
    return data


def check_free_space():
    root = run("docker", "info", "--format", "{{.DockerRootDir}}")
    stats = os.statvfs(root)
    if stats.f_bavail * stats.f_frsize < MIN_FREE_BYTES:
        raise ReleaseError("Less than 1 GiB free for Docker; refusing to build (no cleanup is implicit)")


def preflight(directory, webapp, expected_id=None):
    ids = run("docker", "ps", "--filter", "label=com.docker.compose.project=" + PROJECT,
              "--filter", "label=com.docker.compose.service=" + SERVICE, "--format", "{{.ID}}").splitlines()
    if len(ids) != 1:
        raise ReleaseError("Expected exactly one running deploy/spending container")
    live = run("docker", "inspect", ids[0], json_output=True)[0]
    if expected_id and live["Id"] != expected_id:
        raise ReleaseError("Running Spending container changed during release; stop and reconcile")
    validate_compose_version(live)
    compose, paths = compose_for(live, directory)
    config = run(*compose, "config", "--format", "json", json_output=True)
    service = config.get("services", {}).get(SERVICE, {})
    validate_service_hash(live, compose, config)
    validate_build_source(service, webapp)
    image = run("docker", "image", "inspect", live["Image"], json_output=True)[0]
    validate_environment(environment(live["Config"].get("Env")), configured_environment(service),
                         environment(image["Config"].get("Env")))
    return live, compose, paths, service


VERIFY = (
    "import json,sys,urllib.request\n"
    "sha=sys.argv[1]\n"
    "assert json.load(open('/app/version.json')).get('sha')==sha\n"
    "r=urllib.request.urlopen('http://127.0.0.1:8000/api/version',timeout=5)\n"
    "assert r.status==200 and json.load(r).get('sha')==sha\n"
)

REVISION = (
    "import json\n"
    "try:\n"
    "    print(json.dumps({'sha': json.load(open('/app/version.json')).get('sha')}))\n"
    "except Exception:\n"
    "    print(json.dumps({'sha': None}))\n"
)


def verify(container_id, release):
    # Read-only: baked metadata and the public version endpoint inside the container.
    run("docker", "exec", container_id, "python", "-c", VERIFY, release[:7])


def deployed_revision(live):
    try:
        sha = run("docker", "exec", live["Id"], "python", "-c", REVISION, json_output=True).get("sha")
    except (ReleaseError, AttributeError):
        sha = None
    # Informational only: rollback uses the exact image ID, not this SHA.
    return sha if isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{7,40}", sha) else None


def validate_image_only_override(compose, candidate_compose, image):
    original = run(*compose, "config", "--format", "json", json_output=True)
    candidate = run(*candidate_compose, "config", "--format", "json", json_output=True)
    expected = copy.deepcopy(original)
    expected["services"][SERVICE]["image"] = image
    if candidate != expected:
        raise ReleaseError("Candidate Compose configuration changes more than the Spending image")


def write_image_override(path, image, exclusive=False):
    with path.open("x" if exclusive else "w") as file:
        json.dump({"services": {SERVICE: {"image": image}}}, file)
        file.write("\n")


def acquire(path, deadline):
    handle = path.open("a")
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except BlockingIOError:
            if time.time() > deadline:
                handle.close()
                raise ReleaseError("Another release holds {}; retry later".format(path))
            time.sleep(5)


def release(directory, webapp, sha):
    live, compose, paths, service = preflight(directory, webapp)
    check_cutover_window(paths, time.time())
    release_metadata(webapp, sha)
    check_free_space()
    record_dir = directory / "release-records"
    record_dir.mkdir(exist_ok=True)
    record = record_dir / "spending-{}.json".format(sha)
    if record.exists():
        raise ReleaseError("Release record already exists; inspect it before retrying")
    if image_pinned(service):
        print("Spending image is pinned by a recorded Compose override; this release replaces only that pin", flush=True)
    rollback = "spending-rollback:" + sha
    release_tag = "spending-release:" + sha
    image_override = directory / "spending-image-{}.json".format(sha)
    # Recorded overlays (including any DB runtime cutover) stay in order; only this
    # new release file is added, and only the image may differ from the live model.
    write_image_override(image_override, release_tag, exclusive=True)
    candidate_compose = retained_files(compose, paths) + ["-f", str(image_override)]
    validate_image_only_override(compose, candidate_compose, release_tag)
    run("docker", "image", "tag", live["Image"], rollback)
    recovery = {"sha": sha, "previousContainer": live["Id"], "previousImage": live["Image"],
                "previousImageName": live["Config"]["Image"], "previousSha": deployed_revision(live),
                "rollbackTag": rollback, "compose": compose, "candidateCompose": candidate_compose,
                "imageOverride": str(image_override), "status": "prepared"}
    record.write_text(json.dumps(recovery, indent=2) + "\n")
    print("Rollback image retained: " + rollback, flush=True)
    run(*candidate_compose, "build", SERVICE)
    built_image = run("docker", "image", "inspect", release_tag, json_output=True)[0].get("Id")
    if not isinstance(built_image, str) or not IMAGE_ID.fullmatch(built_image):
        raise ReleaseError("Built Spending image has no immutable image ID")
    write_image_override(image_override, built_image)
    validate_image_only_override(compose, candidate_compose, built_image)
    preflight(directory, webapp, expected_id=live["Id"])
    run(*candidate_compose, "up", "-d", "--no-deps", "--no-build", SERVICE)
    recovery["status"] = "replaced-awaiting-health"
    record.write_text(json.dumps(recovery, indent=2) + "\n")
    for _ in range(60):
        try:
            current, _, _, _ = preflight(directory, webapp)
            if current["Image"] != built_image:
                raise ReleaseError("Running Spending image is not the built release image")
            verify(current["Id"], sha)
        except ReleaseError:
            time.sleep(2)
            continue
        recovery.update(status="healthy", container=current["Id"], image=current["Image"])
        record.write_text(json.dumps(recovery, indent=2) + "\n")
        print(json.dumps({"status": "healthy", "sha": sha, "container": current["Id"],
                          "image": current["Image"], "recoveryRecord": str(record)}))
        return
    recovery["status"] = "unhealthy-needs-review"
    record.write_text(json.dumps(recovery, indent=2) + "\n")
    raise ReleaseError("Spending did not verify; rollback image {} and record {} retained".format(rollback, record))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deploy-directory", type=Path, default=Path.home() / "deploy")
    parser.add_argument("--release", help="Exact 40-character commit SHA; omit for read-only preflight")
    args = parser.parse_args()
    directory = args.deploy_directory.resolve()
    webapp = Path(__file__).resolve().parents[1] / "webapp"
    if args.release and not RELEASE.fullmatch(args.release):
        raise ReleaseError("Release requires an exact lowercase 40-character Git SHA")
    if not args.release:
        live, _, paths, _ = preflight(directory, webapp)
        check_cutover_window(paths, time.time())
        print(json.dumps({"preflight": "passed", "container": live["Id"], "image": live["Image"]}))
        return
    deadline = time.time() + LOCK_WAIT_SECONDS
    # Same lock order as the DB cutover (shared host lock, then the deploy-dir lock).
    with acquire(SHARED_HOST_LOCK, deadline), acquire(directory / ".app-release.lock", deadline):
        release(directory, webapp, args.release)


if __name__ == "__main__":
    try:
        main()
    except (ReleaseError, OSError, ValueError, KeyError, IndexError) as error:
        # Unrecognized exceptions can include command input; never dump the config.
        print(str(error) if isinstance(error, ReleaseError) else
              "Spending deployment failed ({}); inspect the non-secret release record".format(type(error).__name__),
              file=sys.stderr)
        sys.exit(1)
