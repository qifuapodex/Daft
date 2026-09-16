"""Publish each completed platform from an apodex build without waiting for the matrix."""

import argparse
import email.parser
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import time
import zipfile


PLATFORMS = {
    "ubuntu-x86_64": "manylinux_2_24_x86_64",
    "ubuntu-aarch64": "manylinux_2_24_aarch64",
    "macos-x86_64": "macosx_10_12_x86_64",
    "macos-aarch64": "macosx_11_0_arm64",
    "windows-x86_64": "win_amd64",
}


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True, timeout=120)


def api(path):
    return json.loads(gh("api", path))


def pages(path, key):
    return [item for page in json.loads(gh("api", "--paginate", "--slurp", path)) for item in page[key]]


def validate(directory, platform, version):
    expected = {f"daft-{version}-cp310-abi3-{PLATFORMS[platform]}.whl"}
    if platform == "ubuntu-x86_64":
        expected.add(f"daft-{version}.tar.gz")
    files = sorted(directory.iterdir())
    if {p.name for p in files} != expected or not all(p.is_file() for p in files):
        raise ValueError(f"Unexpected assets for {platform}: {[p.name for p in files]}")
    for path in files:
        if path.suffix == ".whl":
            with zipfile.ZipFile(path) as archive:
                entries = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
                if len(entries) != 1:
                    raise ValueError(f"Invalid wheel metadata: {path}")
                metadata = email.parser.BytesParser().parsebytes(archive.read(entries[0]))
        else:
            with tarfile.open(path) as archive:
                entries = [m for m in archive.getmembers() if m.name.endswith("/PKG-INFO") and m.name.count("/") == 1]
                if len(entries) != 1:
                    raise ValueError(f"Invalid source distribution metadata: {path}")
                with archive.extractfile(entries[0]) as source:
                    metadata = email.parser.BytesParser().parse(source)
        if metadata["Name"] != "daft" or metadata["Version"] != version:
            raise ValueError(f"Unexpected package name/version: {path}")
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--sha", required=True)
    args = parser.parse_args()
    repo = os.environ["GITHUB_REPOSITORY"]
    base = f"repos/{repo}"
    run_path = f"{base}/actions/runs/{args.run_id}"
    run = api(run_path)
    if run["head_sha"] != args.sha or run["path"] != ".github/workflows/publish-apodex-release.yml":
        raise ValueError("Source run does not match the expected release build")
    # The release note is created before this publisher starts.
    api(f"{base}/releases/tags/{args.tag}")
    published = set()
    deadline = time.monotonic() + 6 * 3600
    with tempfile.TemporaryDirectory() as temporary:
        while time.monotonic() < deadline:
            artifacts = pages(f"{run_path}/artifacts?per_page=100", "artifacts")
            for platform in PLATFORMS:
                if platform in published:
                    continue
                matches = [a for a in artifacts if a["name"].startswith(f"wheels-{platform}-lts=false-lto=") and not a["expired"]]
                if not matches:
                    continue
                if len(matches) != 1:
                    raise ValueError(f"Ambiguous artifacts for {platform}")
                artifact = matches[0]
                directory = Path(temporary) / str(artifact["id"])
                gh("run", "download", str(args.run_id), "--repo", repo, "--name", artifact["name"], "--dir", str(directory))
                files = validate(directory, platform, args.version)
                # Do not overwrite a newer rebuild if this publisher was delayed.
                ref = api(f"{base}/git/ref/tags/{args.tag}")
                if ref["object"]["type"] != "commit" or ref["object"]["sha"] != args.sha:
                    raise ValueError("Release tag moved away from the source build; refusing stale upload")
                gh("release", "upload", args.tag, "--repo", repo, "--clobber", *(str(p) for p in files))
                published.add(platform)
                print(f"Published {platform}: {', '.join(p.name for p in files)}", flush=True)
            if len(published) == len(PLATFORMS):
                print("All five platforms and source distribution published", flush=True)
                return
            jobs = pages(f"{run_path}/jobs?per_page=100", "jobs")
            builds = [j for j in jobs if j["name"].endswith(" / build")]
            run = api(run_path)
            if run["status"] == "completed" or (len(builds) == 5 and all(j["status"] == "completed" for j in builds)):
                missing = sorted(set(PLATFORMS) - published)
                raise RuntimeError(f"Build finished without publishable artifacts for {missing}; successful platforms remain available")
            print(f"Published {len(published)}/5 platforms; waiting for remaining artifacts", flush=True)
            time.sleep(45)
    raise TimeoutError("Timed out waiting for remaining platform artifacts")


if __name__ == "__main__":
    main()
