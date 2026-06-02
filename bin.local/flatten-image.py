#!/usr/bin/env python3
"""Flatten a podman image into a single layer, preserving its config.

`podman export | podman import` collapses an image to one layer but carries
only the filesystem -- not the config (CMD, ENV, ENTRYPOINT, ...). This script
reads the source image's config as JSON and reconstructs it via `--change`
flags, using JSON exec-form for CMD/ENTRYPOINT so arguments containing spaces
survive intact.

The flattened image overwrites the source tag in place.

Usage: flatten-image.py <image>
"""

import json
import subprocess
import sys


def run(cmd, **kw):
    """Run a command, failing loudly (no silent errors)."""
    return subprocess.run(cmd, check=True, text=True, **kw)


def inspect_config(image):
    out = run(
        ["podman", "image", "inspect", image],
        stdout=subprocess.PIPE,
    ).stdout
    data = json.loads(out)
    if not data:
        sys.exit(f"error: no inspect data for {image!r}")
    return data[0].get("Config") or {}


def build_changes(cfg):
    """Turn an image Config dict into a list of `--change` arguments."""
    changes = []

    def add(directive, value):
        changes.extend(["--change", f"{directive} {value}"])

    # Exec-form (JSON array) preserves arguments that contain spaces. This is
    # the bug the bash version had: it space-joined Cmd and corrupted it.
    if cfg.get("Cmd"):
        add("CMD", json.dumps(cfg["Cmd"]))
    if cfg.get("Entrypoint"):
        add("ENTRYPOINT", json.dumps(cfg["Entrypoint"]))

    # Scalars.
    if cfg.get("WorkingDir"):
        add("WORKDIR", cfg["WorkingDir"])
    if cfg.get("User"):
        add("USER", cfg["User"])
    if cfg.get("StopSignal"):
        add("STOPSIGNAL", cfg["StopSignal"])

    # One --change per entry.
    for env in cfg.get("Env") or []:
        add("ENV", env)
    for port in cfg.get("ExposedPorts") or {}:
        add("EXPOSE", port)
    for vol in cfg.get("Volumes") or {}:
        add("VOLUME", vol)
    for key, val in (cfg.get("Labels") or {}).items():
        add("LABEL", f"{key}={val}")

    return changes


def flatten(image):
    cfg = inspect_config(image)
    changes = build_changes(cfg)

    # `podman create` allocates a container rootfs without running it.
    ctr = run(
        ["podman", "create", image],
        stdout=subprocess.PIPE,
    ).stdout.strip()

    try:
        print(f"Flattening {image} ...", file=sys.stderr)
        # export | import, streamed so we never buffer the whole rootfs.
        exporter = subprocess.Popen(
            ["podman", "export", ctr], stdout=subprocess.PIPE
        )
        importer = subprocess.Popen(
            ["podman", "import", *changes, "-", image],
            stdin=exporter.stdout,
        )
        exporter.stdout.close()  # let exporter get SIGPIPE if importer dies
        importer.communicate()
        export_rc = exporter.wait()
        if export_rc != 0:
            sys.exit(f"error: podman export failed (rc={export_rc})")
        if importer.returncode != 0:
            sys.exit(f"error: podman import failed (rc={importer.returncode})")
    finally:
        subprocess.run(
            ["podman", "rm", "-f", ctr],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    layers = run(
        ["podman", "image", "inspect", image,
         "--format", "{{len .RootFS.Layers}}"],
        stdout=subprocess.PIPE,
    ).stdout.strip()
    print(f"Done. {image} is now {layers} layer(s).", file=sys.stderr)


def main(argv):
    if len(argv) != 2:
        sys.exit(f"Usage: {argv[0]} <image>")
    flatten(argv[1])


if __name__ == "__main__":
    main(sys.argv)
