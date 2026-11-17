"""Prepare an isolated x86_64 Linux guest for test_writer_publication_crash.py.

Usage: python prepare_writer_crash_vm.py --output /tmp/writer-crash-vm
Requires Python 3.11+, Docker/Podman, gcc, ldd and cpio. A container supplies QEMU, an Ubuntu
kernel and static BusyBox; no packages are installed on the host. The initramfs
contains the built live example, its host glibc libraries and the crash shim.
Pass the output directory as --vm-tools to the crash check. Use an output path
on a disk filesystem for crash results; /tmp can be backed by RAM.
"""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile

IMAGE = "localhost/ttf-writer-crash:local"
INIT = """#!/bin/sh
export PATH=/bin
mount -t devtmpfs devtmpfs /dev
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t tmpfs tmpfs /tmp
ip link set lo up
ip link set eth0 up
ip addr add 10.0.2.15/24 dev eth0
ip route add default via 10.0.2.2
mount -t ext4 /dev/vda /data || { echo TTF_MOUNT_FAILED; sleep 999999; }
for option in $(cat /proc/cmdline); do
 case "$option" in
 ttf_stage=*) TTF_CRASH_STAGE=${option#ttf_stage=} ;;
 esac
done
export TTF_CRASH_STAGE TTF_CRASH_ROOT=/data/table TTF_CRASH_VERSION=0000000003.json
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export LD_PRELOAD=/lib64/writer_crash_hook.so
/bin/live_binance_writer /data/table --commits 100 --rows 50 --delay-ms 200
status=$?
echo TTF_WRITER_EXIT=$status
sleep 999999
"""


def pack(output):
    root = output / "initramfs-root"
    names = [".", *(str(path.relative_to(root)) for path in sorted(root.rglob("*")))]
    archive = subprocess.run(
        ["cpio", "--null", "-o", "--format=newc"],
        cwd=root,
        input=("\0".join(names) + "\0").encode(),
        capture_output=True,
        check=True,
    )
    with gzip.open(output / "initramfs.cpio.gz", "wb", compresslevel=1) as target:
        target.write(archive.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--binary", type=Path, default=Path("target/debug/examples/live_binance_writer")
    )
    parser.add_argument("--qemu-image", default=IMAGE)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        parser.error("this manual guest setup requires x86_64 Linux with glibc")
    output = args.output.resolve()
    binary = args.binary.resolve(strict=True)
    output.mkdir(parents=True, exist_ok=False)
    exists = subprocess.run(
        ["docker", "image", "inspect", args.qemu_image], capture_output=True
    )
    if exists.returncode != 0:
        with tempfile.TemporaryDirectory(prefix="ttf-crash-image-") as context:
            Path(context, "Dockerfile").write_text(
                "FROM ubuntu:24.04\n"
                "RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y "
                "--no-install-recommends qemu-system-x86 busybox-static linux-image-virtual "
                "ca-certificates && rm -rf /var/lib/apt/lists/*\n"
            )
            subprocess.run(
                ["docker", "build", "-t", args.qemu_image, context], check=True
            )
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{output}:/work:z",
            "--entrypoint",
            "sh",
            args.qemu_image,
            "-c",
            "cp /boot/vmlinuz /work/vmlinuz && cp /bin/busybox /work/busybox && "
            "cp /etc/ssl/certs/ca-certificates.crt /work/ca-certificates.crt && "
            "chmod a+r /work/vmlinuz /work/busybox",
        ],
        check=True,
    )
    root = output / "initramfs-root"
    for part in ["bin", "lib64", "dev", "proc", "sys", "tmp", "data", "etc/ssl/certs"]:
        (root / part).mkdir(parents=True, exist_ok=True)
    shutil.copy(output / "busybox", root / "bin/busybox")
    for app in ["sh", "mount", "ip", "cat", "sleep"]:
        (root / "bin" / app).symlink_to("busybox")
    linked = subprocess.run(
        ["ldd", str(binary)], capture_output=True, text=True, check=True
    )
    for library in re.findall(r"/[^\s()]+", linked.stdout):
        shutil.copy(library, root / "lib64" / Path(library).name)
    shutil.copy(binary, root / "bin/live_binance_writer")
    subprocess.run(["strip", str(root / "bin/live_binance_writer")], check=True)
    subprocess.run(
        [
            "gcc",
            "-shared",
            "-fPIC",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-o",
            str(root / "lib64/writer_crash_hook.so"),
            str(Path(__file__).with_name("writer_crash_hook.c")),
        ],
        check=True,
    )
    shutil.copy(
        output / "ca-certificates.crt", root / "etc/ssl/certs/ca-certificates.crt"
    )
    (root / "etc/resolv.conf").write_text("nameserver 10.0.2.3\n")
    (root / "init").write_text(INIT)
    (root / "init").chmod(0o755)
    pack(output)
    with binary.open("rb") as binary_file:
        binary_hash = hashlib.file_digest(binary_file, "sha256").hexdigest()
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "binary_sha256": binary_hash,
                "hook_sha256": hashlib.sha256(
                    Path(__file__).with_name("writer_crash_hook.c").read_bytes()
                ).hexdigest(),
                "qemu_image": args.qemu_image,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"Ready: --vm-tools {output} --qemu-image {args.qemu_image}")


if __name__ == "__main__":
    main()
