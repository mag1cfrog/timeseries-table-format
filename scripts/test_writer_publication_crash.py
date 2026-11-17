"""Manual Linux crash checks against real Binance trades, outside offline CI.

Requires Python 3.11+, the live example, tstable CLI, PyArrow and gcc. Process checks:
  python test_writer_publication_crash.py --output /tmp/writer-crash
For guest power cuts, run prepare_writer_crash_vm.py first, then pass its
--vm-tools DIR and --qemu-image IMAGE. The guest mounts /dev/vda at /data,
configures QEMU user networking and runs the same example with the shim.
The container image supplies qemu-system-x86_64. Only its /dev/kvm and disposable
disk image are exposed. Killing QEMU discards guest RAM, with cache=none disk IO.
This models guest power loss, not physical controller or drive failure.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import subprocess
import threading
import time

from test_live_binance_writer import committed_rows, run, verify_remote

STAGES = (
    "log-created",
    "log-synced",
    "current-synced",
    "before-current-rename",
    "after-current-rename",
    "acknowledged",
)


def crash(command, directory, stage, kill, env=None, stopped_pid=False):
    """Wait for an observed boundary, then terminate without running destructors."""
    events = queue.Queue()
    captured = {"stdout": [], "stderr": []}
    (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )

    def read(stream, name):
        for line in stream:
            events.put((name, line))
        events.put((name, None))

    readers = [
        threading.Thread(target=read, args=(process.stdout, "stdout"), daemon=True),
        threading.Thread(target=read, args=(process.stderr, "stderr"), daemon=True),
    ]
    for reader in readers:
        reader.start()
    reached = False
    deadline = time.monotonic() + 90
    closed = 0
    try:
        while time.monotonic() < deadline and closed < 2:
            name, line = events.get(timeout=max(0.01, deadline - time.monotonic()))
            if line is None:
                closed += 1
                continue
            captured[name].append(line)
            if stage == "acknowledged":
                if not line.startswith("{"):
                    continue
                event = json.loads(line)
                reached = event.get("event") == "committed" and event["version"] == 3
            else:
                reached = f"TTF_CRASH_BOUNDARY={stage}" in line
            if reached:
                if stopped_pid and stage != "acknowledged":
                    # The marker precedes SIGSTOP by a few instructions. Wait for
                    # the stop so the target cannot advance past the chosen point.
                    for _ in range(1000):
                        status = Path(f"/proc/{process.pid}/status").read_text()
                        if "State:\tT" in status:
                            break
                        time.sleep(0.001)
                    else:
                        raise AssertionError("shim did not stop the writer")
                kill(process)
                process.wait(timeout=20)
                assert process.returncode != 0, "target was not forcibly terminated"
                break
        assert reached, f"did not reach {stage}"
    finally:
        if process.poll() is None:
            kill(process)
            process.wait(timeout=20)
        for reader in readers:
            reader.join(timeout=5)
        while not events.empty():
            name, line = events.get_nowait()
            if line is not None:
                captured[name].append(line)
        process.stdout.close()
        process.stderr.close()
        for name, lines in captured.items():
            (directory / f"crash.{name}.log").write_text("".join(lines))
    return process.returncode


def process_crash(args, directory, stage, hook, with_competitor=False):
    root = directory / "table"
    env = os.environ | {
        "LD_PRELOAD": str(hook),
        "TTF_CRASH_STAGE": stage,
        "TTF_CRASH_ROOT": str(root),
        "TTF_CRASH_VERSION": "0000000003.json",
    }
    command = [str(args.binary), str(root), "--commits", "100", "--delay-ms", "200"]
    competed = False

    def kill(process):
        nonlocal competed
        if with_competitor and not competed:
            competed = True

            def snapshot():
                return {
                    str(path.relative_to(root)): (path.stat().st_ino, path.read_bytes())
                    for path in root.rglob("*")
                    if path.is_file()
                }

            try:
                before = snapshot()
                competitor = subprocess.run(
                    [str(args.binary), str(root), "--commits", "1"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                (directory / "competitor.stdout.jsonl").write_text(competitor.stdout)
                (directory / "competitor.stderr.log").write_text(competitor.stderr)
                assert competitor.returncode != 0, (
                    "competing commit unexpectedly succeeded"
                )
                assert "CommitInProgress" in competitor.stderr, competitor.stderr
                assert snapshot() == before, (
                    "competitor changed the active writer's files"
                )
            finally:
                process.kill()
        process.kill()

    code = crash(command, directory, stage, kill, env, True)
    assert code == -signal.SIGKILL


def vm_crash(args, directory, stage):
    disk = directory / "disk.raw"
    with disk.open("wb") as target:
        target.truncate(512 * 1024 * 1024)
    seed = directory / "seed"
    seed.mkdir()
    shutil.move(directory / "table", seed / "table")
    subprocess.run(["mkfs.ext4", "-q", "-F", "-d", str(seed), str(disk)], check=True)
    container = f"ttf-crash-{os.getpid()}-{stage}"
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        container,
        "--device",
        "/dev/kvm",
        "-v",
        f"{args.vm_tools}:/tools:ro,z",
        "-v",
        f"{directory}:/case:z",
        args.qemu_image,
        "qemu-system-x86_64",
        "-machine",
        "q35,accel=kvm",
        "-cpu",
        "host",
        "-m",
        "512",
        "-smp",
        "2",
        "-nodefaults",
        "-display",
        "none",
        "-monitor",
        "none",
        "-serial",
        "stdio",
        "-no-reboot",
        "-kernel",
        "/tools/vmlinuz",
        "-initrd",
        "/tools/initramfs.cpio.gz",
        "-append",
        f"console=ttyS0 quiet panic=1 ttf_stage={stage}",
        "-drive",
        "file=/case/disk.raw,format=raw,cache=none,aio=threads,if=virtio",
        "-netdev",
        "user,id=net0",
        "-device",
        "virtio-net-pci,netdev=net0",
    ]

    def cut_power(_process):
        subprocess.run(
            ["docker", "kill", "--signal", "KILL", container],
            check=True,
            capture_output=True,
            text=True,
        )

    crash(command, directory, stage, cut_power)
    # Replay ext4's journal before examining the image. e2fsck returns 1 for
    # corrected errors; higher codes mean this recovery cannot be trusted.
    fsck = subprocess.run(["e2fsck", "-fy", str(disk)], capture_output=True, text=True)
    (directory / "fsck.log").write_text(fsck.stdout + fsck.stderr)
    assert fsck.returncode in (0, 1), "filesystem recovery failed"
    recovered = directory / "recovered"
    recovered.mkdir()
    extraction = subprocess.run(
        ["debugfs", "-R", f"rdump /table {recovered}", str(disk)],
        capture_output=True,
        text=True,
        check=True,
    )
    (directory / "debugfs.log").write_text(extraction.stdout + extraction.stderr)
    shutil.move(recovered / "table", directory / "table")


def verify(args, directory, stage, mode):
    root = directory / "table"
    result = verify_remote(root)
    version, _, rows = committed_rows(root)
    if stage == "acknowledged" or (
        stage == "after-current-rename" and mode == "process"
    ):
        publication_ok = version == 3 and len(rows) == 100
    elif stage == "after-current-rename":
        publication_ok = (version, len(rows)) in ((2, 50), (3, 100))
    else:
        publication_ok = version == 2 and len(rows) == 50
    result["publication_state_ok"] = publication_ok
    query = subprocess.run(
        [
            str(args.cli),
            "query",
            "--table",
            str(root),
            "--sql",
            'SELECT * FROM "table" ORDER BY trade_id',
            "--format",
            "jsonl",
            "--output",
            str(directory / "native-query.jsonl"),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    (directory / "native-query.stderr.log").write_text(query.stderr)
    assert query.returncode == 0, query.stderr
    native_rows = [
        json.loads(line)
        for line in (directory / "native-query.jsonl").read_text().splitlines()
    ]
    for row in native_rows:
        row["trade_time"] = dt.datetime.fromisoformat(row["trade_time"])
    assert native_rows == rows, "native reader returned different rows"
    log = root / "_timeseries_log"
    occupied = log / f"{version + 1:010}.json"
    result |= {
        "native_read_ok": True,
        "unpublished_next_log_bytes": occupied.stat().st_size
        if occupied.exists()
        else None,
        "current_tmp_exists": (log / "CURRENT.tmp").exists(),
    }
    try:
        events, _ = run(args.binary, root, "resume", "--commits", 1)
        assert events[0]["resume_from"] == rows[-1]["trade_id"] + 1
        recovered = verify_remote(root)
        assert recovered["version"] == version + 1
        assert recovered["rows"] == len(rows) + 50
        result |= {"can_resume": True, "after_resume": recovered}
    except AssertionError as error:
        result |= {"can_resume": False, "recovery_error": str(error)}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--binary", type=Path, default=Path("target/debug/examples/live_binance_writer")
    )
    parser.add_argument("--cli", type=Path, default=Path("target/debug/tstable"))
    parser.add_argument(
        "--seed", type=Path, help="existing verified 50-row version-2 table"
    )
    parser.add_argument("--vm-tools", type=Path)
    parser.add_argument("--qemu-image", default="localhost/ttf-writer-crash:local")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.binary = args.binary.resolve(strict=True)
    args.cli = args.cli.resolve(strict=True)
    with args.binary.open("rb") as binary_file:
        binary_hash = hashlib.file_digest(binary_file, "sha256").hexdigest()
    if args.vm_tools:
        args.vm_tools = args.vm_tools.resolve(strict=True)
        manifest = json.loads((args.vm_tools / "manifest.json").read_text())
        assert manifest["binary_sha256"] == binary_hash, (
            "rebuild VM tools for this binary"
        )
        assert (
            manifest["hook_sha256"]
            == hashlib.sha256(
                Path(__file__).with_name("writer_crash_hook.c").read_bytes()
            ).hexdigest()
        ), "rebuild VM tools for this crash shim"
    args.output.mkdir(parents=True, exist_ok=False)
    hook = args.output / "writer_crash_hook.so"
    subprocess.run(
        [
            "gcc",
            "-shared",
            "-fPIC",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-o",
            str(hook),
            str(Path(__file__).with_name("writer_crash_hook.c")),
        ],
        check=True,
    )
    baseline = args.output / "baseline"
    if args.seed:
        shutil.copytree(args.seed, baseline)
    else:
        run(args.binary, baseline, "baseline", "--commits", 1)
    initial = verify_remote(baseline)
    assert initial["version"] == 2 and initial["rows"] == 50
    report = {"binary_sha256": binary_hash, "baseline": initial, "cases": {}}
    for mode in ["process", "vm"] if args.vm_tools else ["process"]:
        for stage in STAGES:
            name = f"{mode}-{stage}"
            directory = args.output / name
            directory.mkdir()
            shutil.copytree(baseline, directory / "table")
            try:
                if mode == "process":
                    process_crash(args, directory, stage, hook)
                else:
                    vm_crash(args, directory, stage)
                result = verify(args, directory, stage, mode)
                result["passed"] = (
                    result["can_resume"] and result["publication_state_ok"]
                )
            except Exception as error:
                result = {"passed": False, "error": f"{type(error).__name__}: {error}"}
            report["cases"][name] = result
            (args.output / "report.json").write_text(
                json.dumps(report, indent=2) + "\n"
            )
            print(json.dumps({"case": name, **result}), flush=True)
    for stage in ("log-created", "before-current-rename", "after-current-rename"):
        name = f"competing-{stage}"
        directory = args.output / name
        directory.mkdir()
        shutil.copytree(baseline, directory / "table")
        try:
            process_crash(args, directory, stage, hook, with_competitor=True)
            result = verify(args, directory, stage, "process")
            result["competing_writer_rejected"] = True
            result["passed"] = result["can_resume"] and result["publication_state_ok"]
        except Exception as error:
            result = {"passed": False, "error": f"{type(error).__name__}: {error}"}
        report["cases"][name] = result
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"case": name, **result}), flush=True)
    if not all(case["passed"] for case in report["cases"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
