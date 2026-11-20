"""Back up the Compose stores; restore into disposable, network-isolated containers."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def output(*args):
    return subprocess.check_output(args, text=True).strip()


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def backup(folder):
    folder.mkdir(mode=0o700)
    db = folder / "postgres.dump"
    rdb = folder / "dump.rdb"
    cli = ("docker", "compose", "exec", "-T", "redis", "redis-cli", "--raw")
    keys = output(*cli, "--scan", "--pattern", "blog:liked:*").splitlines()
    witness = {}
    if keys:
        witness["like"] = [keys[0], output(*cli, "ZRANGE", keys[0], "0", "0")]
    order = output(
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-XAt",
        "-U",
        "xzdp",
        "-d",
        "xzdp",
        "-c",
        "SELECT id,user_id,voucher_id FROM tb_voucher_order ORDER BY id LIMIT 1",
    )
    if order:
        witness["order"] = [int(n) for n in order.replace("|", " ").split()]
    (folder / "witness.json").write_text(json.dumps(witness))
    with db.open("wb") as stream:
        run(
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "pg_dump",
            "-Fc",
            "--no-owner",
            "--no-privileges",
            "-U",
            "xzdp",
            "xzdp",
            stdout=stream,
        )
    with rdb.open("wb") as stream:
        run(
            "docker",
            "compose",
            "exec",
            "-T",
            "redis",
            "redis-cli",
            "--rdb",
            "-",
            stdout=stream,
        )
    (folder / "checksums.json").write_text(
        json.dumps({p.name: checksum(p) for p in (db, rdb, folder / "witness.json")})
    )


def restore(folder):
    witness = json.loads((folder / "witness.json").read_text())
    for name in ("postgres.dump", "dump.rdb", "witness.json"):
        if (
            checksum(folder / name)
            != json.loads((folder / "checksums.json").read_text())[name]
        ):
            raise ValueError("backup checksum mismatch: " + name)
    tag = secrets.token_hex(6)
    db, cache = "xzdp-drill-db-" + tag, "xzdp-drill-redis-" + tag
    created = []
    try:

        def create(name, *args):
            run(
                "docker",
                "create",
                "--name",
                name,
                "--network",
                "none",
                *args,
                stdout=subprocess.DEVNULL,
            )
            created.append(name)

        create(
            db,
            "--memory",
            "512m",
            "-e",
            "POSTGRES_PASSWORD=" + tag,
            "-e",
            "POSTGRES_DB=xzdp",
            "-e",
            "POSTGRES_USER=xzdp",
            "imresamu/postgis:17-3.5",
            "postgres",
            "-c",
            "shared_buffers=96MB",
        )
        run("docker", "start", db, stdout=subprocess.DEVNULL)
        sql = (
            "docker",
            "exec",
            "-i",
            db,
            "psql",
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "xzdp",
            "-d",
            "xzdp",
        )
        for _ in range(120):
            if (
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        db,
                        "pg_isready",
                        "-h",
                        "127.0.0.1",
                        "-U",
                        "xzdp",
                    ],
                    capture_output=True,
                ).returncode
                == 0
            ):
                break
            time.sleep(1)
        run(
            "docker",
            "exec",
            db,
            "createdb",
            "-U",
            "xzdp",
            "-T",
            "template0",
            "restored",
        )
        sql = (*sql[:-1], "restored")
        with (folder / "postgres.dump").open("rb") as stream:
            run(
                "docker",
                "exec",
                "-i",
                db,
                "pg_restore",
                "--exit-on-error",
                "--no-owner",
                "-U",
                "xzdp",
                "-d",
                "restored",
                stdin=stream,
            )
        print(
            "SQL restored (shops, orders, migrations):",
            output(
                *sql,
                "-c",
                "SELECT (SELECT COUNT(*) FROM tb_shop),(SELECT COUNT(*) FROM tb_voucher_order),(SELECT COUNT(*) FROM schema_migrations)",
            ),
        )
        if "order" in witness:
            oid, user, voucher = witness["order"]
            assert (
                output(
                    *sql,
                    "-c",
                    f"SELECT COUNT(*) FROM tb_voucher_order WHERE id={int(oid)} AND user_id={int(user)} AND voucher_id={int(voucher)}",
                )
                == "1"
            ), "missing original order"
        create(
            cache,
            "--memory",
            "128m",
            "redis:7.4-alpine",
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
        )
        cli = ("docker", "exec", cache, "redis-cli", "--raw")

        def start_redis():
            run("docker", "start", cache, stdout=subprocess.DEVNULL)
            for _ in range(100):
                if (
                    subprocess.run([*cli, "PING"], capture_output=True).stdout.strip()
                    == b"PONG"
                ):
                    return
                time.sleep(0.1)
            raise RuntimeError("restore Redis startup timeout")

        start_redis()
        assert output(*cli, "DBSIZE") == "0", "expected fresh isolated Redis"
        run("docker", "stop", cache, stdout=subprocess.DEVNULL)
        run("docker", "cp", str(folder / "dump.rdb"), cache + ":/data/dump.rdb")
        start_redis()
        size = int(output(*cli, "DBSIZE"))
        print("Redis restored keys:", size)
        # Check a concrete business relationship, not just that Redis accepted the file.
        if "like" in witness:
            key, member = witness["like"]
            assert output(
                *cli, "ZSCORE", key, member
            ), "missing original like membership"
            print("Verified original blog and liker pair against source witness")
        if not size:
            print("Snapshot contained no surviving keys")
    finally:
        for name in reversed(created):
            run("docker", "rm", "-fv", name, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", choices=["backup", "verify", "drill"], default="drill", nargs="?"
    )
    parser.add_argument("directory", type=Path, nargs="?")
    args = parser.parse_args()
    os.umask(0o077)
    if args.mode == "drill":
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp) / "snapshot"
            backup(folder)
            restore(folder)
    elif args.directory is None:
        parser.error("backup and verify require a directory")
    elif args.mode == "backup":
        backup(args.directory)
    else:
        restore(args.directory)
