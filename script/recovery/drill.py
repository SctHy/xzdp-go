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
    db = folder / "mysql.sql"
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
        "mysql",
        "sh",
        "-ec",
        'MYSQL_PWD="$MYSQL_PASSWORD" exec mysql -N -uxzdp xzdp -e "SELECT id,user_id,voucher_id FROM tb_voucher_order ORDER BY id LIMIT 1"',
    )
    if order:
        witness["order"] = [int(n) for n in order.split()]
    (folder / "witness.json").write_text(json.dumps(witness))
    with db.open("wb") as stream:
        run(
            "docker",
            "compose",
            "exec",
            "-T",
            "mysql",
            "sh",
            "-ec",
            'MYSQL_PWD="$MYSQL_PASSWORD" exec mysqldump -uxzdp --single-transaction '
            "--quick --no-tablespaces --set-gtid-purged=OFF --triggers xzdp",
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
    for name in ("mysql.sql", "dump.rdb", "witness.json"):
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
            "MYSQL_ALLOW_EMPTY_PASSWORD=yes",
            "-e",
            "MYSQL_DATABASE=xzdp",
            "-e",
            "MYSQL_USER=xzdp",
            "-e",
            "MYSQL_PASSWORD=" + tag,
            "mysql:8.4",
            "--innodb-buffer-pool-size=96M",
        )
        run("docker", "start", db, stdout=subprocess.DEVNULL)
        sql = ("docker", "exec", "-i", db, "mysql", "-uroot", "-N", "xzdp")
        for _ in range(120):
            if (
                subprocess.run(
                    [*sql, "--protocol=tcp", "-e", "SELECT 1"], capture_output=True
                ).returncode
                == 0
            ):
                break
            time.sleep(1)
        with (folder / "mysql.sql").open("rb") as stream:
            run(*sql, stdin=stream)
        print(
            "SQL restored (shops, orders, migrations):",
            output(
                *sql,
                "-e",
                "SELECT (SELECT COUNT(*) FROM tb_shop),(SELECT COUNT(*) FROM tb_voucher_order),(SELECT COUNT(*) FROM schema_migrations)",
            ),
        )
        if "order" in witness:
            oid, user, voucher = witness["order"]
            assert (
                output(
                    *sql,
                    "-e",
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
