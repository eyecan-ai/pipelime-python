import datetime as dt
import typing as t
from enum import Enum

from loguru import logger
from pydantic import ByteSize, Field

from pipelime.piper import PipelimeCommand


class SortBy(Enum):
    NAME = "name"
    SIZE = "size"
    TIME = "time"


date_or_time_t = t.Union[dt.datetime, dt.date, dt.time]


class TempCommand(PipelimeCommand, title="tmp"):
    """Show pipelime temporary files and free space.

    Examples:
        Show disk usage: ``pipelime tmp``

        Clear all temporary folders with no confirmation: ``pipelime tmp +all +f``

        Clear folders with size greater than 1GB: ``pipelime tmp +m 1GB``

        Clear all folders (date-time and time period as in ISO-8601):
          - accessed after a date-time:   ``pipelime tmp +a 2023-04-05T12:30``
          - accessed before a date-time:  ``pipelime tmp +b 2023-04-05T12:30``
          - recently accessed:            ``pipelime tmp +da P1DT10H30M5S``
          - except the recently accessed: ``pipelime tmp +db P1DT10H30M5S``

        When multiple options are specified, the intersection of times is used.
    """

    force: bool = Field(False, alias="f", description="Do not ask for confirmation")

    after: t.Optional[date_or_time_t] = Field(
        None, alias="a", description="Clear folders after this date-time"
    )
    before: t.Optional[date_or_time_t] = Field(
        None, alias="b", description="Clear folders before this date-time"
    )
    delta_after: t.Optional[dt.timedelta] = Field(
        None, alias="da", description="Clear folders after ``now - delta_after``"
    )
    delta_before: t.Optional[dt.timedelta] = Field(
        None, alias="db", description="Clear folders before ``now - delta_before``"
    )

    min_size: t.Optional[ByteSize] = Field(
        None, alias="m", description="Clear folders with size greater than this"
    )
    max_size: t.Optional[ByteSize] = Field(
        None, alias="M", description="Clear folders with size lower than this"
    )

    name: t.Optional[str] = Field(
        None, alias="n", description="Clear folders including this name"
    )

    user: t.Union[str, t.Sequence[str], None] = Field(
        None,
        alias="u",
        description="Clear folders created by given users (defaults to current user)",
    )
    all_users: bool = Field(
        False,
        alias="au",
        description="Clear folders created by any user (overrides 'user' option)",
    )

    all: bool = Field(False, description="Clear all temporary folders")

    sort_by: SortBy = Field(
        SortBy.TIME, alias="s", description="Sort by name, size or time"
    )

    def run(self):
        import shutil

        from rich import print as rprint
        from rich.prompt import Confirm

        if (
            self.after is None
            and self.before is None
            and self.delta_after is None
            and self.delta_before is None
            and self.min_size is None
            and self.max_size is None
            and self.name is None
            and self.user is None
            and not self.all_users
            and not self.all
        ):
            self.show_usage()
            return

        to_delete = self._folders_to_delete()

        if not self.force and to_delete:  # pragma: no cover
            table, total_size = self._paths_table(to_delete)
            table.title = f"\nTotal clean up: {self._human_size(total_size)}"
            rprint(table)
            print("")
            if not Confirm.ask(
                "Do you want to PERMANENTLY delete these folders?", default=False
            ):
                return

        for p in to_delete:
            logger.debug(f"Deleting {p}")
            shutil.rmtree(
                p,
                onerror=lambda fn, pt, exc: logger.warning(
                    f"Error deleting {pt}: {exc}"
                ),
            )

    def show_usage(self):
        from rich import print as rprint
        from rich.markup import escape

        from pipelime.choixe.utils.io import PipelimeTmp
        from pipelime.cli.pretty_print import print_info

        print_info(f"Temporary folder: {PipelimeTmp.base_dir()}")

        for user, paths in PipelimeTmp.get_temp_dirs().items():
            table, total_size = self._paths_table(paths)
            table.title = f"\n{escape(user)} ({self._human_size(total_size)})"
            rprint(table)
            print("")

    def _folders_to_delete(self):
        from pathlib import Path

        from pipelime.choixe.utils.io import PipelimeTmp

        if self.all:
            return [p for ps in PipelimeTmp.get_temp_dirs().values() for p in ps]

        delta_after_t = (
            dt.datetime.now() - self.delta_after if self.delta_after else None
        )
        delta_before_t = (
            dt.datetime.now() - self.delta_before if self.delta_before else None
        )

        delta_after_t = self._as_ts(delta_after_t, inf_limit=True)
        delta_before_t = self._as_ts(delta_before_t, inf_limit=False)

        after = self._as_ts(self.after, inf_limit=True)
        before = self._as_ts(self.before, inf_limit=False)

        min_time = max(delta_after_t, after)
        max_time = min(delta_before_t, before)

        min_size = self.min_size or 0
        max_size = self.max_size or float("inf")

        name = Path(self.name or "").stem

        users = (
            [PipelimeTmp.current_user()]
            if not self.user
            else ([self.user] if isinstance(self.user, str) else self.user)
        )

        to_delete: t.List[str] = []
        for u, ps in PipelimeTmp.get_temp_dirs().items():
            if not self.all_users and u not in users:
                continue

            for p in ps:
                if name not in Path(p).stem:
                    continue
                tm = self._get_time(p)
                if tm < min_time or tm > max_time:
                    continue
                s = self._get_size(p)
                if s < min_size or s > max_size:
                    continue
                to_delete.append(p)
        return to_delete

    def _paths_table(self, paths: t.Sequence[str]):
        import time
        from pathlib import Path

        from rich.markup import escape
        from rich.table import Column, Table

        table = Table(
            Column("Path", justify="center", overflow="fold"),
            Column("Last Modification Time", justify="center", overflow="fold"),
            Column("Size", justify="center", overflow="fold"),
            padding=(0, 5),
        )

        total_size = 0
        rows = []

        for p in paths:
            size = self._get_size(p)
            total_size += size
            rows.append(
                (
                    Path(p),
                    self._get_time(p),
                    size,
                )
            )

        rows.sort(
            key=lambda x: x[1]
            if self.sort_by is SortBy.TIME
            else (x[2] if self.sort_by is SortBy.SIZE else x[0].stem),
            reverse=self.sort_by is SortBy.SIZE,
        )

        for r in rows:
            table.add_row(
                f"[link={r[0].as_uri()}]{escape(r[0].stem)}[/link]",
                escape(time.ctime(r[1])),
                self._human_size(r[2]),
            )

        return table, total_size

    def _get_size(self, root_path: str):
        return self._traverse(root_path, self._size_update, 0)

    def _get_time(self, root_path: str):
        return self._traverse(root_path, self._time_update, 0)

    def _traverse(self, root_path: str, update_fn: t.Callable, total_init):
        import os

        total = total_init
        seen = set()
        for dirpath, subdirs, filenames in os.walk(root_path):
            if not subdirs and not filenames:
                filenames = [dirpath]

            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    stat = os.stat(fp)
                except OSError:  # pragma: no cover
                    continue

                if stat.st_ino in seen:
                    continue

                seen.add(stat.st_ino)
                total = update_fn(fp, stat, total)

        return total

    def _size_update(self, fp: str, stat, total):
        return total + stat.st_size

    def _time_update(self, fp: str, stat, total):
        return max(total, stat.st_mtime)

    def _human_size(self, size, u=[" bytes", "KB", "MB", "GB", "TB", "PB", "EB"]):
        return str(size) + u[0] if size < 1024 else self._human_size(size >> 10, u[1:])

    def _as_ts(self, dttm: t.Optional[date_or_time_t], inf_limit: bool):
        if dttm:
            try:
                if isinstance(dttm, dt.date):
                    dttm = dt.datetime.combine(
                        date=dttm,
                        time=dt.time(23, 59, 59, 999999)
                        if inf_limit
                        else dt.time(0, 0, 0, 0),
                    )
                elif isinstance(dttm, dt.time):
                    dttm = dt.datetime.combine(date=dt.date.today(), time=dttm)
                return dttm.timestamp()
            except OverflowError:
                pass
        return 0 if inf_limit else float("inf")
