# -*- coding: utf-8 -*-

import asyncio
import os
import signal
import sys
import orjson


def convert_size(size_bytes):
    """Convert bytes to human readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def get_file_size():
    """A decorator for TTL cache functionality.

    https://medium.com/@ryan_forrester_/getting-file-sizes-in-python-a-complete-guide-01293aaa68ef

    Args:

    """
    import os

    file_path = "error.log"
    size_bytes = convert_size(os.path.getsize(file_path))
    print(f"{file_path} file size: {size_bytes} bytes")


def get_platform() -> str:
    """
    Check current platform/operating system where app is running

    Args:
        None

    Returns:
        Current platform (str): linux, OS X, win

    References:
        https://www.webucator.com/article/how-to-check-the-operating-system-with-python/
        https://stackoverflow.com/questions/1325581/how-do-i-check-if-im-running-on-windows-in-python
    """

    platforms: dict = {
        "linux1": "linux",
        "linux2": "linux",
        "darwin": "OS X",
        "win32": "win",
    }

    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]


def provide_path_for_file(
    end_point: str,
    marker: str = None,
    status: str = None,
    method: str = None,
) -> str:
    """

    Provide uniform format for file/folder path address

    Args:
        marker(str): currency, instrument, other
        end_point(str): orders, myTrades
        status(str): open, closed
        method(str): web/manual, api/bot

    Returns:
        Path address(str): in linux/windows format
    """
    from pathlib import Path

    current_os = get_platform()

    # Set root equal to  current folder
    root: str = Path(".")

    exchange = None

    if bool(
        [
            o
            for o in [
                "portfolio",
                "positions",
                "sub_accounts",
            ]
            if (o in end_point)
        ]
    ):
        exchange: str = "deribit"
        sub_folder: str = f"databases/exchanges/{exchange}/portfolio"

    if bool(
        [
            o
            for o in [
                "orders",
                "myTrades",
                "my_trades",
            ]
            if (o in end_point)
        ]
    ):
        exchange: str = "deribit"
        sub_folder: str = f"databases/exchanges/{exchange}/transactions"

    if bool(
        [
            o
            for o in [
                "ordBook",
                "index",
                "instruments",
                "currencies",
                "ohlc",
                "futures_analysis",
                "ticker-all",
                "ticker_all",
                "ticker",
            ]
            if (o in end_point)
        ]
    ):
        sub_folder = "databases/market"
        exchange = "deribit"

    if bool(
        [
            o
            for o in [
                "openInterestHistorical",
                "openInterestHistorical",
                "openInterestAggregated",
            ]
            if (o in end_point)
        ]
    ):
        sub_folder = "databases/market"
        exchange = "general"

    if marker != None:
        file_name = f"{marker.lower()}-{end_point}"

        if status != None:
            file_name = f"{file_name}-{status}"

        if method != None:
            file_name = f"{file_name}-{method}"

    else:
        file_name = f"{end_point}"

    if ".env" in end_point:
        sub_folder = "configuration"

    if "config_strategies.toml" in end_point:
        sub_folder = "strategies"

    if "api_url_end_point.toml" in end_point:
        sub_folder = "transaction_management/binance"

    # to accomodate pytest env
    if "test.env" in end_point:
        sub_folder = "src/configuration"
        end_point = ".env"

    config_file = ".env" in file_name or ".toml" in file_name

    file_name = (f"{end_point}") if config_file else (f"{file_name}.pkl")

    # Combine root + folders
    my_path_linux: str = (
        root / sub_folder if exchange == None else root / sub_folder / exchange
    )
    my_path_win: str = (
        root / "src" / sub_folder
        if exchange == None
        else root / "src" / sub_folder / exchange
    )

    if "portfolio" in sub_folder or "transactions" in sub_folder:
        my_path_linux: str = (
            root / sub_folder if exchange == None else root / sub_folder
        )
        my_path_win: str = (
            root / "src" / sub_folder if exchange == None else root / "src" / sub_folder
        )

    # Create target Directory if it doesn't exist in linux
    if not os.path.exists(my_path_linux) and current_os == "linux":
        os.makedirs(my_path_linux)

    return (
        (my_path_linux / file_name)
        if get_platform() == "linux"
        else (my_path_win / file_name)
    )


def reading_from_db_pickle(
    end_point,
    instrument: str = None,
    status: str = None,
) -> float:
    """ """
    from utilities import pickling

    return pickling.read_data(
        provide_path_for_file(
            end_point,
            instrument,
            status,
        )
    )


def check_file_attributes(filepath: str) -> None:
    """

    Check file attributes

    Args:
        filepath (str): name of the file

    Returns:
        st_mode=Inode protection mode
        st_ino=Inode number
        st_dev=Device inode resides on
        st_nlink=Number of links to the inode
        st_uid=User id of the owner
        st_gid=Group id of the owner
        st_size=Size in bytes of a plain file; amount of data waiting on some special files
        st_atime=Time of last access
        st_mtime=Time of last modification
        st_ctime=
            Unix: is the time of the last metadata change
            Windows: is the creation time (see platform documentation for details).

    Reference:
        https://medium.com/@BetterEverything/automate-removal-of-old-files-in-python-2085381fdf51
    """
    return os.stat(filepath)


async def back_up_db(idle_time: int):

    from db_management.sqlite_management import back_up_db_sqlite

    extensions = ".bak"

    while True:

        folder_path = "databases/"

        try:
            file_list = os.listdir(folder_path)

            for currentFile in file_list:
                # log.error(currentFile)
                if ".bak" in currentFile:
                    os.remove(f"{folder_path}{currentFile}")
            await back_up_db_sqlite()

        except Exception as error:

            parse_error_message(error)

        await asyncio.sleep(idle_time)


class SignalHandler:
    """
    https://medium.com/@cziegler_99189/gracefully-shutting-down-async-multiprocesses-in-python-2223be384510
    """

    KEEP_PROCESSING = True

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):

        print(f"signum {signum} frame {frame}")
        print("Exiting gracefully")

        self.KEEP_PROCESSING = False


def handle_ctrl_c() -> None:
    """
    https://stackoverflow.com/questions/67866293/how-to-subscribe-to-multiple-websocket-streams-using-muiltiprocessing
    """
    signal.signal(signal.SIGINT, sys.exit(0))


def get_config_tomli(config_path) -> list:
    """ """

    import tomli

    if os.path.exists(config_path):

        with open(config_path, "rb") as handle:
            read = tomli.load(handle)
            return read
