# src\shared\utils\system_tools.py

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
    """Provide uniform format for file/folder path address"""
    # Use DB_BASE_PATH for all persistent storage
    base_path = os.getenv("DB_BASE_PATH", "/app/data")
    
    # Map endpoint types to subdirectories
    endpoint_map = {
        "portfolio": f"exchanges/deribit/portfolio",
        "positions": f"exchanges/deribit/portfolio",
        "sub_accounts": f"exchanges/deribit/portfolio",
        "orders": f"exchanges/deribit/transactions",
        "myTrades": f"exchanges/deribit/transactions",
        "my_trades": f"exchanges/deribit/transactions",
        "ordBook": "market",
        "index": "market",
        # ... other endpoint mappings ...
    }
    
    # Determine subfolder based on endpoint
    sub_folder = endpoint_map.get(end_point, "general")
    
    # Construct full path
    path_components = [base_path, "databases", sub_folder]
    if marker:
        path_components.append(marker.lower())
    if status:
        path_components.append(status)
    
    my_path = os.path.join(*path_components)
    
    # Create directory if needed
    os.makedirs(my_path, exist_ok=True)
    os.chmod(my_path, 0o755)  # Ensure write permissions
    
    # Handle filename
    if ".env" in end_point or ".toml" in end_point:
        return os.path.join(my_path, end_point)
    return os.path.join(my_path, f"{marker.lower()}-{end_point}.pkl")


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
