import os
from datetime import datetime

def exists_file(file_path: str) -> bool:
    """
    Check if the file exists at the given file path.

    Args:
        file_path (str): The path of the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.exists(file_path)

def is_allowed_file_extension(file_path: str, allowed_extensions: list|str = '*') -> bool:
    """
    Validate the file extension of the given file path.

    Args:
        file_path (str): The path of the file to validate.
        allowed_extensions (list | str): A list of allowed file extensions or '*' to allow all extensions.

    Returns:
        bool: True if the file extension is valid, False otherwise.
    """

    _, ext = os.path.splitext(file_path)
    ext = ext.lower().split('.')[-1]

    if type(allowed_extensions) == str and allowed_extensions == '*':
        return True
    else:
        allowed_extensions = [e.lower().split('.')[-1] for e in allowed_extensions]

    return ext.lower() in allowed_extensions

def get_date_suffix_in_filename(file_path: str) -> str:
    """
    Append a date suffix to the file name before the extension.

    Args:
        file_path (str): The original file path.

    Returns:
        str: The modified file path with the date suffix.
    """
    date = datetime.today()
    date_str = date.strftime("%Y%m%d")

    file_name, ext = os.path.splitext(file_path)
    file_name = file_name.split('/')[-1]

    return f"{file_name}_{date_str}{ext}"