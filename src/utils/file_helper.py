import os


def get_absolute_root_path():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_folder_path() -> str:
    root_path = get_absolute_root_path()
    relative_path = input("Enter the relative path from the root to the folder with your files: ").strip()
    full_path = os.path.join(root_path, relative_path)
    if not os.path.isdir(full_path):
        raise NotADirectoryError(f"Provided path is not a valid directory: {full_path}")
    return full_path

