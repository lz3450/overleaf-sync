import os
import hashlib
import argparse


def compute_md5(file_path):
    """Compute MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None


def get_files_with_md5(folder_path):
    """Get a dictionary of files and their MD5 hashes for a given folder, excluding .git folder."""
    files_md5 = {}
    for root, _, files in os.walk(folder_path):
        # Skip the .git folder
        if ".git" in root.split(os.sep):
            continue
        if ".overleaf-sync" in root.split(os.sep):
            continue
        if "_minted-main" in root.split(os.sep):
            continue
        for file in files:
            file_path = os.path.join(root, file)
            md5_hash = compute_md5(file_path)
            relative_path = os.path.relpath(file_path, folder_path)
            if md5_hash:
                files_md5[relative_path] = md5_hash
    return files_md5


def compare_folders(folder1, folder2):
    """Compare files and MD5 hashes between two folders."""
    folder1_files = get_files_with_md5(folder1)
    folder2_files = get_files_with_md5(folder2)

    folder1_set = set(folder1_files.keys())
    folder2_set = set(folder2_files.keys())

    common_files = folder1_set.intersection(folder2_set)
    only_in_folder1 = folder1_set - folder2_set
    only_in_folder2 = folder2_set - folder1_set

    modified_files = [file for file in common_files if folder1_files[file] != folder2_files[file]]

    # Display results
    if only_in_folder1:
        print(f"Files only in {folder1}:")
        for file in only_in_folder1:
            print(f"  {file}")
    else:
        print(f"No unique files found in {folder1}.")

    if only_in_folder2:
        print(f"\nFiles only in {folder2}:")
        for file in only_in_folder2:
            print(f"  {file}")
    else:
        print(f"\nNo unique files found in {folder2}.")

    if modified_files:
        print("\nModified files:")
        for file in modified_files:
            print(f"  {file}")
    else:
        print("\nNo modified files found.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare files and MD5 hashes between two folders.")
    parser.add_argument("folder1", type=str, help="Path of the first folder")
    parser.add_argument("folder2", type=str, help="Path of the second folder")

    args = parser.parse_args()

    folder1 = args.folder1
    folder2 = args.folder2

    if os.path.isdir(folder1) and os.path.isdir(folder2):
        compare_folders(folder1, folder2)
    else:
        print("Please provide valid folder paths.")
