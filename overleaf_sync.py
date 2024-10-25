#!/usr/bin/env python3.12

################################################################
### Name: overleaf-sync.py
### Description: Overleaf Project Sync Tool
### Author: KZL
################################################################

from __future__ import annotations

import os
import subprocess
import argparse
import logging
import requests
import json
from bs4 import BeautifulSoup
import zipfile
import websocket

from time import sleep
from datetime import datetime
from enum import Enum


OVERLEAF_URL = "https://overleaf.s3lab.io"
LOGIN_URL = f"{OVERLEAF_URL}/login"
PROJECTS_URL = f"{OVERLEAF_URL}/project"

WORKING_DIR_NAME = ".overleaf-sync"
WORKING_DIR = os.path.join(os.getcwd(), WORKING_DIR_NAME)
CONFIG_FILE = os.path.join(WORKING_DIR, "config.json")
ZIP_FILE = os.path.join(WORKING_DIR, "latex.zip")
LATEX_PROJECT_DIR = os.path.abspath(os.path.join(WORKING_DIR, ".."))
PROJECT_UPDATES_FILE = os.path.join(WORKING_DIR, "updates.json")
LATEST_COMMIT_FILE = os.path.join(WORKING_DIR, "latest_commit.txt")
REMOTE_VERSION_FILE = os.path.join(WORKING_DIR, "remote_version.txt")
IDS_FILE = os.path.join(WORKING_DIR, "file_ids.json")
LOG_DIR = ".overleaf-sync-logs"

LOGGER = logging.getLogger(__name__)
LOGGER_FORMATTER = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

REQUEST_INTERVAL = 9


def setup_logger(logger: logging.Logger, debug: bool) -> None:
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    ch.setFormatter(LOGGER_FORMATTER)
    logger.addHandler(ch)


def setup_file_logger(logger: logging.Logger) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, ".gitignore"), "w") as f:
        f.write("*")
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"{now}.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(LOGGER_FORMATTER)
    logger.addHandler(fh)


class ErrorNumber(Enum):
    EN_OK = 0
    EN_WKDIR_NOT_EXIST = 1
    EN_CONFIG_NOT_EXIST = 2
    EN_PULL_ERROR = 3
    EN_PUSH_ERROR = 4
    EN_NOT_INITIALIZED = 5


class OverleafProject:
    def __init__(self, username: str, password: str, project_id: str) -> None:
        self.username = username
        self.password = password
        self._project_id = project_id
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
            }
        )
        self._logged_in = False
        self._root_folder_id = None
        self._csrf_token = None
        self._original_file_ids = None
        self._indexed_file_ids = None

    @property
    def project_url(self) -> str:
        return f"{PROJECTS_URL}/{self._project_id}"

    @property
    def download_url(self) -> str:
        return f"{PROJECTS_URL}/{self._project_id}/download/zip"

    @property
    def upload_url(self) -> str:
        return f"{PROJECTS_URL}/{self._project_id}/upload"

    @property
    def updates_url(self) -> str:
        return f"{PROJECTS_URL}/{self._project_id}/updates"

    @property
    def root_folder_id(self) -> str:
        # return self._root_folder_id if self._root_folder_id else hex(int(self._project_id, 16) - 1)[2:].lower()
        if not self._root_folder_id:
            self._root_folder_id = self.original_file_ids["_id"]
        return self._root_folder_id

    def login(self) -> None:
        if self._logged_in:
            return
        LOGGER.info("Logging in to Overleaf...")
        response = self._session.get(LOGIN_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        csrf_token: str
        csrf_token = soup.find("input", {"name": "_csrf"})["value"]  # type: ignore
        if not csrf_token:
            raise ValueError("Failed to fetch CSRF token.")
        payload = {"email": self.username, "password": self.password, "_csrf": csrf_token}
        response = self._session.post(LOGIN_URL, data=payload)
        response.raise_for_status()
        self._logged_in = True
        LOGGER.info("Login successful.")

    @property
    def csrf_token(self) -> str:
        if self._csrf_token:
            return self._csrf_token
        response = self._session.get(self.project_url)
        soup = BeautifulSoup(response.text, "html.parser")
        _csrf_token: str
        _csrf_token = soup.find("meta", {"name": "ol-csrfToken"})["content"]  # type: ignore
        if not _csrf_token:
            raise ValueError("Failed to fetch CSRF token.")
        self._csrf_token = _csrf_token
        return self._csrf_token

    def download(self, version: int | None = None) -> None:
        url = f"{self.project_url}/version/{version}/zip" if version else self.download_url
        LOGGER.info("Downloading project ZIP from url: %s...", url)
        response = self._session.get(url)
        response.raise_for_status()
        with open(ZIP_FILE, "wb") as f:
            f.write(response.content)
        LOGGER.debug("Project ZIP downloaded as %s", ZIP_FILE)

    def unzip(self, file_list: list | None = None, keep=True) -> None:
        """
        Unzip the downloaded ZIP file to the LaTeX project directory.
        `file_list`: List of files to extract. If `None`, extract all files.
        `keep`: Keep remotely deleted files.
        """
        LOGGER.info("Unzipping file %s to directory %s...", ZIP_FILE, LATEX_PROJECT_DIR)
        with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
            # TODO: not tested
            if file_list:
                for file in zip_ref.filelist:
                    LOGGER.info("Extracting %s...", file.filename)
                    zip_ref.extract(file, LATEX_PROJECT_DIR)
            else:
                zip_ref.extractall(LATEX_PROJECT_DIR)
            if not keep:
                return
            for file in self.managed_files:
                if file and file not in (zip_info.filename for zip_info in zip_ref.filelist):
                    LOGGER.info("Deleting local %s...", file)
                    os.remove(os.path.join(LATEX_PROJECT_DIR, file))

    def _upload(self, file_path: str, dry_run=False) -> None:
        LOGGER.info("Uploading %s...", file_path)
        if dry_run:
            return

        relative_path = "null" if not os.path.dirname(file_path) else file_path
        file_name = os.path.basename(file_path)

        headers = {
            "Accept": "*/*",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        params = {"folder_id": self.root_folder_id}
        data = {"relativePath": f"{relative_path}", "name": file_name, "type": "application/octet-stream"}
        qqfile = open(os.path.join(LATEX_PROJECT_DIR, file_path), "rb")
        files = {"qqfile": (file_name, qqfile, "application/octet-stream")}
        response = self._session.post(self.upload_url, headers=headers, params=params, data=data, files=files)
        qqfile.close()
        response.raise_for_status()

    def _create_folder(self, path: str, dry_run=False) -> None:
        LOGGER.info("Creating folder %s...", path)
        if dry_run:
            return
        parent_folder_id, type = self._find_id_type(os.path.dirname(path))
        assert type == "folder"

        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        data = {"name": os.path.basename(path), "parent_folder_id": parent_folder_id}
        response = self._session.post(f"{self.project_url}/folder", headers=headers, data=data)
        response.raise_for_status()

    def _delete(self, path: str, dry_run=False) -> None:
        id, type = self._find_id_type(path)
        LOGGER.info("Deleting %s: %s %s", type, path, id)

        if type == "folder":
            if input(f"Are you sure you want to delete folder {path}? (y/n): ").strip().lower() not in ["y", "yes"]:
                LOGGER.info("Operation cancelled.")
                return
        if dry_run:
            return

        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        if type not in ["file", "doc", "folder"]:
            raise ValueError(f"Invalid type: {type}")
        response = self._session.delete(f"{self.project_url}/{type}/{id}", headers=headers)
        response.raise_for_status()

    @property
    def original_file_ids(self) -> dict:
        if self._original_file_ids:
            return self._original_file_ids

        LOGGER.info("Fetching document IDs from Overleaf project %s...", self._project_id)
        response = self._session.get(f"{OVERLEAF_URL}/socket.io/1/?projectId={self._project_id}")
        ws_id = response.text.split(":")[0]
        ws = websocket.create_connection(
            f"wss://overleaf.s3lab.io/socket.io/1/websocket/{ws_id}?projectId={self._project_id}"
        )
        data: str
        while True:
            data = ws.recv()  # type: ignore
            if data.startswith("5:::"):
                break
        ws.close()
        ids: dict
        ids = json.loads(data[4:])["args"][0]["project"]["rootFolder"][0]

        with open(IDS_FILE, "w") as f:
            json.dump(ids, f)

        self._original_file_ids = ids
        return self._original_file_ids

    @property
    def indexed_file_ids(self) -> dict[str, dict[str, str]]:
        if self._indexed_file_ids:
            return self._indexed_file_ids

        ids = {"folders": {}, "fileRefs": {}, "docs": {}}

        def _restructure(folder_data: dict, current_folder="") -> None:
            if folder_data["_id"] != self.root_folder_id:
                current_folder = f'{current_folder}{folder_data["name"]}/'
            for folder in folder_data.get("folders", []):
                ids["folders"][f'{current_folder}{folder["name"]}'] = folder["_id"]
                _restructure(folder, current_folder)
            for doc in folder_data.get("docs", []):
                ids["docs"][f'{current_folder}{doc["name"]}'] = doc["_id"]
            for file_ref in folder_data.get("fileRefs", []):
                ids["fileRefs"][f'{current_folder}{file_ref["name"]}'] = file_ref["_id"]

        _restructure(self.original_file_ids)

        self._indexed_file_ids = ids
        return self._indexed_file_ids

    def _find_empty_folder(self) -> list[str]:
        empty_folders: list[str] = []

        def _traverse_folders(folder: dict, parent_folder="") -> None:
            if not folder.get("folders") and not folder.get("fileRefs") and not folder.get("docs"):
                empty_folders.append(f'{parent_folder}/{folder["name"]}')
            else:
                all_sub_folders_empty = True
                for sub_folder in folder.get("folders", []):
                    _traverse_folders(sub_folder, f'{parent_folder}/{folder["name"]}')
                    if sub_folder["_id"] not in empty_folders:
                        all_sub_folders_empty = False
                if all_sub_folders_empty and not folder.get("fileRefs") and not folder.get("docs"):
                    empty_folders.append(f'{parent_folder}/{folder["name"]}')

        # Start checking from the root level folders
        for folder in self.original_file_ids.get("folders", []):
            _traverse_folders(folder)

        return [p.lstrip("/") for p in empty_folders]

    def _find_id_type(self, path: str) -> tuple[str, str]:
        LOGGER.info("Finding id for `%s`...", path)
        ids = self.indexed_file_ids
        if path in ids["fileRefs"]:
            return ids["fileRefs"][path], "file"
        elif path in ids["docs"]:
            return ids["docs"][path], "doc"
        elif path in ids["folders"]:
            return ids["folders"][path], "folder"
        else:
            raise ValueError(f"No id found for `{path}`")

    @property
    def history(self) -> str:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        }
        LOGGER.debug("Fetching project updates from %s...", self.updates_url)
        response = self._session.get(self.updates_url, headers=headers)
        response.raise_for_status()
        with open(PROJECT_UPDATES_FILE, "w") as f:
            f.write(response.text)
        return response.text

    @property
    def remote_version(self) -> int:
        return json.loads(self.history)["updates"][0]["toV"]

    def _get_changed_files(self) -> str:
        if not os.path.exists(LATEST_COMMIT_FILE):
            raise FileNotFoundError(f"File {LATEST_COMMIT_FILE} does not exist.")
        with open(LATEST_COMMIT_FILE, "r") as f:
            latest_commit_id = f.read().strip()
        LOGGER.info("Latest commit ID: %s", latest_commit_id)

        return subprocess.run(
            ["git", "-C", LATEX_PROJECT_DIR, "diff", "--name-status", latest_commit_id], capture_output=True, text=True
        ).stdout.strip()

    @property
    def latest_commit_id(self) -> str:
        return subprocess.run(
            ["git", "-C", LATEX_PROJECT_DIR, "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()

    @property
    def is_empty_git_repo(self) -> bool:
        return (
            subprocess.run(
                ["git", "-C", LATEX_PROJECT_DIR, "rev-list", "-n", "1", "--all"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            == ""
        )

    @property
    def is_local_clean(self) -> bool:
        return (
            subprocess.run(
                ["git", "-C", LATEX_PROJECT_DIR, "status", "--porcelain"], capture_output=True, text=True, check=True
            ).stdout.strip()
            == ""
        )

    @property
    def is_remote_updated(self) -> bool:
        if not os.path.exists(REMOTE_VERSION_FILE):
            LOGGER.info("File %s does not exist, treating as updated.", REMOTE_VERSION_FILE)
            return False
        with open(REMOTE_VERSION_FILE, "r") as f:
            recorded_remote_version = json.load(f)
        remote_version = int(self.remote_version)
        LOGGER.info("Recorded remote version: %s", recorded_remote_version)
        LOGGER.info("Remote version: %s", remote_version)
        assert remote_version >= recorded_remote_version
        return remote_version > recorded_remote_version

    @property
    def managed_files(self) -> list[str]:
        return (
            subprocess.run(["git", "-C", LATEX_PROJECT_DIR, "ls-files"], capture_output=True, text=True, check=True)
            .stdout.strip()
            .split("\n")
        )

    def pull(self, keep=True) -> None:
        if not os.path.exists(os.path.join(LATEX_PROJECT_DIR, ".git")):
            LOGGER.info("Initializing git repository in %s...", LATEX_PROJECT_DIR)
            subprocess.run(["git", "init", LATEX_PROJECT_DIR], check=True)
            LOGGER.info("Git repository initialized.")
        if not self.is_remote_updated:
            LOGGER.info("No updates available.")
            return
        if not self.is_local_clean:
            LOGGER.error("Cannot pull changes to a dirty repository.")
            exit(ErrorNumber.EN_PULL_ERROR.value)

        self._download()
        self._unzip(keep=keep)
        remote_version = self.remote_version
        subprocess.run(["git", "-C", LATEX_PROJECT_DIR, "add", "."], check=True)
        if self.is_local_clean:
            commit_message = subprocess.run(
                ["git", "-C", LATEX_PROJECT_DIR, "log", "-1", "--pretty=%B"], capture_output=True, text=True, check=True
            ).stdout.strip()
            subprocess.run(
                [
                    "git",
                    "-C",
                    LATEX_PROJECT_DIR,
                    "commit",
                    "--amend",
                    "-m",
                    f"Overleaf version: {remote_version}\n{commit_message}",
                ],
                check=True,
            )
        else:
            subprocess.run(
                ["git", "-C", LATEX_PROJECT_DIR, "commit", "-m", f"Overleaf version: {remote_version}"], check=True
            )
        with open(REMOTE_VERSION_FILE, "w") as f:
            f.write(str(remote_version))
        with open(LATEST_COMMIT_FILE, "w") as f:
            LOGGER.info("Writing latest commit ID to %s: %s", LATEST_COMMIT_FILE, self.latest_commit_id)
            f.write(self.latest_commit_id)

    def push(self, force=False, prune=False, dry_run=False) -> None:
        if not os.path.exists(os.path.join(LATEX_PROJECT_DIR, ".git")):
            LOGGER.error("LaTeX project directory `%s` does not initialized.", LATEX_PROJECT_DIR)
            exit(ErrorNumber.EN_PUSH_ERROR.value)
        if not self.is_local_clean:
            LOGGER.error("Cannot push changes from a dirty repository.")
            exit(ErrorNumber.EN_PUSH_ERROR.value)
        if self.is_remote_updated:
            LOGGER.error("Cannot push changes to a updated remote repository.")
            exit(ErrorNumber.EN_PUSH_ERROR.value)

        if force:
            LOGGER.info("Force pushing changes to Overleaf project...")

            if dry_run:
                sleep_time = 0
            else:
                sleep_time = 9 if len(self.managed_files) >= 200 else 1
            for file_path in self.managed_files:
                self._upload(file_path, dry_run)
                sleep(sleep_time)
            return

        delete_list: list[str] = []
        upload_list: list[str] = []
        for line in self._get_changed_files().split("\n"):
            if not line:
                continue
            LOGGER.info("status: %s", line)
            row = line.split("\t")
            status = row[0]
            match status:
                case "D":
                    assert len(row) == 2
                    file_path = row[1]
                    delete_list.append(file_path)
                case "M":
                    assert len(row) == 2
                    file_path = row[1]
                    upload_list.append(file_path)
                case "R100":
                    assert len(row) == 3
                    old_file_path = row[1]
                    new_file_path = row[2]
                    delete_list.append(old_file_path)
                    upload_list.append(new_file_path)
                case _:
                    raise ValueError(f"Unsupported status: {status}")
        for file_path in delete_list:
            self._delete(file_path, dry_run)
        for file_path in upload_list:
            self._upload(file_path, dry_run)
        if prune:
            for folder_path in self._find_empty_folder():
                self._delete(folder_path, dry_run)

        if not dry_run:
            project.pull()

    @classmethod
    def init(cls, user: str, password: str, project_id: str) -> None:
        # Create working directory
        os.makedirs(WORKING_DIR, exist_ok=True)
        # Check if working directory exists
        if os.path.exists(CONFIG_FILE):
            LOGGER.warning("Configuration file `%s` already exists. Overwriting...", CONFIG_FILE)
        # Write `config.json`
        with open(CONFIG_FILE, "w") as f:
            json.dump({"username": user, "password": password, "project_id": project_id}, f)
        LOGGER.info("Configuration file saved to %s", CONFIG_FILE)
        # Write `.gitignore`
        with open(os.path.join(WORKING_DIR, ".gitignore"), "w") as f:
            f.write("*")

        #
        project = cls(user, password, project_id)
        project.login()

        LOGGER.info("Initializing git repository in %s...", LATEX_PROJECT_DIR)
        cmd = ["git", "-C", LATEX_PROJECT_DIR, "init"]
        LOGGER.debug("Running command: %s", " ".join(cmd))
        subprocess.run(cmd, check=True)
        LOGGER.info("Git repository initialized.")

        # Migrate to git
        history: list[dict] = json.loads(project.history)["updates"]

        for version in reversed(history):
            toV = version["toV"]
            LOGGER.debug("Migrate version: %s", toV)
            project.download(version=toV)
            sleep(9)
            project.unzip()
            date = f"@{version["meta"]["end_ts"] // 1000}"
            names: list[str] = []
            emails: list[str] = []
            for u in version["meta"]["users"]:
                u: dict[str, str]
                names.append(f"{u["last_name"]}, {u["first_name"]}")
                emails.append(u["email"])
            cmd = ["git", "-C", LATEX_PROJECT_DIR, "add", "."]
            LOGGER.debug("Running command: %s", " ".join(cmd))
            subprocess.run(cmd, check=True)
            cmd = [
                "git",
                "-C",
                LATEX_PROJECT_DIR,
                "commit",
                f"--date={date}",
                f"--author={";".join(names)} <{';'.join(emails)}>",
                "-m",
                f"Overleaf version: {toV}",
            ]
            LOGGER.debug("Running command: %s", " ".join(cmd))
            subprocess.run(cmd, check=True)
            LOGGER.info("Version %s migrated.", toV)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="overleaf-sync.py", description="Overleaf Project Sync Tool")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s 1.0")
    parser.add_argument("-L", "--log", action="store_true", help="Log to file")
    parser.add_argument("-D", "--debug", action="store_true", help="Debug mode")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    init_parser = subparsers.add_parser("init", help="Initialize Overleaf project")
    init_parser.add_argument("-u", "--username", required=True, help="Overleaf username")
    init_parser.add_argument("-p", "--password", required=True, help="Overleaf password")
    init_parser.add_argument("-i", "--project-id", required=True, help="Overleaf project ID")

    pull_parser = subparsers.add_parser("pull", help="Pull changes from Overleaf project")
    pull_parser.add_argument("-k", "--keep", action="store_true", help="Keep remotely deleted files")

    push_parser = subparsers.add_parser("push", help="Push changes to Overleaf project")
    push_parser.add_argument("-f", "--force", action="store_true", help="Force push changes to Overleaf project")
    push_parser.add_argument("-p", "--prune", action="store_true", help="Prune empty folders")
    push_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    setup_logger(LOGGER, args.debug)
    if args.log:
        setup_file_logger(LOGGER)

    if args.command == "init":
        OverleafProject.init(args.username, args.password, args.project_id)
        exit(ErrorNumber.EN_OK.value)

    if not os.path.exists(WORKING_DIR):
        LOGGER.error("Overleaf sync directory `%s` does not exist. Please run `init` command first.", WORKING_DIR_NAME)
        exit(ErrorNumber.EN_WKDIR_NOT_EXIST.value)

    try:
        with open(CONFIG_FILE, "r") as config_file:
            config = json.load(config_file)
    except FileNotFoundError:
        LOGGER.error("Configuration file %s does not exist. Overleaf sync directory is corrupted.", CONFIG_FILE)
        exit(ErrorNumber.EN_CONFIG_NOT_EXIST.value)

    project = OverleafProject(config["username"], config["password"], config["project_id"])
    project.login()

    match args.command:
        case "pull":
            project.pull(keep=args.keep)
        case "push":
            project.push(force=args.force, prune=args.prune, dry_run=args.dry_run)
        case _:
            parser.print_help()
