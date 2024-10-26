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
import zipfile
import websocket
import urllib.parse

from itertools import takewhile
from enum import IntEnum, unique
from time import sleep, time
from datetime import datetime
from bs4 import BeautifulSoup


OVERLEAF_URL = "https://overleaf.s3lab.io"
LOGIN_URL = f"{OVERLEAF_URL}/login"
PROJECTS_URL = f"{OVERLEAF_URL}/project"

WORKING_DIR_NAME = ".overleaf-sync"
WORKING_DIR = os.path.join(os.getcwd(), WORKING_DIR_NAME)
LATEX_PROJECT_DIR = os.path.abspath(os.path.join(WORKING_DIR, ".."))
CONFIG_FILE = os.path.join(WORKING_DIR, "config.json")
ZIP_FILE = os.path.join(WORKING_DIR, "latex.zip")
PROJECT_UPDATES_FILE = os.path.join(WORKING_DIR, "updates.json")
IDS_FILE = os.path.join(WORKING_DIR, "file_ids.json")
REVISION_MAPPING_FILE = os.path.join(WORKING_DIR, "revision_mapping")
LATEST_COMMIT_FILE = os.path.join(WORKING_DIR, "latest_commit.txt")
REMOTE_VERSION_FILE = os.path.join(WORKING_DIR, "remote_version.txt")

OVERLEAF_BRANCH = "overleaf"
LOCAL_BRANCH = "local"

LOGGER = logging.getLogger(__name__)
LOGGER_FORMATTER = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
LOG_DIR = ".overleaf-sync-logs"


@unique
class ErrorNumber(IntEnum):
    OK = 0
    NOT_INITIALIZED_ERROR = 1
    WKDIR_CORRUPTED_ERROR = 2
    GIT_DIR_CORRUPTED_ERROR = 3
    HTTP_ERROR = 4
    GIT_ERROR = 5
    PULL_ERROR = 6
    PUSH_ERROR = 7
    REINITIALIZATION_ERROR = 8


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


def _git(*args: str) -> str:
    cmd = ["git", "-C", LATEX_PROJECT_DIR, *args]
    LOGGER.debug("Git command: %s", " ".join(cmd))
    try:
        output = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
    except subprocess.CalledProcessError as e:
        LOGGER.error("Git command failed: %s\noutput: %s", e, e.output)
        exit(ErrorNumber.GIT_ERROR)
    LOGGER.debug("Git output: \n%s", output)
    return output


def _sleep_until(ts: float) -> None:
    now = time()
    time_to_sleep = ts - now
    LOGGER.debug("Sleeping for %.3f seconds...", time_to_sleep)

    if time_to_sleep > 0:
        sleep(time_to_sleep)


class OverleafBroker:
    def __init__(self, username: str, password: str, project_id: str) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
            }
        )
        self._username = username
        self._password = password
        self._project_id = project_id
        self._project_url = f"{PROJECTS_URL}/{self._project_id}"
        self._logged_in = False
        self._updates: list[dict] | None = None
        self._csrf_token: str | None = None
        self._original_file_ids: dict | None = None
        self._root_folder_id: str | None = None
        self._indexed_file_ids: dict[str, dict[str, str]] | None = None

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
        payload = {"email": self._username, "password": self._password, "_csrf": csrf_token}
        response = self._session.post(LOGIN_URL, data=payload)
        response.raise_for_status()
        self._logged_in = True
        LOGGER.info("Login successful.")

    @property
    def updates(self) -> list[dict]:
        if self._updates:
            return self._updates

        url = f"{PROJECTS_URL}/{self._project_id}/updates"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        }
        LOGGER.debug("Fetching project updates from %s...", url)
        response = self._session.get(url, headers=headers)
        response.raise_for_status()
        self._updates = json.loads(response.text)["updates"]
        if not self._updates:
            raise ValueError("Failed to fetch project updates.")
        with open(PROJECT_UPDATES_FILE, "w") as f:
            json.dump(self._updates, f)
        return self._updates

    def download_zip(self, revision: int | None = None) -> float:
        """
        Download the project/revision ZIP file from Overleaf.
        There is a rate limit of 30 request per hour when downloading revision ZIP.
        """
        url = f"{self._project_url}/version/{revision}/zip" if revision else f"{self._project_url}/download/zip"
        LOGGER.debug("Downloading project ZIP from url: %s...", url)
        response = self._session.get(url)
        ts = time()
        response.raise_for_status()
        with open(ZIP_FILE, "wb") as f:
            f.write(response.content)
        LOGGER.debug("Project ZIP downloaded as %s", ZIP_FILE)
        return ts

    @property
    def csrf_token(self) -> str:
        if self._csrf_token:
            return self._csrf_token
        response = self._session.get(self._project_url)
        soup = BeautifulSoup(response.text, "html.parser")
        self._csrf_token = soup.find("meta", {"name": "ol-csrfToken"})["content"]  # type: ignore
        if not self._csrf_token:
            raise ValueError("Failed to fetch CSRF token.")
        return self._csrf_token

    def filetree_diff(self, from_: int, to_: int) -> list[dict]:
        LOGGER.info("Fetching filetree diff from %d to %d...", from_, to_)
        url = f"{PROJECTS_URL}/{self._project_id}/filetree/diff?from={from_}&to={to_}"
        headers = {
            "Accept": "application/json",
            "Referer": self._project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._session.get(url, headers=headers)
        response.raise_for_status()
        return json.loads(response.text)["diff"]

    def diff(self, from_: int, to_: int, pathname: str) -> list[dict]:
        LOGGER.info("Fetching diff of file %s from %d to %d...", pathname, from_, to_)
        url = f"{PROJECTS_URL}/{self._project_id}/diff?from={from_}&to={to_}&pathname={pathname}"
        headers = {
            "Accept": "application/json",
            "Referer": self._project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._session.get(url, headers=headers)
        response.raise_for_status()
        return json.loads(response.text)["diff"]

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
        self._original_file_ids = json.loads(data[4:])["args"][0]["project"]["rootFolder"][0]

        with open(IDS_FILE, "w") as f:
            json.dump(self._original_file_ids, f)

        if not self._original_file_ids:
            raise ValueError("Failed to fetch document IDs.")
        return self._original_file_ids

    @property
    def root_folder_id(self) -> str:
        # return self._root_folder_id if self._root_folder_id else hex(int(self._project_id, 16) - 1)[2:].lower()
        if self._root_folder_id:
            return self._root_folder_id
        self._root_folder_id = self.original_file_ids["_id"]
        if not self._root_folder_id:
            raise ValueError("Failed to fetch root folder ID.")
        return self._root_folder_id

    def upload(self, file_path: str, dry_run=False) -> None:
        """
        Upload the file to the Overleaf project.
        There is a rate limit of 200 request per 15 minutes.
        """
        LOGGER.info("Uploading %s...", file_path)
        if dry_run:
            return

        relative_path = "null" if not os.path.dirname(file_path) else file_path
        file_name = os.path.basename(file_path)

        url = f"{PROJECTS_URL}/{self._project_id}/upload"
        headers = {
            "Accept": "*/*",
            "Origin": OVERLEAF_URL,
            "Referer": self._project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        params = {"folder_id": self.root_folder_id}
        data = {"relativePath": f"{relative_path}", "name": file_name, "type": "application/octet-stream"}
        qqfile = open(os.path.join(LATEX_PROJECT_DIR, file_path), "rb")
        files = {"qqfile": (file_name, qqfile, "application/octet-stream")}
        response = self._session.post(url, headers=headers, params=params, data=data, files=files)
        qqfile.close()
        response.raise_for_status()

    @property
    def indexed_file_ids(self) -> dict[str, dict[str, str]]:
        if self._indexed_file_ids:
            return self._indexed_file_ids

        ids: dict[str, dict[str, str]] = {"folders": {}, "fileRefs": {}, "docs": {}}

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

    def create_folder(self, path: str, dry_run=False) -> None:
        LOGGER.info("Creating folder %s...", path)
        if dry_run:
            return
        parent_folder_id, type = self._find_id_type(os.path.dirname(path))
        assert type == "folder"

        url = f"{self._project_url}/folder"
        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self._project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        data = {"name": os.path.basename(path), "parent_folder_id": parent_folder_id}
        response = self._session.post(url, headers=headers, data=data)
        response.raise_for_status()

    def delete(self, path: str, dry_run=False) -> None:
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
            "Referer": self._project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        if type not in ["file", "doc", "folder"]:
            raise ValueError(f"Invalid type: {type}")
        response = self._session.delete(f"{self._project_url}/{type}/{id}", headers=headers)
        response.raise_for_status()


class OverleafProject:
    def __init__(
        self,
        init: bool = False,
        username: str | None = None,
        password: str | None = None,
        project_id: str | None = None,
        # n_revisions: int = 3,
    ) -> None:
        # Do initialization
        if init:
            if not (username and password and project_id):
                LOGGER.error("Missing required arguments for initialization.")
                exit(ErrorNumber.NOT_INITIALIZED_ERROR)
            # Create working directory
            LOGGER.info("Initializing working directory...")
            os.makedirs(WORKING_DIR, exist_ok=True)
            # Write `config.json`
            if os.path.exists(CONFIG_FILE):
                LOGGER.warning("Overwriting config file `%s`...", CONFIG_FILE)
            else:
                LOGGER.info("Saving config file to %s.", CONFIG_FILE)
            with open(CONFIG_FILE, "w") as f:
                json.dump({"username": username, "password": password, "project_id": project_id}, f)
            # Write `.gitignore`
            with open(os.path.join(WORKING_DIR, ".gitignore"), "w") as f:
                f.write("*")
            # Initialize git repository
            if os.path.exists(os.path.join(LATEX_PROJECT_DIR, ".git")):
                LOGGER.error("Git repository already exists in %s. Exiting...", LATEX_PROJECT_DIR)
                exit(ErrorNumber.REINITIALIZATION_ERROR)
            LOGGER.info("Initializing git repository in %s...", LATEX_PROJECT_DIR)
            _git("init", "-b", OVERLEAF_BRANCH)
            # Initialize Overleaf broker
            self.overleaf_broker = OverleafBroker(username, password, project_id)
            self.overleaf_broker.login()
            # Migration of revisions
            # self._git_repo_init_zip(n_revisions)
            self._git_repo_init_diff()
            # Sanity checks
            self._sanity_check()
            exit(ErrorNumber.OK.value)

        # Sanity checks
        self._sanity_check()
        # Initialize Overleaf broker
        with open(CONFIG_FILE, "r") as config_file:
            config: dict[str, str] = json.load(config_file)
        self.overleaf_broker = OverleafBroker(config["username"], config["password"], config["project_id"])
        self.overleaf_broker.login()

    def _sanity_check(self) -> None:
        # Check if working directory exists
        if not os.path.exists(WORKING_DIR):
            LOGGER.error(
                "Overleaf sync directory `%s` does not exist. Please run `init` command first.", WORKING_DIR_NAME
            )
            exit(ErrorNumber.NOT_INITIALIZED_ERROR)
        # Check if configuration file exists
        if not os.path.exists(CONFIG_FILE):
            LOGGER.error("Configuration file `%s` does not exist. Please reinitialize the project.", CONFIG_FILE)
            exit(ErrorNumber.WKDIR_CORRUPTED_ERROR)
        # Check if git repository is initialized
        if not os.path.exists(os.path.join(LATEX_PROJECT_DIR, ".git")):
            LOGGER.error(
                "Git is not initialized for LaTeX project in directory `%s`. Please reinitialize the project.",
                LATEX_PROJECT_DIR,
            )
            exit(ErrorNumber.GIT_DIR_CORRUPTED_ERROR)
        # Check if revision mapping file exists
        if not os.path.exists(REVISION_MAPPING_FILE):
            LOGGER.error(
                "Revision mapping file `%s` does not exist. Please reinitialize the project.", REVISION_MAPPING_FILE
            )
            exit(ErrorNumber.WKDIR_CORRUPTED_ERROR)

    def _unzip(self, zip_file: str, file_list: list | None = None) -> None:
        """
        Unzip the downloaded ZIP file to the LaTeX project directory.
        `file_list`: List of files to extract. If `None`, extract all files.
        """
        LOGGER.debug("Unzipping file %s to directory %s...", zip_file, LATEX_PROJECT_DIR)
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            if file_list:
                # TODO: not tested
                for file in zip_ref.filelist:
                    LOGGER.debug("Extracting %s...", file)
                    zip_ref.extract(file, LATEX_PROJECT_DIR)
            else:
                zip_ref.extractall(LATEX_PROJECT_DIR)
            # Delete files not in the ZIP file
            zip_filenames = {zip_info.filename for zip_info in zip_ref.filelist}
            for managed_file in self.managed_files:
                if managed_file and managed_file not in zip_filenames:
                    LOGGER.info("Deleting local file %s...", managed_file)
                    os.remove(os.path.join(LATEX_PROJECT_DIR, managed_file))

    def _migrate_revision_zip(self, revision: dict, merge_old: bool = False) -> float:
        """
        Migrate the overleaf revision to a git revision using revision ZIP.
        Note that this function is not responsible for switching branch.
        `old`: Whether to merge all old revisions.
        """
        toV = revision["toV"]
        LOGGER.info("Migrating (ZIP) revision %d...", toV)
        try:
            ts = self.overleaf_broker.download_zip(toV)
        except requests.HTTPError as e:
            LOGGER.error("Failed to download revision %d:\n%s.", toV, e)
            LOGGER.error("Please remove the working directory and try again later. Exiting...")
            exit(ErrorNumber.HTTP_ERROR)
        self._unzip(ZIP_FILE)
        name = ";".join(f"{user["last_name"]}, {user["first_name"]}" for user in revision["meta"]["users"])
        email = ";".join(user["email"] for user in revision["meta"]["users"])
        # _git("switch", OVERLEAF_BRANCH)
        _git("add", ".")
        _git(
            "commit",
            f"--date=@{revision["meta"]["end_ts"] // 1000}",
            f"--author={name} <{email}>",
            "-m",
            f"{"old" if merge_old else revision['fromV']}->{toV}",
        )
        LOGGER.info("Version %s migrated.", toV)
        return ts

    def _migrate_revisions_zip(self, revisions: list[dict]) -> None:
        """
        Migrate all the given revisions to git revisions.
        Note that this function is not responsible for switching branch.
        """
        revision_length = len(revisions)
        for i, rev in enumerate(reversed(revisions)):
            LOGGER.info("%d revision(s) to be migrated (ZIP).", revision_length - i)
            ts = self._migrate_revision_zip(rev)
            if revision_length >= 30:
                _sleep_until(ts + 120)

    def _migrate_revision_diff(self, revision: dict, one_by_one: bool = False) -> None:
        """
        Migrate the overleaf revision to a git revision using diff requests.
        Note that this function is **not** responsible for switching branch.
        `FromV` and `toV` are required if `revision` is `None`.
        """
        fromV = revision["fromV"]
        toV = revision["toV"]
        ts = revision["meta"]["end_ts"] // 1000
        name = ";".join(f"{user["last_name"]}, {user["first_name"]}" for user in revision["meta"]["users"])
        email = ";".join(user["email"] for user in revision["meta"]["users"])

        def _diff_to_content(diff: list[dict]) -> str:
            exclusive_status = {"u", "i", "d"}
            content = ""

            for d in diff:
                found_status = exclusive_status.intersection(d.keys())
                assert len(found_status) == 1
                status = found_status.pop()
                match status:
                    case "u" | "i":
                        content += d[status]
                    case "d":
                        pass
                    case _:
                        raise ValueError(f"Unsupported diff status: {status}")
            return content

        def _migrate(operation: str, pathname: str, diff: list[dict]) -> None:
            path = os.path.join(LATEX_PROJECT_DIR, pathname)
            match operation:
                case "added" | "edited":
                    LOGGER.debug("Adding/Editing %s...", item["pathname"])
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w") as f:
                        f.write(_diff_to_content(diff))
                case "removed":
                    os.remove(path)
                case "renamed":
                    os.rename(path, os.path.join(LATEX_PROJECT_DIR, item["newPathname"]))
                case _:
                    raise ValueError(f"Unsupported operation: {item['operation']}")

        LOGGER.info("Migrating (diff) revision from %d to %d...", fromV, toV)
        for from_, to_ in ((i, i + 1) for i in range(fromV, toV)) if one_by_one else [(fromV, toV)]:
            # Operate local files
            filetree_diff = self.overleaf_broker.filetree_diff(from_, to_)
            for item in filetree_diff:
                if not (operation := item.get("operation", None)):
                    continue
                pathname = item["pathname"]
                diff = self.overleaf_broker.diff(from_, to_, pathname)
                _migrate(operation, pathname, diff)
            # Make git commit
            _git("add", ".")
            _git(
                "commit",
                f"--date=@{ts}",
                f"--author={name} <{email}>",
                "-m",
                f"{to_}" if one_by_one else f"{from_}->{to_}",
            )
            LOGGER.info("Revision %d->%d migrated.", from_, to_)

    def _migrate_revisions_diff(self, revisions: list[dict]) -> None:
        """
        Migrate all the given revisions to git revisions.
        Note that this function is **not** responsible for switching branch.
        """
        revision_length = len(revisions)
        for i, rev in enumerate(reversed(revisions)):
            LOGGER.info("%d revision(s) to be migrated (diff).", revision_length - i)
            self._migrate_revision_diff(rev)

    @property
    def latest_commit_id(self) -> str:
        return _git("rev-parse", "HEAD")

    @property
    def remote_overleaf_revision(self) -> int:
        return self.overleaf_broker.updates[0]["toV"]

    @property
    def local_overleaf_revision(self) -> int:
        with open(REVISION_MAPPING_FILE, "r") as f:
            return int(f.read().split(":")[0])

    @local_overleaf_revision.setter
    def local_overleaf_revision(self, revision: int) -> None:
        with open(REVISION_MAPPING_FILE, "w") as f:
            f.write(f"{revision}:{self.latest_commit_id}")

    def _git_repo_init_zip(self, n_revisions: int) -> None:
        updates: list[dict] = self.overleaf_broker.updates
        updates_length = len(updates)
        # Fetch one more revision: old->xxx
        updates = updates[: n_revisions + 1 if n_revisions > 0 or n_revisions + 1 > updates_length else None]

        LOGGER.info("Migrating all older revisions into the first git revision...")
        _git("switch", "-c", OVERLEAF_BRANCH)
        self._migrate_revision_zip(updates[-1], merge_old=True)
        LOGGER.info("Migrating the rest of the revisions...")
        self._migrate_revisions_zip(updates[:-1])
        # Record the mapping of synced remote version and local commit ID
        self.local_overleaf_revision = self.remote_overleaf_revision
        _git("switch", "-c", LOCAL_BRANCH)

    def _git_repo_init_diff(self) -> None:
        # Latest revision may change during the migration
        updates = (
            self.overleaf_broker.updates[1:] if len(self.overleaf_broker.updates) > 1 else self.overleaf_broker.updates
        )
        LOGGER.info("Migrating all revisions except the latest one...")
        _git("switch", "-c", OVERLEAF_BRANCH)
        self._migrate_revisions_diff(updates)
        self._migrate_revision_diff(self.overleaf_broker.updates[0], one_by_one=True)
        self.local_overleaf_revision = self.remote_overleaf_revision
        _git("switch", "-c", LOCAL_BRANCH)

    @property
    def new_overleaf_revisions(self) -> bool:
        LOGGER.info(
            "remote/local overleaf revision: %d/%d", self.remote_overleaf_revision, self.local_overleaf_revision
        )
        assert self.remote_overleaf_revision >= self.local_overleaf_revision
        return self.remote_overleaf_revision > self.local_overleaf_revision

    @property
    def new_local_revisions(self) -> bool:
        # TODO: not tested
        return _git("status", "--porcelain") != ""

    def pull(self) -> None:
        if not self.new_overleaf_revisions:
            LOGGER.info("No new changes to pull.")
            return

        # Get all new overleaf revisions
        upcoming_overleaf_revisions = list(
            takewhile(lambda revision: revision["toV"] > self.local_overleaf_revision, self.overleaf_broker.updates)
        )
        # upcoming_overleaf_revisions = []
        # for revision in self.overleaf_broker.history:
        #     if revision["toV"] <= self.local_overleaf_revision:
        #         break
        #     upcoming_overleaf_revisions.append(revision)
        LOGGER.info(
            "%d upcoming revisions: %s",
            len(upcoming_overleaf_revisions),
            ", ".join(str(rev["toV"]) for rev in upcoming_overleaf_revisions),
        )
        _git("switch", OVERLEAF_BRANCH)
        self._migrate_revisions_zip(upcoming_overleaf_revisions)
        # Record the mapping of synced remote version and local commit ID
        self.local_overleaf_revision = self.remote_overleaf_revision
        if self.new_local_revisions:
            _git("switch", LOCAL_BRANCH)
        _git("switch", "-m", LOCAL_BRANCH)

    ################################################################################

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
        for folder in self.overleaf_broker.original_file_ids.get("folders", []):
            _traverse_folders(folder)

        return [p.lstrip("/") for p in empty_folders]

    def _get_changed_files(self) -> str:
        try:
            with open(LATEST_COMMIT_FILE, "r") as f:
                latest_commit_id = f.read().strip()
            LOGGER.info("Latest commit ID: %s", latest_commit_id)
        except FileNotFoundError:
            LOGGER.error("File %s does not exist.", LATEST_COMMIT_FILE)
            exit(ErrorNumber.NOT_INITIALIZED_ERROR)

        return subprocess.run(
            ["git", "-C", LATEX_PROJECT_DIR, "diff", "--name-status", latest_commit_id], capture_output=True, text=True
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
    def managed_files(self) -> list[str]:
        return (
            subprocess.run(["git", "-C", LATEX_PROJECT_DIR, "ls-files"], capture_output=True, text=True, check=True)
            .stdout.strip()
            .split("\n")
        )

    def push(self, force=False, prune=False, dry_run=False) -> None:
        if not self.new_local_revisions:
            LOGGER.error("Cannot push changes from a dirty repository.")
            exit(ErrorNumber.PUSH_ERROR)
        if self.new_overleaf_revisions:
            LOGGER.error("Cannot push changes to a updated remote repository.")
            exit(ErrorNumber.PUSH_ERROR)

        if force:
            LOGGER.info("Force pushing changes to Overleaf project...")

            for file_path in self.managed_files:
                self._upload(file_path, dry_run)
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
                case "M" | "A":
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
    # init_parser.add_argument(
    #     "-r",
    #     "--revision",
    #     type=int,
    #     default=3,
    #     help="Fetch the most recent N revisions, when N > 30, it will sleep for 2 minutes after each download. N <= 0 means all revisions.",
    # )

    pull_parser = subparsers.add_parser("pull", help="Pull changes from Overleaf project")

    push_parser = subparsers.add_parser("push", help="Push changes to Overleaf project")
    push_parser.add_argument("-f", "--force", action="store_true", help="Force push changes to Overleaf project")
    push_parser.add_argument("-p", "--prune", action="store_true", help="Prune empty folders")
    push_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    setup_logger(LOGGER, args.debug)
    if args.log:
        setup_file_logger(LOGGER)

    if args.command == "init":
        project = OverleafProject(
            init=True,
            username=args.username,
            password=args.password,
            project_id=args.project_id,
            # n_revisions=args.revision,
        )
    else:
        project = OverleafProject()

    match args.command:
        case "pull":
            project.pull()
        case "push":
            project.push(force=args.force, prune=args.prune, dry_run=args.dry_run)
        case _:
            parser.print_help()
