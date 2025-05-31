#!/opt/bin/python3

################################################################
### Name: overleaf-sync.py
### Description: Overleaf Project Sync Tool
### Author: KZL
################################################################

from __future__ import annotations

import sys
import os
import shutil
import subprocess
import argparse
import logging
import requests
import json
import zipfile
import websocket
import urllib.parse
import traceback

from pathlib import Path
from itertools import takewhile
from enum import IntEnum, unique
from time import sleep, time
from datetime import datetime
from bs4 import BeautifulSoup


OVERLEAF_URL = "https://overleaf.s3lab.io"
LOGIN_URL = f"{OVERLEAF_URL}/login"
PROJECTS_URL = f"{OVERLEAF_URL}/project"

OVERLEAF_SYNC_DIR_NAME = ".overleaf-sync"

LOG_FORMAT = "[%(asctime)s.%(msecs)03d] [%(name)s] [%(levelname)s] %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_DIR = os.path.join(OVERLEAF_SYNC_DIR_NAME, "logs")


def setup_logger(logger: logging.Logger, debug: bool, log_file: bool = True) -> None:
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if debug else logging.INFO)
    ch.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
    logger.addHandler(ch)

    if not log_file:
        return

    os.makedirs(LOG_DIR, exist_ok=True)
    with open(os.path.join(LOG_DIR, ".gitignore"), "w") as f:
        f.write("*")
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"{now}.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
    logger.addHandler(fh)


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
    WORKING_TREE_DIRTY_ERROR = 8
    REINITIALIZATION_ERROR = 9


class GitBroker:
    logger = logging.getLogger(__qualname__)
    WORKING_BRANCH_START_COMMIT_TAG = "ws"

    def __init__(self, working_dir, overleaf_branch="overleaf", working_branch="working") -> None:
        self.working_dir = working_dir
        self.overleaf_branch = overleaf_branch
        self.working_branch = working_branch

    def __call__(self, *args: str, check=True) -> str:
        cmd = ["git", "-C", self.working_dir, *args]
        self.logger.debug("Git command: %s", " ".join(cmd))
        try:
            output = subprocess.run(cmd, capture_output=True, text=True, check=check).stdout.strip()
        except subprocess.CalledProcessError as e:
            self.logger.error("Git command failed: %s\noutput:\n%s\n---", e, e.output)
            traceback.print_stack()
            exit(ErrorNumber.GIT_ERROR)
        self.logger.debug("Git output: \n%s", output)
        return output

    def init(self, force=False) -> None:
        if os.path.exists(os.path.join(self.working_dir, ".git")):
            if force:
                self.logger.warning("Reinitializing git repository in %s...", self.working_dir)
                shutil.rmtree(os.path.join(self.working_dir, ".git"))
            else:
                self.logger.error("Git repository already exists in %s. Exiting...", self.working_dir)
                exit(ErrorNumber.REINITIALIZATION_ERROR)
        self("init", "-b", self.overleaf_branch)

    def sanity_check(self) -> None:
        # Check if git repository is initialized
        if not os.path.exists(os.path.join(self.working_dir, ".git")):
            self.logger.error(
                "Git is not initialized for LaTeX project in directory `%s`. Please reinitialize the project",
                self.working_dir,
            )
            exit(ErrorNumber.GIT_DIR_CORRUPTED_ERROR)
        # Check if both overleaf branch and working branch exist
        branches = [_.lstrip("*").strip() for _ in self("branch", "--list").splitlines()]
        if not (self.overleaf_branch in branches and self.working_branch in branches):
            self.logger.error(
                "Branches `%s` or `%s` are missing. Working directory corrupted. Please reinitialize the project",
                self.overleaf_branch,
                self.working_branch,
            )
            exit(ErrorNumber.WKDIR_CORRUPTED_ERROR)

    @property
    def managed_files(self) -> list[str]:
        return self("ls-files").splitlines()

    def add_all(self) -> bool:
        output = self("add", ".")
        if "nothing to commit" in output:
            self.logger.debug("No changes to commit")
            return False
        return True

    def commit(self, msg: str, ts: int, name: str, email: str) -> None:
        self(
            "commit",
            "--allow-empty",
            f"--date=@{ts}",
            f"--author={name} <{email}>",
            "-m",
            msg,
        )

    @property
    def starting_working_commit(self) -> str:
        # The first commit ID where working branch forked from overleaf branch
        # return self("merge-base", self.overleaf_branch, self.overleaf_branch)
        return self("rev-parse", self.WORKING_BRANCH_START_COMMIT_TAG)

    @property
    def current_working_commit(self) -> str:
        return self("rev-parse", self.working_branch)

    @property
    def is_current_branch_clean(self) -> bool:
        return not self("status", "--porcelain")

    def switch_to_overleaf_branch(self, create=False) -> None:
        assert self.is_current_branch_clean
        if create:
            self("switch", "-c", self.overleaf_branch)
        else:
            self("switch", self.overleaf_branch)

    def _update_working_branch_start_commit(self) -> None:
        self("tag", "-f", self.WORKING_BRANCH_START_COMMIT_TAG, self.overleaf_branch)

    def switch_to_working_branch(self, force=False) -> None:
        if force:
            self("branch", "-f", self.working_branch, self.overleaf_branch)
            self._update_working_branch_start_commit()
        self("switch", self.working_branch)

    @property
    def local_overleaf_version(self) -> int:
        """The latest overleaf update in local git repository"""
        return int(self("log", "-1", "--pretty=%B", self.overleaf_branch).split("->")[1])

    def reset_hard(self, n: int) -> None:
        self("reset", "--hard", f"HEAD~{n}")

    def tag_working_branch(self, tag: str) -> None:
        self("tag", tag, self.working_branch)

    def rebase_working_branch(self) -> bool:
        """Rebase working branch to overleaf branch"""
        result = self("rebase", self.overleaf_branch, self.working_branch, check=False)
        self._update_working_branch_start_commit()
        if "CONFLICT" in result:
            self.logger.error(
                "Failed to rebase `%s` to `%s`.\n%s\nFix conflicts and run `git rebase --continue`",
                self.working_branch,
                self.overleaf_branch,
                result,
            )
            return False
        return True

    @property
    def is_there_unmerged_overleaf_rev(self) -> bool:
        return not self("log", f"{self.working_branch}..{self.overleaf_branch}")

    @property
    def is_identical_working_overleaf(self) -> bool:
        return not self("diff-tree", "-r", self.working_branch, self.overleaf_branch)

    @property
    def current_branch(self) -> str:
        return self("branch", "--show-current")

    def stash_working(self) -> bool:
        if not self.current_branch == self.working_branch:
            return False
        if self("stash", "-u").startswith("No local changes to save"):
            return False
        return True

    def stash_pop_working(self) -> None:
        assert self.current_branch == self.working_branch
        self("stash", "pop")

    @property
    def working_branch_status(self) -> list[str]:
        assert self.current_branch == self.working_branch
        return self("diff", "--name-status", self.WORKING_BRANCH_START_COMMIT_TAG).splitlines()


class OverleafBroker:
    logger = logging.getLogger(__qualname__)

    def __init__(self, working_dir: str, overleaf_sync_dir: str) -> None:
        self.working_dir = working_dir
        self.overleaf_sync_dir = overleaf_sync_dir
        self.updates_file = os.path.join(self.overleaf_sync_dir, "updates.json")
        self.overleaf_zip = os.path.join(self.overleaf_sync_dir, "overleaf.zip")
        self.ids_file = os.path.join(self.overleaf_sync_dir, "ids.json")
        self.indexed_ids_file = os.path.join(self.overleaf_sync_dir, "indexed_ids.json")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            }
        )
        self.username: str | None = None
        self.password: str | None = None
        self.project_id: str | None = None
        self._logged_in = False
        self._updates_min_count: int = 100
        self._updates: list[dict] | None = None
        self._csrf_token: str | None = None
        self._download_zip_ts: float = 0
        self._original_file_ids: dict | None = None
        self._root_folder_id: str | None = None
        self._indexed_file_ids: dict[str, dict[str, str]] | None = None

    @property
    def project_url(self) -> str:
        return f"{PROJECTS_URL}/{self.project_id}"

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        if not self._logged_in:
            self.logger.error("Not logged in. Please login first")
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def _get(self, url: str, **kwargs) -> requests.Response:
        return self._request("GET", url, **kwargs)

    def _post(self, url: str, **kwargs) -> requests.Response:
        return self._request("POST", url, **kwargs)

    def _delete(self, url: str, **kwargs) -> requests.Response:
        return self._request("DELETE", url, **kwargs)

    def login(self, username: str | None = None, password: str | None = None, project_id: str | None = None) -> None:
        if self._logged_in:
            return

        self.username = username
        self.password = password
        self.project_id = project_id

        self.logger.info("Logging in to Overleaf...")
        response = self._session.get(LOGIN_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        csrf_token: str
        csrf_token = soup.find("input", {"name": "_csrf"})["value"]  # type: ignore
        if not csrf_token:
            raise ValueError("Failed to fetch CSRF token")
        payload = {"email": self.username, "password": self.password, "_csrf": csrf_token}
        response = self._session.post(LOGIN_URL, data=payload)
        self._logged_in = True
        self.logger.info("Successfully logged in to Overleaf")

    def _get_updates(self, before=0) -> tuple[list[dict], int]:
        url = (
            f"{PROJECTS_URL}/{self.project_id}/updates?before={before}"
            if before > 0
            else f"{PROJECTS_URL}/{self.project_id}/updates"
        )
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        }
        self.logger.debug("Fetching project updates from %s...", url)
        response = self._get(url, headers=headers)
        response_json: dict = response.json()
        return response_json["updates"], response_json.get("nextBeforeTimestamp", 0)

    def get_updates(self, before=0) -> list[dict]:
        updates, _ = self._get_updates(before)
        if not updates:
            raise ValueError("Failed to fetch project updates")
        return updates

    def dump_updates(self) -> None:
        with open(self.updates_file, "w") as f:
            json.dump(self.updates, f)

    @property
    def updates(self) -> list[dict]:
        if self._updates:
            return self._updates
        self._updates, next_before_ts = self._get_updates()
        while next_before_ts > 0:
            updates, next_before_ts = self._get_updates(before=next_before_ts)
            self._updates.extend(updates)
        self.dump_updates()
        return self._updates

    @property
    def remote_overleaf_version(self) -> int:
        """The latest overleaf update in remote Overleaf project"""
        return self.updates[0]["toV"]

    def refresh_updates(self) -> None:
        self.logger.debug("Overleaf updates marked outdated...")
        self._updates = None

    def download_zip(self, update: int | None = None) -> None:
        """
        Download the project/update ZIP file from Overleaf.
        There is a rate limit of 30 request per hour when downloading update ZIP.
        """

        def _sleep_until(ts: float) -> None:
            now = time()
            time_to_sleep = ts - now if ts > now else 0
            self.logger.debug("Sleeping for %.3f seconds...", time_to_sleep)
            sleep(time_to_sleep)

        url = f"{self.project_url}/version/{update}/zip" if update else f"{self.project_url}/download/zip"
        self.logger.debug("Downloading project ZIP from url: %s...", url)
        _sleep_until(self._download_zip_ts + 120)
        response = self._get(url)
        self._download_zip_ts = time()
        with open(self.overleaf_zip, "wb") as f:
            f.write(response.content)
        self.logger.debug("Project ZIP downloaded as %s", self.overleaf_zip)

    def unzip(self, file_list: list | None = None) -> None:
        """
        Unzip the downloaded ZIP file to the LaTeX project directory.
        `file_list`: List of files to extract. If `None`, extract all files.
        """
        self.logger.debug("Unzipping file %s to directory %s...", self.overleaf_zip, self.working_dir)
        with zipfile.ZipFile(self.overleaf_zip, "r") as zip_ref:
            if file_list:
                # TODO: not tested
                for file in zip_ref.filelist:
                    self.logger.debug("Extracting %s...", file)
                    zip_ref.extract(file, self.working_dir)
            else:
                zip_ref.extractall(self.working_dir)

    @property
    def csrf_token(self) -> str:
        if self._csrf_token:
            return self._csrf_token
        response = self._get(self.project_url)
        soup = BeautifulSoup(response.text, "html.parser")
        self._csrf_token = soup.find("meta", {"name": "ol-csrfToken"})["content"]  # type: ignore
        if not self._csrf_token:
            raise ValueError("Failed to fetch CSRF token")
        return self._csrf_token

    def filetree_diff(self, from_: int, to_: int) -> list[dict]:
        self.logger.debug("Fetching filetree diff: %d->%d", from_, to_)
        url = f"{PROJECTS_URL}/{self.project_id}/filetree/diff?from={from_}&to={to_}"
        headers = {
            "Accept": "application/json",
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._get(url, headers=headers)
        return response.json()["diff"]

    def diff(self, from_: int, to_: int, pathname: str) -> list[dict]:
        self.logger.debug("Fetching file `%s` diff: %d->%d", pathname, from_, to_)
        url = f"{PROJECTS_URL}/{self.project_id}/diff?from={from_}&to={to_}&pathname={urllib.parse.quote(pathname)}"
        headers = {
            "Accept": "application/json",
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._get(url, headers=headers)
        return response.json()["diff"]

    def find_id_type(self, pathname: str) -> tuple[str, str] | tuple[None, None]:
        self.logger.debug("Finding id for `%s`...", pathname)

        if pathname == "":
            return self.root_folder_id, "folder"

        ids = self.indexed_ids
        if pathname in ids["fileRefs"]:
            id, type = ids["fileRefs"][pathname], "file"
        elif pathname in ids["docs"]:
            id, type = ids["docs"][pathname], "doc"
        elif pathname in ids["folders"]:
            id, type = ids["folders"][pathname], "folder"
        else:
            return (None, None)
        self.logger.debug("Found file ID for `%s`: %s (%s)", pathname, id, type)
        return id, type

    def download_file(self, id: str, pathname: str) -> bool:
        self.logger.debug("Downloading file %s...", pathname)
        url = f"{PROJECTS_URL}/{self.project_id}/file/{id}"
        headers = {
            "Accept": "*/*",
            "Referer": self.project_url,
        }
        try:
            response = self._get(url, headers=headers)
        except requests.HTTPError as e:
            self.logger.error("Failed to download file %s:\n%s", pathname, e)
            return False
        if dirname := os.path.dirname(pathname):
            os.makedirs(dirname, exist_ok=True)
        with open(os.path.join(self.working_dir, pathname), "wb") as f:
            f.write(response.content)
        self.logger.debug("Succeed to download file %s...", pathname)
        return True

    def download_doc_file(self, id: str, pathname: str) -> bool:
        self.logger.debug("Downloading doc file %s...", pathname)
        url = f"{PROJECTS_URL}/{self.project_id}/doc/{id}/download"
        headers = {
            "Accept": "*/*",
            "Referer": self.project_url,
        }
        try:
            response = self._get(url, headers=headers)
        except requests.HTTPError as e:
            self.logger.error("Failed to download doc file %s:\n%s", pathname, e)
            return False
        if dirname := os.path.dirname(pathname):
            os.makedirs(dirname, exist_ok=True)
        with open(os.path.join(self.working_dir, pathname), "wb") as f:
            f.write(response.content)
        self.logger.debug("Succeed to download doc file %s...", pathname)
        return True

    def _get_root_folder_json(self) -> dict:
        self.logger.debug("Fetching pathname IDs from Overleaf project %s...", self.project_id)
        response = self._get(f"{OVERLEAF_URL}/socket.io/1/?projectId={self.project_id}")
        ws_id = response.text.split(":")[0]
        ws = websocket.create_connection(
            f"wss://overleaf.s3lab.io/socket.io/1/websocket/{ws_id}?projectId={self.project_id}"
        )
        while True:
            try:
                data = ws.recv()
                assert isinstance(data, str)
            except websocket.WebSocketConnectionClosedException:
                self.logger.critical("WebSocket connection closed")
                exit(ErrorNumber.HTTP_ERROR)
            else:
                if data.startswith("5:::"):
                    data_json = json.loads(data[4:])
                    response_name = data_json["name"]
                    self.logger.debug("WebSocket response: %s", response_name)
                    if response_name == "joinProjectResponse":
                        break
        ws.close()

        ids = data_json["args"][0]["project"]["rootFolder"][0]
        if not ids:
            raise RuntimeError("Failed to fetch document IDs")
        return ids

    @property
    def root_folder_json(self) -> dict:
        if self._original_file_ids:
            return self._original_file_ids
        self._original_file_ids = self._get_root_folder_json()
        with open(self.ids_file, "w") as f:
            json.dump(self._original_file_ids, f)
        return self._original_file_ids

    @property
    def root_folder_id(self) -> str:
        # return self._root_folder_id if self._root_folder_id else hex(int(self._project_id, 16) - 1)[2:].lower()
        if self._root_folder_id:
            return self._root_folder_id
        self._root_folder_id = self.root_folder_json["_id"]
        if not self._root_folder_id:
            raise RuntimeError("Failed to fetch root folder ID")
        return self._root_folder_id

    def upload(self, pathname: str, dry_run=False) -> None:
        """
        Upload the file to the Overleaf project.
        There is a rate limit of 200 request per 15 minutes.
        """
        self.logger.info("Uploading `%s`...", pathname)
        if dry_run:
            return

        folder_name = os.path.dirname(pathname)
        if folder_name != "":
            folder_id, type = self.find_id_type(folder_name)
            if folder_id is None:
                folder_id = self.create_folder(folder_name)
            else:
                assert type == "folder"
        else:
            folder_id = self.root_folder_id
        file_name = os.path.basename(pathname)

        url = f"{PROJECTS_URL}/{self.project_id}/upload"
        headers = {
            "Accept": "*/*",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        params = {"folder_id": folder_id}
        data = {
            "relativePath": "null",
            "type": "application/octet-stream",
            "name": file_name,
        }
        with open(os.path.join(self.working_dir, pathname), "rb") as qqfile:
            files = {
                "qqfile": (file_name, qqfile, "application/octet-stream"),
            }
            self._post(url, headers=headers, params=params, data=data, files=files)

    def _get_indexed_ids(self) -> dict[str, dict[str, str]]:
        ids: dict[str, dict[str, str]] = {"folders": {}, "fileRefs": {}, "docs": {}}

        def _restructure(folder_json: dict, current_folder_pathname="") -> None:
            for sub_folder in folder_json["folders"]:
                sub_folder_pathname = f"{current_folder_pathname}{sub_folder['name']}"
                ids["folders"][sub_folder_pathname] = sub_folder["_id"]
                _restructure(sub_folder, f"{sub_folder_pathname}/")
            for doc in folder_json["docs"]:
                ids["docs"][f"{current_folder_pathname}{doc['name']}"] = doc["_id"]
            for file_ref in folder_json["fileRefs"]:
                ids["fileRefs"][f"{current_folder_pathname}{file_ref['name']}"] = file_ref["_id"]

        _restructure(self.root_folder_json)
        return ids

    @property
    def indexed_ids(self) -> dict[str, dict[str, str]]:
        if self._indexed_file_ids:
            return self._indexed_file_ids
        self._indexed_file_ids = self._get_indexed_ids()
        with open(self.indexed_ids_file, "w") as f:
            json.dump(self._indexed_file_ids, f)
        return self._indexed_file_ids

    def refresh_indexed_file_ids(self) -> None:
        self.logger.debug("Indexed file IDs marked outdated...")
        self._original_file_ids = None
        self._indexed_file_ids = None

    def create_folder(self, pathname: str, dry_run=False) -> str:
        self.logger.info("Creating folder %s...", pathname)

        if pathname == "":
            return self.root_folder_id

        # Check if the parent folder exists
        dirname = os.path.dirname(pathname)
        parent_folder_id, type = self.find_id_type(dirname)
        if parent_folder_id is None:
            parent_folder_id = self.create_folder(dirname, dry_run=dry_run)
        else:
            assert type == "folder"

        if dry_run:
            return ""

        url = f"{self.project_url}/folder"
        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        data = {"name": os.path.basename(pathname), "parent_folder_id": parent_folder_id}
        response_json = self._post(url, headers=headers, data=data).json()
        self.refresh_indexed_file_ids()
        self.logger.info("Folder %s created", pathname)
        return response_json["_id"]

    def delete(self, pathname: str, dry_run=False) -> None:
        id, type = self.find_id_type(pathname)
        self.logger.info("Deleting `%s`(%s): %s", pathname, type, id)

        if type == "folder" and input(
            f"Are you sure you want to delete folder {pathname}? (y/n): "
        ).strip().lower() not in ["y", "yes"]:
            self.logger.info("Operation cancelled")
            return

        if dry_run:
            return

        url = f"{self.project_url}/{type}/{id}"
        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        if type not in ["file", "doc", "folder"]:
            raise ValueError(f"Invalid type: {type}")
        self._delete(url, headers=headers)

    def set_label(self, version: int, label: str) -> None:
        self.logger.info("Labelling version %d as `%s`...", version, label)
        url = f"{PROJECTS_URL}/{self.project_id}/label"
        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        data = {
            "comment": label,
            "version": version,
        }
        self._post(url, headers=headers, data=data)
        self.logger.info("Version %d labelled as `%s`", version, label)

    def get_labels(self) -> dict:
        self.logger.debug("Fetching labels...")
        url = f"{PROJECTS_URL}/{self.project_id}/labels"
        headers = {
            "Accept": "application/json",
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._get(url, headers=headers)
        return response.json()


class OverleafProject:
    logger = logging.getLogger(__qualname__)

    def __init__(self, working_dir: str = ".") -> None:
        self.working_dir = working_dir
        self.overleaf_sync_dir = os.path.join(self.working_dir, OVERLEAF_SYNC_DIR_NAME)
        self.config_file = os.path.join(self.overleaf_sync_dir, "config.json")
        self._initialized = False

        # Initialize git broker
        self.git_broker = GitBroker(self.working_dir)
        # Initialize overleaf broker
        self.overleaf_broker = OverleafBroker(self.working_dir, self.overleaf_sync_dir)
        if not os.path.exists(self.config_file):
            return

        self.sanity_check()
        self._initialized = True
        with open(self.config_file, "r") as f:
            config: dict[str, str] = json.load(f)
        self.overleaf_broker.login(config["username"], config["password"], config["project_id"])

    @property
    def initialized(self) -> bool:
        return self._initialized

    def sanity_check(self) -> None:
        self.logger.debug("Sanity checking...")
        # git repo
        self.git_broker.sanity_check()

    def _remove(self, path):
        try:
            os.remove(os.path.join(self.working_dir, path))
        except FileNotFoundError:
            self.logger.debug('File "%s" not found. Skipping...', path)

    def _apply_changes_zip(self, to_v: int, filetree_diff_entries: list[dict]) -> None:
        """
        Fetch and apply the changes between two overleaf updates via downloaded ZIP.
        """
        for filetree_diff_entry in filetree_diff_entries:
            pathname = filetree_diff_entry["pathname"]
            operation = filetree_diff_entry["operation"]
            path = os.path.join(self.working_dir, pathname)
            match operation:
                case "removed":
                    self.logger.debug("Remove `%s`...", pathname)
                    self._remove(path)
                case "renamed":
                    self.logger.debug("Rename `%s`...", pathname)
                    self._remove(path)
                case _:
                    pass
        try:
            self.overleaf_broker.download_zip(to_v)
        except requests.HTTPError as e:
            self.logger.critical("Failed to download update %d:\n%s", to_v, e)
            self.logger.critical("Please remove the working directory and try again later. Exiting...")
            exit(ErrorNumber.HTTP_ERROR)
        self.overleaf_broker.unzip()

    def _apply_changes_diff(self, from_v: int, to_v: int, filetree_diff_entries: list[dict]) -> None:
        """
        Fetch and apply the changes between two overleaf updates via filetree diff.
        """

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

        for filetree_diff_entry in filetree_diff_entries:
            pathname = filetree_diff_entry["pathname"]
            operation = filetree_diff_entry["operation"]
            path = os.path.join(self.working_dir, pathname)
            match operation:
                case "added" | "edited":
                    self.logger.info("Add/Edit `%s`...", pathname)
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w") as f:
                        f.write(_diff_to_content(self.overleaf_broker.diff(from_v, to_v, pathname)))
                case "removed":
                    self.logger.info("Remove `%s`...", pathname)
                    self._remove(path)
                case "renamed":
                    self.logger.info("Rename `%s`...", pathname)
                    new_path = os.path.join(self.working_dir, filetree_diff_entry["newPathname"])
                    os.makedirs(os.path.dirname(new_path), exist_ok=True)
                    os.rename(path, new_path)
                case _:
                    raise ValueError(f"Unsupported operation: {operation}")

    def _migrate(self, from_v: int, to_v: int, ts: int, user: dict[str, str] | None = None) -> None:
        """
        Migrate the overleaf update from `from_v` to `to_v` to a git update.
        """

        self.logger.debug("Migrating overleaf update %d->%d...", from_v, to_v)
        # Operate files on filesystem
        filetree_diff_entries = [
            entry for entry in self.overleaf_broker.filetree_diff(from_v, to_v) if "operation" in entry
        ]

        if all(_.get("editable", True) or _["operation"] in ("removed", "renamed") for _ in filetree_diff_entries):
            # if all(self.overleaf_broker.find_id_type(_["pathname"])[1] == "doc" for _ in filetree_diff_entries):
            self._apply_changes_diff(from_v, to_v, filetree_diff_entries)
        else:
            self.logger.info("Switch to ZIP migration: %d", to_v)
            self._apply_changes_zip(to_v, filetree_diff_entries)

        self.git_broker.add_all()
        self.git_broker.commit(
            f"{from_v}->{to_v}",
            ts,
            f"{user.get('last_name', '')}, {user.get('first_name', '')}" if user else "Unknown",
            user.get("email", "") if user else "Unknown",
        )
        self.logger.debug("Successfully migrated overleaf update %d->%d", from_v, to_v)

    def _migrate_update(self, update: dict) -> None:
        """
        Migrate the given update to git update.
        If there are multiple users in the update, split the update into multiple git updates.
        Note that this function is **not** responsible for switching branch.
        """

        def _get_filetree_diff_users_ts(from_: int, to_: int) -> tuple[list[dict[str, str]], int]:
            _users = []
            _ts = 0
            _seen_ids = set()
            for filetree_diff in self.overleaf_broker.filetree_diff(from_, to_):
                if "operation" not in filetree_diff:
                    continue
                for diff in self.overleaf_broker.diff(from_, to_, pathname=filetree_diff["pathname"]):
                    if "i" in diff or "d" in diff:
                        for u in diff["meta"]["users"]:
                            if u["id"] not in _seen_ids:
                                _users.append(u)
                                _seen_ids.add(u["id"])
                        _ts = max(_ts, diff["meta"]["end_ts"])
            return _users, _ts

        fromV = update["fromV"]
        toV = update["toV"]
        users: list[dict[str, str]] = update["meta"]["users"]

        if len(users) == 1:
            ts = update["meta"]["end_ts"] // 1000
            self._migrate(fromV, toV, ts, users[0])
        else:
            self.logger.debug(
                "Multiple users detected: %s",
                "; ".join([f"{u['last_name']}, {u['first_name']}" for u in users]),
            )
            from_v = fromV
            ts = update["meta"]["end_ts"]
            current_user: dict | None = None
            for v in range(from_v + 1, toV):
                users, ts = _get_filetree_diff_users_ts(from_v, v)
                if len(users) == 0:
                    # There exists cases that there is no modification in the diff, so no users
                    continue
                elif len(users) == 1:
                    if current_user is None:
                        current_user = users[0]
                    elif current_user["id"] != users[0]["id"]:
                        assert v - 1 > from_v, "There should be at least one update before this"
                        self._migrate(from_v, v - 1, ts, current_user)
                        from_v = v - 1
                        current_user = users[0]
                    # else: continue  # current user is the same, no need to migrate
                elif len(users) == 2:
                    assert v - 1 > from_v, "There should be at least one update before this"
                    assert current_user is not None, "Current user should be set"
                    self._migrate(from_v, v - 1, ts, current_user)
                    from_v = v - 1
                    # get the other user
                    current_user = next(user for user in users if user is not current_user)
                else:
                    # not possible
                    raise ValueError("Too many users (%d) in the update", len(users))
            else:
                self._migrate(from_v, toV, ts, current_user)

            # There exists cases that there is no modification in the diff, so no users
            # For example, (fromV, fromV + 1) no modification, but (fromV, fromV + 2) has modification
            # for to_v in range(fromV + 1, toV):
            #     users, ts = _get_filetree_diff_users_ts(fromV, to_v)
            #     if len(users) == 1:
            #         first_user_id = users[0]["id"]
            #         break
            # else:
            #     # not possible
            #     raise ValueError("No user found in the update")
            # for v in range(to_v, toV):
            #     users, ts = _get_filetree_diff_users_ts(v, v + 1)
            #     if len(users) == 0:
            #         # There exists cases that there is no modification in the diff, so no users
            #         continue
            #     assert len(users) == 1
            #     user = users[0]
            #     user_id = user["id"]
            #     if user_id != first_user_id:
            #         self._migrate(from_v, v, ts, users[0])
            #         from_v = v
            #         first_user_id = user_id
            # users, ts = _get_filetree_diff_users_ts(from_v, toV)
            # assert len(users) == 1
            # self._migrate(from_v, toV, ts, users[0])

    def _migrate_updates(self, updates: list[dict], dry_run=False) -> None:
        """
        Migrate all the given updates to git updates.
        Note that this function is **not** responsible for switching branch.
        """
        update_length = len(updates)
        log_msg = f"Migrating overleaf update %{len(str(update_length))}d/{update_length}: %d->%d"
        for i, update in enumerate(reversed(updates)):
            self.logger.info(log_msg, i + 1, update["fromV"], update["toV"])
            if not dry_run:
                self._migrate_update(update)

    @property
    def new_working_commit_exists(self) -> bool:
        return self.git_broker.current_working_commit != self.git_broker.starting_working_commit

    def _git_repo_init(self) -> None:
        self.logger.info("Migrating all updates...")
        self.git_broker.switch_to_overleaf_branch(create=True)
        self._migrate_updates(self.overleaf_broker.updates)
        self.git_broker.switch_to_working_branch(force=True)

    def init(self, username: str, password: str, project_id: str) -> ErrorNumber:
        self.logger.info("Initializing working directory...")
        # Check if the working directory is empty except for the overleaf-sync directory
        entries = os.listdir(self.working_dir)
        entries.remove(OVERLEAF_SYNC_DIR_NAME)
        if entries:
            self.logger.error(
                "Working directory `%s` is not empty. Please clean up the directory first",
                os.path.realpath(self.working_dir),
            )
            shutil.rmtree(self.overleaf_sync_dir)
            exit(ErrorNumber.WORKING_TREE_DIRTY_ERROR)
        # Create overleaf-sync directory
        os.makedirs(self.overleaf_sync_dir, exist_ok=True)
        # Write `config.json`
        if os.path.exists(self.config_file):
            self.logger.warning("Overwriting config file `%s`...", self.config_file)
        else:
            self.logger.info("Saving config file to %s", self.config_file)
        with open(self.config_file, "w") as f:
            json.dump({"username": username, "password": password, "project_id": project_id}, f)
        # Write `.gitignore`
        with open(os.path.join(self.overleaf_sync_dir, ".gitignore"), "w") as f:
            f.write("*")
        # Initialize git repo
        self.git_broker.init()
        # login overleaf broker
        self.overleaf_broker.login(username, password, project_id)
        # Migrate overleaf updates to git repo
        self._git_repo_init()

        return ErrorNumber.OK

    @property
    def new_remote_overleaf_rev(self) -> bool:
        local_overleaf_version = self.git_broker.local_overleaf_version
        remote_overleaf_version = self.overleaf_broker.remote_overleaf_version
        self.logger.info("Overleaf remote/local version: %d/%d", remote_overleaf_version, local_overleaf_version)
        assert remote_overleaf_version >= local_overleaf_version
        return remote_overleaf_version > local_overleaf_version

    def _pull_push_stash(self, stash=True) -> bool:
        if stash:
            self.logger.info("Stashing changes before pulling/pushing")
            self.git_broker.switch_to_working_branch()
            # Reuse `stash` to check if there are stashed changes
            stash = self.git_broker.stash_working()
        elif not self.git_broker.is_current_branch_clean:
            # Check if the working tree is clean
            self.logger.error(
                "Working tree is dirty. Pull/Push stopped. Either run with `--no-stash` or commit changes first"
            )
            exit(ErrorNumber.WORKING_TREE_DIRTY_ERROR)
        return stash

    def _pull_push_stash_pop(self, stash: bool) -> None:
        if stash:
            if self.git_broker.current_branch != self.git_broker.working_branch:
                self.logger.warning("Stash not pop'ed. Please run `git stash pop` to restore changes")
            else:
                self.logger.info("Pop'ing stashed changes...")
                self.git_broker.stash_pop_working()

    def _pull(self, dry_run: bool) -> None:
        """
        Pull the latest changes from Overleaf and apply them to the working branch.
        """
        self.logger.info("Pulling changes from Overleaf...")
        # Get all new overleaf updates
        local_overleaf_version = self.git_broker.local_overleaf_version
        upcoming_overleaf_versions = list(
            takewhile(lambda rev: rev["toV"] > local_overleaf_version, self.overleaf_broker.updates)
        )
        assert len(upcoming_overleaf_versions) > 0

        # The corresponding remove overleaf update of latest local overleaf update may changed after the migration
        # For example, 63->67 may become 63->64, 64->68
        if upcoming_overleaf_versions[-1]["fromV"] < local_overleaf_version:
            assert upcoming_overleaf_versions[-1]["toV"] > local_overleaf_version
            upcoming_overleaf_versions[-1]["fromV"] = local_overleaf_version

        self.logger.debug(
            "%d upcoming updates: %s",
            len(upcoming_overleaf_versions),
            ", ".join(f"{rev['fromV']}->{rev['toV']}" for rev in reversed(upcoming_overleaf_versions)),
        )

        self.git_broker.switch_to_overleaf_branch()
        self._migrate_updates(upcoming_overleaf_versions, dry_run=dry_run)
        self.logger.debug("Current branch (after `pull`): %s", self.git_broker.current_branch)

    def _pull_prune(self, dry_run: bool) -> None:
        """
        Prune local folders that are not present in the remote Overleaf project.
        """
        self.logger.info("Pruning local folders not present in Overleaf...")
        remote_overleaf_folders = self.overleaf_broker.indexed_ids["folders"].keys()
        for folder in (_ for _ in Path(self.working_dir).iterdir() if _.is_dir()):
            if (
                folder.name != ".git"
                and folder.name != OVERLEAF_SYNC_DIR_NAME
                and folder.name not in remote_overleaf_folders
            ):
                self.logger.info("Pruning local folder `%s`...", folder)
                if not dry_run:
                    folder.rmdir()

    def pull(self, stash=True, prune=False, dry_run=False) -> ErrorNumber:
        if not self.initialized:
            self.logger.error("Project not initialized. Please run `init` first")
            shutil.rmtree(self.overleaf_sync_dir)
            return ErrorNumber.NOT_INITIALIZED_ERROR

        new_remote_overleaf_rev = self.new_remote_overleaf_rev
        if not new_remote_overleaf_rev and not prune:
            self.logger.info("No new changes to pull")
            return ErrorNumber.OK

        # Perform stash before pulling to prevent uncommitted changes in working branch
        # Reuse `stash` to check if there are stashed changes
        stash = self._pull_push_stash(stash)
        self.logger.info("Pulling changes from Overleaf...")
        if new_remote_overleaf_rev:
            self._pull(dry_run=dry_run)
        if prune:
            self._pull_prune(dry_run=dry_run)
        self.logger.debug("Rebasing working branch after pulling...")
        if self.git_broker.rebase_working_branch():
            self.logger.debug("Switching back to working branch without rebasing after pulling...")
            self.git_broker.switch_to_working_branch()
        else:
            return ErrorNumber.PULL_ERROR
        self._pull_push_stash_pop(stash)

        self.logger.info("Successfully pulled changes from Overleaf")
        return ErrorNumber.OK

    @property
    def empty_folders(self) -> list[str]:
        empty_folders: list[str] = []

        def _traverse_folders(folder_json: dict, parent_folder_pathname: str = "") -> None:
            folder_pathname = (
                f"{parent_folder_pathname}/{folder_json['name']}" if parent_folder_pathname else folder_json["name"]
            )

            for sub_folder in folder_json["folders"]:
                _traverse_folders(sub_folder, folder_pathname)
                if sub_folder["_id"] not in empty_folders:
                    all_sub_folders_empty = False
                    break
            else:
                all_sub_folders_empty = True

            if all_sub_folders_empty and not folder_json["fileRefs"] and not folder_json["docs"]:
                empty_folders.append(folder_pathname)

        # Start checking from the root level folders
        for folder in self.overleaf_broker.root_folder_json["folders"]:
            _traverse_folders(folder)

        return empty_folders

    def _push(self, dry_run: bool) -> None:
        """Perform push operation"""
        self.logger.info("Pushing changes to Overleaf...")
        self.git_broker.switch_to_working_branch()
        delete_list: list[str] = []
        upload_list: list[str] = []
        for line in self.git_broker.working_branch_status:
            self.logger.info("status: %s", line)
            columns = line.split("\t")
            status = columns[0][0]
            match status:
                case "M" | "A":
                    assert len(columns) == 2
                    pathname = columns[1]
                    upload_list.append(pathname)
                case "D":
                    assert len(columns) == 2
                    pathname = columns[1]
                    delete_list.append(pathname)
                case "R":
                    assert len(columns) == 3
                    old_pathname, new_pathname = columns[1], columns[2]
                    delete_list.append(old_pathname)
                    upload_list.append(new_pathname)
                case _:
                    raise ValueError(f"Unsupported status: {status}")

        assert len(delete_list) + len(upload_list) > 0

        for pathname in delete_list:
            self.overleaf_broker.delete(pathname, dry_run)
        for pathname in upload_list:
            self.overleaf_broker.upload(pathname, dry_run)
        # It is possible that the refresh happened after changes from other remote overleaf users
        # The push verification may fail in this case
        self.overleaf_broker.refresh_updates()
        self.overleaf_broker.refresh_indexed_file_ids()

    def _push_prune(self, dry_run: bool) -> None:
        """Prune empty folders from Overleaf"""
        self.logger.info("Pruning empty folders from Overleaf...")
        for pathname in self.empty_folders:
            self.overleaf_broker.delete(pathname, dry_run)

    def push(self, prune=False, dry_run=False) -> ErrorNumber:
        if not self.initialized:
            self.logger.error("Project not initialized. Please run `init` first")
            shutil.rmtree(self.overleaf_sync_dir)
            return ErrorNumber.NOT_INITIALIZED_ERROR

        if prune:
            self._push_prune(dry_run)
            return ErrorNumber.OK

        # Check if there are new changes to push
        if not self.new_working_commit_exists and not prune:
            self.logger.info("No new changes to push")
            return ErrorNumber.OK

        # Check if there are new changes in remote overleaf
        if self.new_remote_overleaf_rev:
            self.logger.error("There are new remote overleaf updates. Please pull first")
            return ErrorNumber.PUSH_ERROR

        # Perform stash before pushing to prevent uncommitted changes in working branch
        # Reuse `stash` to check if there are stashed changes
        stash = self._pull_push_stash()
        if self.new_working_commit_exists:
            self._push(dry_run=dry_run)
            self._pull(dry_run=dry_run)
        if prune:
            self._push_prune(dry_run=dry_run)
            self._pull_prune(dry_run=dry_run)
        if not self.git_broker.is_identical_working_overleaf:
            self.logger.warning("Working branch is not identical to overleaf branch")
        self.git_broker.tag_working_branch(str(self.git_broker.local_overleaf_version))
        self.git_broker.rebase_working_branch()
        self._pull_push_stash_pop(stash)

        self.logger.info("Successfully pushed changes to Overleaf")
        return ErrorNumber.OK

    def sync(self, prune=False, dry_run=False) -> ErrorNumber:
        if (result := self.pull(prune=prune, dry_run=dry_run)) != ErrorNumber.OK:
            return result
        if (result := self.push(prune=prune, dry_run=dry_run)) != ErrorNumber.OK:
            return result
        self.logger.info("Successfully completed sync")
        return ErrorNumber.OK


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
    pull_parser.add_argument(
        "-n", "--no-stash", action="store_true", help="Do not stash changes in the working branch before "
    )
    pull_parser.add_argument("-p", "--prune", action="store_true", help="Prune empty folders")
    pull_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    push_parser = subparsers.add_parser("push", help="Push changes to Overleaf project")
    push_parser.add_argument("-p", "--prune", action="store_true", help="Prune empty folders")
    push_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    sync_parser = subparsers.add_parser("sync", help="Sync changes between Overleaf project and local git repo")
    sync_parser.add_argument("-p", "--prune", action="store_true", help="Prune empty folders")
    sync_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    # setup_logger(logging.getLogger(GitBroker.__qualname__), args.debug, args.log)
    # setup_logger(logging.getLogger(OverleafBroker.__qualname__), args.debug, args.log)
    # setup_logger(logging.getLogger(OverleafProject.__qualname__), args.debug, args.log)

    setup_logger(logging.getLogger(GitBroker.__qualname__), args.debug)
    setup_logger(logging.getLogger(OverleafBroker.__qualname__), args.debug)
    setup_logger(logging.getLogger(OverleafProject.__qualname__), args.debug)

    project = OverleafProject()

    match args.command:
        case "init":
            sys.exit(project.init(username=args.username, password=args.password, project_id=args.project_id))
        case "pull":
            sys.exit(project.pull(stash=(not args.no_stash), prune=args.prune, dry_run=args.dry_run))
        case "push":
            sys.exit(project.push(prune=args.prune, dry_run=args.dry_run))
        case "sync":
            sys.exit(project.sync(prune=args.prune, dry_run=args.dry_run))
        case _:
            parser.print_help()
