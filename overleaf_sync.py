#!/usr/bin/env python3.12

################################################################
### Name: overleaf-sync.py
### Description: Overleaf Project Sync Tool
### Author: KZL
################################################################

from __future__ import annotations

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

from itertools import takewhile
from enum import IntEnum, unique
from time import sleep, time
from datetime import datetime
from bs4 import BeautifulSoup


OVERLEAF_URL = "https://overleaf.s3lab.io"
LOGIN_URL = f"{OVERLEAF_URL}/login"
PROJECTS_URL = f"{OVERLEAF_URL}/project"

OVERLEAF_SYNC_DIR_NAME = ".overleaf-sync"

LOGGER = logging.getLogger(__name__)
LOGGER_FORMATTER = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
LOG_DIR = os.path.join(OVERLEAF_SYNC_DIR_NAME, "logs")


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
    WORKING_BRANCH_START_COMMIT_TAG = "ws"

    def __init__(self, working_dir, overleaf_branch="overleaf", working_branch="working") -> None:
        self.working_dir = working_dir
        self.overleaf_branch = overleaf_branch
        self.working_branch = working_branch

    def __call__(self, *args: str, check=True) -> str:
        cmd = ["git", "-C", self.working_dir, *args]
        LOGGER.debug("Git command: %s", " ".join(cmd))
        try:
            output = subprocess.run(cmd, capture_output=True, text=True, check=check).stdout.strip()
        except subprocess.CalledProcessError as e:
            LOGGER.error("Git command failed: %s\noutput:\n%s\n---", e, e.output)
            traceback.print_stack()
            exit(ErrorNumber.GIT_ERROR)
        LOGGER.debug("Git output: \n%s", output)
        return output

    def init(self, force=False) -> None:
        if os.path.exists(os.path.join(self.working_dir, ".git")):
            if force:
                LOGGER.warning("Reinitializing git repository in %s...", self.working_dir)
                shutil.rmtree(os.path.join(self.working_dir, ".git"))
            else:
                LOGGER.error("Git repository already exists in %s. Exiting...", self.working_dir)
                exit(ErrorNumber.REINITIALIZATION_ERROR)
        self("init", "-b", self.overleaf_branch)

    def sanity_check(self) -> None:
        # Check if git repository is initialized
        if not os.path.exists(os.path.join(self.working_dir, ".git")):
            LOGGER.error(
                "Git is not initialized for LaTeX project in directory `%s`. Please reinitialize the project.",
                self.working_dir,
            )
            exit(ErrorNumber.GIT_DIR_CORRUPTED_ERROR)
        # Check if both overleaf branch and working branch exist
        branches = [_.lstrip("*").strip() for _ in self("branch", "--list").splitlines()]
        if not (self.overleaf_branch in branches and self.working_branch in branches):
            LOGGER.error(
                "Branches `%s` or `%s` are missing. Working directory corrupted. Please reinitialize the project.",
                self.overleaf_branch,
                self.working_branch,
            )
            exit(ErrorNumber.WKDIR_CORRUPTED_ERROR)

    @property
    def managed_files(self) -> list[str]:
        return self("ls-files").splitlines()

    def add_all(self) -> None:
        self("add", ".")

    def commit(self, msg: str, ts: int, name: str, email: str) -> None:
        self(
            "commit",
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

    def update_working_branch(self) -> None:
        self("branch", "-f", self.working_branch, self.overleaf_branch)
        self("tag", "-f", self.WORKING_BRANCH_START_COMMIT_TAG, self.overleaf_branch)

    def switch_to_working_branch(self, update=False) -> None:
        if update:
            self.update_working_branch()
        self("switch", self.working_branch)

    @property
    def local_overleaf_rev(self) -> int:
        return int(self("log", "-1", "--pretty=%B", self.overleaf_branch).split("->")[1])

    @property
    def local_overleaf_previous_rev(self) -> int:
        return int(self("log", "-1", "--pretty=%B", self.overleaf_branch).split("->")[0])

    def reset_hard(self, n: int) -> None:
        self("reset", "--hard", f"HEAD~{n}")

    def rebase_working_branch(self) -> None:
        # Rebase working branch to overleaf branch
        result = self("rebase", self.overleaf_branch, self.working_branch, check=False)
        if "CONFLICT" in result:
            LOGGER.error(
                "Failed to rebase `%s` to branch `%s`.\n" "%s\n" "Fix conflicts and run `git rebase --continue`.",
                self.working_branch,
                self.overleaf_branch,
                result,
            )
            exit(ErrorNumber.GIT_ERROR)

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
        if self("stash").startswith("No local changes to save"):
            return False
        return True

    def stash_pop_working(self) -> None:
        assert self.current_branch == self.working_branch
        self("stash", "pop")

    @property
    def working_branch_status(self) -> list[str]:
        assert self.current_branch == self.working_branch
        return self("diff", "--name-status", self.starting_working_commit).splitlines()


class OverleafBroker:
    def __init__(self, working_dir: str, overleaf_sync_dir: str) -> None:
        self.working_dir = working_dir
        self.overleaf_sync_dir = overleaf_sync_dir
        self.updates_file = os.path.join(self.overleaf_sync_dir, "updates.json")
        self.overleaf_zip = os.path.join(self.overleaf_sync_dir, "overleaf.zip")
        self.ids_file = os.path.join(self.overleaf_sync_dir, "ids.json")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
            }
        )
        self.username: str | None = None
        self.password: str | None = None
        self.project_id: str | None = None
        self._logged_in = False
        self._updates: list[dict] | None = None
        self._csrf_token: str | None = None
        self._original_file_ids: dict | None = None
        self._root_folder_id: str | None = None
        self._indexed_file_ids: dict[str, dict[str, str]] | None = None

    @property
    def project_url(self) -> str:
        return f"{PROJECTS_URL}/{self.project_id}"

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        if not self._logged_in:
            LOGGER.error("Not logged in. Please login first.")
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
    def updates(self) -> list[dict]:
        if self._updates:
            return self._updates

        url = f"{PROJECTS_URL}/{self.project_id}/updates"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        }
        LOGGER.debug("Fetching project updates from %s...", url)
        response = self._get(url, headers=headers)
        self._updates = json.loads(response.text)["updates"]
        if not self._updates:
            raise ValueError("Failed to fetch project updates.")
        with open(self.updates_file, "w") as f:
            json.dump(self._updates, f)
        return self._updates

    def refresh_updates(self) -> None:
        self._updates = None

    def download_zip(self, revision: int | None = None) -> float:
        """
        Download the project/revision ZIP file from Overleaf.
        There is a rate limit of 30 request per hour when downloading revision ZIP.
        """
        url = f"{self.project_url}/version/{revision}/zip" if revision else f"{self.project_url}/download/zip"
        LOGGER.debug("Downloading project ZIP from url: %s...", url)
        response = self._get(url)
        ts = time()
        with open(self.overleaf_zip, "wb") as f:
            f.write(response.content)
        LOGGER.debug("Project ZIP downloaded as %s", self.overleaf_zip)
        return ts

    def unzip(self, file_list: list | None = None) -> list[str]:
        """
        Unzip the downloaded ZIP file to the LaTeX project directory.
        `file_list`: List of files to extract. If `None`, extract all files.
        """
        LOGGER.debug("Unzipping file %s to directory %s...", self.overleaf_zip, self.working_dir)
        with zipfile.ZipFile(self.overleaf_zip, "r") as zip_ref:
            if file_list:
                # TODO: not tested
                for file in zip_ref.filelist:
                    LOGGER.debug("Extracting %s...", file)
                    zip_ref.extract(file, self.working_dir)
            else:
                zip_ref.extractall(self.working_dir)
            # Delete files not in the ZIP file
            return [zip_info.filename for zip_info in zip_ref.filelist]

    @property
    def csrf_token(self) -> str:
        if self._csrf_token:
            return self._csrf_token
        response = self._get(self.project_url)
        soup = BeautifulSoup(response.text, "html.parser")
        self._csrf_token = soup.find("meta", {"name": "ol-csrfToken"})["content"]  # type: ignore
        if not self._csrf_token:
            raise ValueError("Failed to fetch CSRF token.")
        return self._csrf_token

    def filetree_diff(self, from_: int, to_: int) -> list[dict]:
        LOGGER.debug("Fetching filetree diff from %d to %d...", from_, to_)
        url = f"{PROJECTS_URL}/{self.project_id}/filetree/diff?from={from_}&to={to_}"
        headers = {
            "Accept": "application/json",
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._get(url, headers=headers)
        return json.loads(response.text)["diff"]

    def diff(self, from_: int, to_: int, pathname: str) -> list[dict]:
        LOGGER.debug("Fetching diff of file %s from %d to %d...", pathname, from_, to_)
        url = f"{PROJECTS_URL}/{self.project_id}/diff?from={from_}&to={to_}&pathname={urllib.parse.quote(pathname)}"
        headers = {
            "Accept": "application/json",
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        response = self._get(url, headers=headers)
        response.raise_for_status()
        return json.loads(response.text)["diff"]

    def _find_id_type(self, path: str) -> tuple[str, str]:
        LOGGER.debug("Finding id for `%s`...", path)
        ids = self.indexed_file_ids
        if path in ids["fileRefs"]:
            return ids["fileRefs"][path], "file"
        elif path in ids["docs"]:
            return ids["docs"][path], "doc"
        elif path in ids["folders"]:
            return ids["folders"][path], "folder"
        else:
            raise ValueError(f"No id found for `{path}`")

    def download_binary_file(self, pathname: str) -> None:
        LOGGER.debug("Downloading binary file %s...", pathname)
        id, type = self._find_id_type(pathname)
        url = f"{PROJECTS_URL}/{self.project_id}/file/{id}"
        headers = {
            "Accept": "*/*",
            "Referer": self.project_url,
        }
        response = self._get(url, headers=headers)
        response.raise_for_status()
        if dirname := os.path.dirname(pathname):
            os.makedirs(dirname, exist_ok=True)
        with open(os.path.join(self.working_dir, pathname), "wb") as f:
            f.write(response.content)

    @property
    def original_file_ids(self) -> dict:
        if self._original_file_ids:
            return self._original_file_ids

        LOGGER.debug("Fetching document IDs from Overleaf project %s...", self.project_id)
        response = self._get(f"{OVERLEAF_URL}/socket.io/1/?projectId={self.project_id}")
        ws_id = response.text.split(":")[0]
        ws = websocket.create_connection(
            f"wss://overleaf.s3lab.io/socket.io/1/websocket/{ws_id}?projectId={self.project_id}"
        )
        data: str
        while True:
            data = ws.recv()  # type: ignore
            if data.startswith("5:::"):
                break
        ws.close()
        self._original_file_ids = json.loads(data[4:])["args"][0]["project"]["rootFolder"][0]

        with open(self.ids_file, "w") as f:
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

    def upload(self, pathname: str, dry_run=False) -> None:
        """
        Upload the file to the Overleaf project.
        There is a rate limit of 200 request per 15 minutes.
        """
        LOGGER.info("Uploading %s...", pathname)
        if dry_run:
            return

        relative_path = "null" if not os.path.dirname(pathname) else pathname
        file_name = os.path.basename(pathname)

        url = f"{PROJECTS_URL}/{self.project_id}/upload"
        headers = {
            "Accept": "*/*",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        params = {"folder_id": self.root_folder_id}
        data = {"relativePath": f"{relative_path}", "name": file_name, "type": "application/octet-stream"}
        qqfile = open(os.path.join(self.working_dir, pathname), "rb")
        files = {"qqfile": (file_name, qqfile, "application/octet-stream")}
        response = self._post(url, headers=headers, params=params, data=data, files=files)
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

    def refresh_indexed_file_ids(self) -> None:
        self._original_file_ids = None
        self._indexed_file_ids = None

    def create_folder(self, path: str, dry_run=False) -> None:
        LOGGER.info("Creating folder %s...", path)
        if dry_run:
            return
        parent_folder_id, type = self._find_id_type(os.path.dirname(path))
        assert type == "folder"

        url = f"{self.project_url}/folder"
        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        data = {"name": os.path.basename(path), "parent_folder_id": parent_folder_id}
        response = self._post(url, headers=headers, data=data)
        response.raise_for_status()

    def delete(self, path: str, dry_run=False) -> None:
        id, type = self._find_id_type(path)
        LOGGER.info('Deleting "%s": %s %s', type, path, id)

        if type == "folder" and input(
            f"Are you sure you want to delete folder {path}? (y/n): "
        ).strip().lower() not in ["y", "yes"]:
            LOGGER.info("Operation cancelled.")
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
        response = self._delete(url, headers=headers)
        response.raise_for_status()


class OverleafProject:
    def __init__(self, working_dir: str = ".") -> None:
        self.working_dir = working_dir
        self.overleaf_sync_dir = os.path.join(self.working_dir, OVERLEAF_SYNC_DIR_NAME)
        self.config_file = os.path.join(self.overleaf_sync_dir, "config.json")

        # Initialize git broker
        self.git_broker = GitBroker(self.working_dir)
        # Initialize overleaf broker
        self.overleaf_broker = OverleafBroker(self.working_dir, self.overleaf_sync_dir)
        if not os.path.exists(self.config_file):
            return

        self.sanity_check()
        with open(self.config_file, "r") as f:
            config: dict[str, str] = json.load(f)
        self.overleaf_broker.login(config["username"], config["password"], config["project_id"])

    def sanity_check(self) -> None:
        # git repo
        self.git_broker.sanity_check()

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
        extracted_pathnames = self.overleaf_broker.unzip()
        # Delete files not in the ZIP
        for managed_file in self.git_broker.managed_files:
            if managed_file not in extracted_pathnames:
                LOGGER.info("Deleting file %s from filesystem...", managed_file)
                os.remove(os.path.join(self.working_dir, managed_file))
        name = ";".join(f"{user["last_name"]}, {user["first_name"]}" for user in revision["meta"]["users"])
        email = ";".join(user["email"] for user in revision["meta"]["users"])
        # _git("switch", self.overleaf_branch)
        self.git_broker.add_all()
        self.git_broker.commit(
            msg=f"{"old" if merge_old else revision['fromV']}->{toV}",
            ts=revision["meta"]["end_ts"] // 1000,
            name=name,
            email=email,
        )
        LOGGER.info("Revision %s migrated.", toV)
        return ts

    def _migrate_revisions_zip(self, revisions: list[dict]) -> None:
        """
        Migrate all the given revisions to git revisions.
        Note that this function is not responsible for switching branch.
        """

        def _sleep_until(ts: float) -> None:
            now = time()
            time_to_sleep = ts - now
            LOGGER.debug("Sleeping for %.3f seconds...", time_to_sleep)
            if time_to_sleep > 0:
                sleep(time_to_sleep)

        revision_length = len(revisions)
        for i, rev in enumerate(reversed(revisions)):
            LOGGER.info("%d revision(s) to be migrated (ZIP).", revision_length - i)
            ts = self._migrate_revision_zip(rev)
            if revision_length >= 30:
                _sleep_until(ts + 120)

    def _migrate_revision_diff(self, revision: dict) -> None:
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
            path = os.path.join(self.working_dir, pathname)
            match operation:
                case "added" | "edited":
                    LOGGER.debug("Add/Edit %s...", item["pathname"])
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w") as f:
                        f.write(_diff_to_content(diff))
                case "removed":
                    LOGGER.debug("Remove %s...", item["pathname"])
                    os.remove(path)
                case "renamed":
                    LOGGER.debug("Rename %s...", item["pathname"])
                    os.rename(path, os.path.join(self.working_dir, item["newPathname"]))
                case _:
                    raise ValueError(f"Unsupported operation: {item['operation']}")

        LOGGER.debug("Migrating (diff) revision %d->%d...", fromV, toV)
        # Operate files on filesystem
        filetree_diff = self.overleaf_broker.filetree_diff(fromV, toV)
        for item in filetree_diff:
            if not (operation := item.get("operation", None)):
                continue
            pathname = item["pathname"]
            diff = self.overleaf_broker.diff(fromV, toV, pathname)
            _migrate(operation, pathname, diff)
        # Make git commit
        self.git_broker.add_all()
        self.git_broker.commit(f"{fromV}->{toV}", ts, name, email)
        LOGGER.debug("Revision %d->%d migrated.", fromV, toV)

    def _migrate_revisions_diff(self, revisions: list[dict]) -> None:
        """
        Migrate all the given revisions to git revisions.
        Note that this function is **not** responsible for switching branch.
        """
        revision_length = len(revisions)
        log_msg = f"Migrating overleaf revision %{len(str(revision_length))}d/{revision_length}: %d->%d"
        for i, rev in enumerate(reversed(revisions)):
            LOGGER.info(log_msg, i + 1, rev["fromV"], rev["toV"])
            self._migrate_revision_diff(rev)

    @property
    def remote_overleaf_rev(self) -> int:
        return self.overleaf_broker.updates[0]["toV"]

    @property
    def is_there_new_working_commit(self) -> bool:
        return self.git_broker.current_working_commit != self.git_broker.starting_working_commit

    def _git_repo_init_zip(self, n_revisions: int) -> None:
        updates: list[dict] = self.overleaf_broker.updates
        updates_length = len(updates)
        # Fetch one more revision: old->xxx
        updates = updates[: n_revisions + 1 if n_revisions > 0 or n_revisions + 1 > updates_length else None]

        LOGGER.info("Migrating all older revisions into the first git revision...")
        self.git_broker.switch_to_overleaf_branch(create=True)
        self._migrate_revision_zip(updates[-1], merge_old=True)
        LOGGER.info("Migrating the rest of the revisions...")
        self._migrate_revisions_zip(updates[:-1])
        self.git_broker.switch_to_working_branch(update=True)

    def _git_repo_init_diff(self) -> None:
        LOGGER.info("Migrating all revisions...")
        self.git_broker.switch_to_overleaf_branch(create=True)
        self._migrate_revisions_diff(self.overleaf_broker.updates)
        self.git_broker.switch_to_working_branch(update=True)

    def init(self, username: str, password: str, project_id: str):
        LOGGER.info("Initializing working directory...")
        # Create overleaf-sync directory
        os.makedirs(self.overleaf_sync_dir, exist_ok=True)
        # Write `config.json`
        if os.path.exists(self.config_file):
            LOGGER.warning("Overwriting config file `%s`...", self.config_file)
        else:
            LOGGER.info("Saving config file to %s.", self.config_file)
        with open(self.config_file, "w") as f:
            json.dump({"username": username, "password": password, "project_id": project_id}, f)
        # Write `.gitignore`
        with open(os.path.join(self.overleaf_sync_dir, ".gitignore"), "w") as f:
            f.write("*")
        # Initialize git repo
        self.git_broker.init()
        # login overleaf broker
        self.overleaf_broker.login(username, password, project_id)
        # Migrate overleaf revisions to git repo
        # self._git_repo_init_zip(n_revisions)
        self._git_repo_init_diff()

    @property
    def is_there_new_remote_overleaf_rev(self) -> bool:
        local_overleaf_rev = self.git_broker.local_overleaf_rev
        remote_overleaf_rev = self.remote_overleaf_rev
        LOGGER.info("Fetched remote/local overleaf revision: %d/%d", remote_overleaf_rev, local_overleaf_rev)
        assert remote_overleaf_rev >= local_overleaf_rev
        return remote_overleaf_rev > local_overleaf_rev

    def pull(self, stash=True, dry_run=False, _stash_pop=True, _branch_switching=True, _branch_rebasing=True) -> None:
        if not self.is_there_new_remote_overleaf_rev:
            LOGGER.info("No new changes to pull.")
            return

        # Perform stash before pushing to prevent uncommitted changes in working branch
        if stash:
            LOGGER.info("Stashing changes before pushing.")
            self.git_broker.switch_to_working_branch()
            self.git_broker.stash_working()
        elif not self.git_broker.is_current_branch_clean:
            # Check if the working tree is clean
            LOGGER.error(
                "Cannot push changes from a dirty working tree. Either run without `--no-stash` or commit changes first."
            )
            exit(ErrorNumber.PULL_ERROR)

        LOGGER.info("Pulling changes from Overleaf...")

        # Get all new overleaf revisions
        upcoming_overleaf_rev = list(
            takewhile(
                lambda rev: rev["fromV"] >= self.git_broker.local_overleaf_previous_rev, self.overleaf_broker.updates
            )
        )

        LOGGER.debug(
            "%d upcoming revisions: %s",
            len(upcoming_overleaf_rev),
            ", ".join(f"{rev["fromV"]}->{rev["toV"]}" for rev in reversed(upcoming_overleaf_rev)),
        )

        if dry_run:
            return

        # The corresponding remove overleaf revision of latest local overleaf revision may changed after the migration
        # For example, 63->67 may become 63->64, 64->68
        self.git_broker.switch_to_overleaf_branch()
        if upcoming_overleaf_rev[0]["toV"] != self.git_broker.local_overleaf_rev:
            self.git_broker.reset_hard(1)
        else:
            del upcoming_overleaf_rev[0]

        self._migrate_revisions_diff(upcoming_overleaf_rev)

        if _branch_switching:
            self.git_broker.switch_to_working_branch()

        if _branch_rebasing:
            if self.is_there_new_working_commit:
                self.git_broker.rebase_working_branch()
            else:
                self.git_broker.switch_to_working_branch(update=True)

        if stash and _stash_pop:
            self.git_broker.stash_pop_working()

        # TODO: Implement prune

        def _finalize_push() -> None:
            # Verify the push
            assert not self.git_broker.is_diff_working_overleaf
            self.git_broker.rebase_working_branch()

        # Check if there are new changes to push
        if not self.is_there_new_working_commit:
            LOGGER.info("No new changes to push.")
            return

        self.git_broker.switch_to_working_branch()

        # Perform stash before pushing to prevent uncommitted changes in working branch
        if stash:
            LOGGER.info(
                "Stashing changes before pushing. Please run `git stash pop` after pushing to restore uncommitted changes."
            )
            self.git_broker.stash_working()
        elif not self.git_broker.is_current_branch_clean:
            # Check if the working tree is clean
            LOGGER.error(
                "Cannot push changes from a dirty working tree. Either run without `--no-stash` or commit changes first."
            )
            exit(ErrorNumber.PUSH_ERROR)

        # Perform force push
        if force:
            LOGGER.warning("Force pushing changes to Overleaf project...")
            LOGGER.warning("Not implemented yet.")
            _finalize_push()
            return

        # Perform pull before pushing to prevent new changes in remote overleaf
        if pull:
            self.pull(_branch_switching=True, _branch_rebasing=True)
        elif self.is_there_new_remote_overleaf_rev:
            # Check if there are new changes in remote overleaf
            LOGGER.error("There are new remote overleaf revisions. Please pull first.")
            exit(ErrorNumber.PUSH_ERROR)

        # Perform rebase before pushing to prevent unmerged changes in overleaf branch
        if rebase:
            self.git_broker.rebase_working_branch()

        delete_list: list[str] = []
        upload_list: list[str] = []
        for line in self.git_broker.working_branch_status:
            LOGGER.info("status: %s", line)
            columns = line.split("\t")
            status = columns[0]
            match status:
                case "M" | "A":
                    assert len(columns) == 2
                    pathname = columns[1]
                    upload_list.append(pathname)
                case "D":
                    assert len(columns) == 2
                    pathname = columns[1]
                    delete_list.append(pathname)
                case "R100":
                    assert len(columns) == 3
                    old_pathname, new_pathname = columns[1], columns[2]
                    delete_list.append(old_pathname)
                    upload_list.append(new_pathname)
                case _:
                    raise ValueError(f"Unsupported status: {status}")

        for pathname in delete_list:
            self.overleaf_broker.delete(pathname, dry_run)
        for pathname in upload_list:
            self.overleaf_broker.upload(pathname, dry_run)
        # It is possible that the refresh happened after changes from other remote overleaf users
        # The push verification may fail in this case
        self.overleaf_broker.refresh_updates()
        self.pull(dry_run=dry_run, _branch_switching=False, _branch_rebasing=False)
        _finalize_push()


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
    pull_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    push_parser = subparsers.add_parser("push", help="Push changes to Overleaf project")
    push_parser.add_argument(
        "-s", "--stash", action="store_true", help="Stash changes in the working branch before pushing"
    )
    push_parser.add_argument("-P", "--no-pull", action="store_true", help="Pull remote overleaf changes before pushing")
    push_parser.add_argument(
        "-R", "--no-rebase", action="store_true", help="Pull remote overleaf changes before pushing"
    )
    push_parser.add_argument("-f", "--force", action="store_true", help="Force push changes to Overleaf project")
    push_parser.add_argument("-p", "--prune", action="store_true", help="Prune empty folders")
    push_parser.add_argument("-d", "--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    setup_logger(LOGGER, args.debug)
    if args.log:
        setup_file_logger(LOGGER)

    project = OverleafProject()

    match args.command:
        case "init":
            project.init(username=args.username, password=args.password, project_id=args.project_id)
        case "pull":
            project.pull(dry_run=args.dry_run)
        case "push":
            project.push(
                stash=args.stash,
                pull=(not args.no_pull),
                rebase=(not args.no_rebase),
                force=args.force,
                dry_run=args.dry_run,
            )
        case _:
            parser.print_help()
