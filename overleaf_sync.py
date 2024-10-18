import os
import subprocess
import argparse
import requests
import json
from bs4 import BeautifulSoup
import zipfile
import websocket


OVERLEAF_URL = "https://overleaf.s3lab.io"
LOGIN_URL = f"{OVERLEAF_URL}/login"
PROJECTS_URL = f"{OVERLEAF_URL}/project"

SCRIPT_DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.json")
ZIP_FILE = os.path.join(SCRIPT_DIR, "latex.zip")
LATEX_PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LATEST_COMMIT_FILE = os.path.join(SCRIPT_DIR, "latest_commit.txt")
REMOTE_VERSION_FILE = os.path.join(SCRIPT_DIR, "remote_version.json")
IDS_FILE = os.path.join(SCRIPT_DIR, "file_ids.json")


class OverleafProject:
    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9,zh;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
            }
        )
        self._root_folder_id = hex(int(self.project_id, 16) - 1)[2:].lower()
        self._csrf_token = None

    @property
    def project_url(self) -> str:
        return f"{PROJECTS_URL}/{self.project_id}"

    @property
    def download_url(self) -> str:
        return f"{PROJECTS_URL}/{self.project_id}/download/zip"

    @property
    def upload_url(self) -> str:
        return f"{PROJECTS_URL}/{self.project_id}/upload"

    @property
    def updates_url(self) -> str:
        return f"{PROJECTS_URL}/{self.project_id}/updates"

    @property
    def root_folder_id(self) -> str:
        return self._root_folder_id

    def login(self, username: str, password: str) -> None:
        print("Logging in to Overleaf...")
        response = self.session.get(LOGIN_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        csrf_token: str
        csrf_token = soup.find("input", {"name": "_csrf"})["value"]  # type: ignore
        if not csrf_token:
            raise RuntimeError("Failed to fetch CSRF token.")
        payload = {"email": username, "password": password, "_csrf": csrf_token}
        response = self.session.post(LOGIN_URL, data=payload)
        response.raise_for_status()
        print("Login successful.")

    @property
    def csrf_token(self) -> str:
        if self._csrf_token:
            return self._csrf_token
        response = self.session.get(self.project_url)
        soup = BeautifulSoup(response.text, "html.parser")
        _csrf_token: str
        _csrf_token = soup.find("meta", {"name": "ol-csrfToken"})["content"]  # type: ignore
        if not _csrf_token:
            raise ValueError("Failed to fetch CSRF token.")
        self._csrf_token = _csrf_token
        return self._csrf_token

    def download(self) -> None:
        print(f"Downloading project ZIP from url: {self.download_url}...")
        response = self.session.get(self.download_url)
        response.raise_for_status()
        with open(ZIP_FILE, "wb") as f:
            f.write(response.content)
        print(f"Project ZIP downloaded as {ZIP_FILE}")

    def unzip(self) -> None:
        print(f"Unzipping file {ZIP_FILE} to directory {LATEX_PROJECT_DIR}...")
        with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
            zip_ref.extractall(LATEX_PROJECT_DIR)
        print(f"Files unzipped to {LATEX_PROJECT_DIR}.")

    def upload(self, file_path: str) -> None:
        print(f"Uploading files from {LATEX_PROJECT_DIR} to Overleaf project {self.project_id}...")

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
        response = self.session.post(self.upload_url, headers=headers, params=params, data=data, files=files)
        qqfile.close()
        response.raise_for_status()

    def folder(self, path: str) -> None:
        print(f"Creating folder {path} in Overleaf project {self.project_id}...")
        parent_folder_id, type = self._find_id_type(os.path.dirname(path))
        assert type == "folder"

        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        data = {"name": os.path.basename(path), "parent_folder_id": parent_folder_id}
        response = self.session.post(f"{self.project_url}/folder", headers=headers, data=data)
        response.raise_for_status()

    def delete(self, id: str, type: str) -> None:
        print(f"Deleting object {id} from Overleaf project {self.project_id}...")

        headers = {
            "Accept": "application/json",
            "Origin": OVERLEAF_URL,
            "Referer": self.project_url,
            "X-CSRF-TOKEN": self.csrf_token,
        }
        if type not in ["file", "doc", "folder"]:
            raise ValueError(f"Invalid type: {type}")
        response = self.session.delete(f"{self.project_url}/{type}/{id}", headers=headers)
        response.raise_for_status()

    def _get_file_ids(self) -> dict[str, dict[str, str]]:
        print(f"Fetching document IDs from Overleaf project {self.project_id}...")
        response = self.session.get(f"{OVERLEAF_URL}/socket.io/1/?projectId={self.project_id}")
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
        root_folder_data: dict
        root_folder_data = json.loads(data[4:])["args"][0]["project"]["rootFolder"][0]
        assert self.root_folder_id == root_folder_data["_id"]
        root_folder_id = root_folder_data["_id"]

        ids = {"folders": {}, "fileRefs": {}, "docs": {}}

        def _restructure(folder_data: dict, current_folder="") -> None:
            if folder_data["_id"] != root_folder_id:
                current_folder = f'{current_folder}{folder_data["name"]}/'
            for folder in folder_data.get("folders", []):
                ids["folders"][f'{current_folder}{folder["name"]}'] = folder["_id"]
                _restructure(folder, current_folder)
            for doc in folder_data.get("docs", []):
                ids["docs"][f'{current_folder}{doc["name"]}'] = doc["_id"]
            for file_ref in folder_data.get("fileRefs", []):
                ids["fileRefs"][f'{current_folder}{file_ref["name"]}'] = file_ref["_id"]

        _restructure(root_folder_data)

        # with open(IDS_FILE, "w") as f:
        #     json.dump(ids, f)
        return ids

    def _find_id_type(self, path: str) -> tuple[str, str]:
        ids = self._get_file_ids()
        if path in ids["fileRefs"]:
            return ids["fileRefs"][path], "file"
        elif path in ids["docs"]:
            return ids["docs"][path], "doc"
        elif path in ids["folders"]:
            return ids["folders"][path], "folder"
        else:
            raise ValueError(f"No id found for `{path}`")

    def _get_remote_version(self) -> int:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        }
        response = self.session.get(self.updates_url, headers=headers)
        response.raise_for_status()
        return json.loads(response.text)["updates"][0]["toV"]

    def _get_changed_files(self) -> str:
        if not os.path.exists(LATEST_COMMIT_FILE):
            raise RuntimeError(f"File {LATEST_COMMIT_FILE} does not exist.")
        with open(LATEST_COMMIT_FILE, "r") as f:
            latest_commit_id = f.read().strip()
        print(f"Latest commit ID: {latest_commit_id}")

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
            print(f"File {REMOTE_VERSION_FILE} does not exist, treating as updated.")
            return False
        with open(REMOTE_VERSION_FILE, "r") as f:
            recorded_remote_version = json.load(f)
        remote_version = int(self._get_remote_version())
        print(f"Recorded remote version: {recorded_remote_version}")
        print(f"Remote version: {remote_version}")
        assert remote_version >= recorded_remote_version
        return remote_version > recorded_remote_version

    def pull(self) -> None:
        if not os.path.exists(os.path.join(LATEX_PROJECT_DIR, ".git")):
            os.makedirs(LATEX_PROJECT_DIR, exist_ok=True)
            print(f"Initializing git repository in {LATEX_PROJECT_DIR}...")
            subprocess.run(["git", "init", LATEX_PROJECT_DIR], check=True)
            print("Git repository initialized.")
        else:
            if not self.is_remote_updated:
                print("No updates available.")
                return
            if not self.is_local_clean:
                raise RuntimeError("Cannot pull changes to a dirty repository.")
        self.download()
        self.unzip()
        remote_version = self._get_remote_version()
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
            print(f"Writing latest commit ID to {LATEST_COMMIT_FILE}: {self.latest_commit_id}")
            f.write(self.latest_commit_id)

    def push(self) -> None:
        if not os.path.exists(os.path.join(LATEX_PROJECT_DIR, ".git")):
            raise RuntimeError(f"LaTeX project directory `{LATEX_PROJECT_DIR}` does not initialized.")
        if not self.is_local_clean:
            raise RuntimeError("Cannot push changes from a dirty repository.")
        if self.is_remote_updated:
            raise RuntimeError("Cannot push changes to a updated remote repository.")
        for line in self._get_changed_files().split("\n"):
            if not line:
                continue
            status, file_path = line.split("\t")
            print(f"{status}\t{file_path}")
            if status == "D":
                file_id, type = self._find_id_type(file_path)
                print(f"Deleting object {file_id} of type {type}")
                self.delete(file_id, type)
            else:
                self.upload(file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="overleaf-sync.py", description="Overleaf Project Sync Tool")
    parser.add_argument("command", choices=["pull", "push", "sync", "dummy"], help="Command to execute")
    args = parser.parse_args()

    with open(CONFIG_FILE, "r") as config_file:
        config = json.load(config_file)

    project = OverleafProject(config["project_id"])
    project.login(config["username"], config["password"])

    match args.command:
        case "pull":
            project.pull()
        case "push":
            project.push()
            project.pull()
        case "sync":
            pass
        case "dummy":
            pass
