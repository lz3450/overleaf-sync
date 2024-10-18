#!/usr/bin/env bash

# Variables
USERNAME="lz3450@outlook.com"
PASSWORD="skzlj999"
PROJECT_ID="66ebe2622f3359beb0af1600"
OVERLEAF_ADDRESS="https://overleaf.s3lab.io"
# OVERLEAF_ADDRESS="http://localhost"
LOGIN_URL="$OVERLEAF_ADDRESS/login"
PROJECT_DOWNLOAD_URL="$OVERLEAF_ADDRESS/project/$PROJECT_ID/download/zip"
PROJECT_FILE_UPLOAD_URL="$OVERLEAF_ADDRESS/project/$PROJECT_ID/upload?folder_id=66fdd1610c3f448b0bccddc6"
COOKIE_FILE="cookies.txt"
ZIP_FILE="paper.zip"
WORKING_DIR="paper"
LAST_COMMIT_FILE=".last_commit"

# Function to fetch the CSRF token
fetch_csrf_token() {
    echo "Fetching CSRF token..."
    csrf_token=$(curl -c $COOKIE_FILE -s $LOGIN_URL | grep -oP '(?<=name="_csrf" type="hidden" value=")[^"]*')
    if [[ -z "$csrf_token" ]]; then
        echo "Failed to fetch CSRF token."
        exit 1
    fi
}

# Function to log in to Overleaf
login_to_overleaf() {
    echo "Logging in to Overleaf..."
    fetch_csrf_token
    curl -b $COOKIE_FILE -c $COOKIE_FILE -s -X POST $LOGIN_URL \
        -d "email=$USERNAME&password=$PASSWORD&_csrf=$csrf_token" > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "Login failed. Please check your credentials."
        exit 1
    fi
}

# Function to download the ZIP file from Overleaf
download_zip() {
    echo "Downloading project ZIP from Overleaf..."
    curl -b $COOKIE_FILE -s -o "$ZIP_FILE" $PROJECT_DOWNLOAD_URL
    if [[ $? -ne 0 ]]; then
        echo "Failed to download the project ZIP."
        exit 1
    fi
    echo "Project ZIP downloaded as $ZIP_FILE"
}

# Function to unzip the ZIP file and overwrite the existing files
unzip_and_overwrite() {
    echo "Unzipping and overwriting files in the '$WORKING_DIR' directory..."
    unzip -o "$ZIP_FILE" -d "$WORKING_DIR"
    if [[ $? -ne 0 ]]; then
        echo "Failed to unzip the project files."
        exit 1
    fi
}

# Function to record the current Git commit hash
record_last_commit() {
    echo "Recording the current commit hash..."
    git rev-parse HEAD > "$LAST_COMMIT_FILE"
}

# Function to identify changed files within the 'paper/' directory since the last commit
identify_changed_files() {
    if [[ ! -f "$LAST_COMMIT_FILE" ]]; then
        echo "No previous commit recorded. Uploading all files under paper/..."
        git ls-files "$WORKING_DIR" > "changed_files.txt"
    else
        echo "Identifying changed files in the paper/ directory since the last commit..."
        git diff --name-only "$(cat $LAST_COMMIT_FILE)" -- "$WORKING_DIR" > "changed_files.txt"
    fi

    # Filter out files that are no longer in the repo (e.g., deleted files) from the list
    sed -i '/^$/d' changed_files.txt
}

# Function to upload individual files to Overleaf
upload_files() {
    echo "Uploading changed files to Overleaf..."

    # while IFS= read -r file; do
    #     echo "Uploading $file..."
    #     fetch_csrf_token
    #     curl -v -A 'Mozilla/5.0' -b $COOKIE_FILE -H "Content-Type: multipart/form-data" \
    #         -F "name=$(basename $file)" \
    #         -F "type=text/plain" \
    #         -F "qqfile=@$file" \
    #          "$PROJECT_FILE_UPLOAD_URL"
    #     if [[ $? -ne 0 ]]; then
    #         echo "Failed to upload $file."
    #         exit 1
    #     fi
    # done < "changed_files.txt"

    curl -X POST "https://overleaf.s3lab.io/project/66ebe2622f3359beb0af1600/upload?folder_id=66ebe2622f3359beb0af15ff" \
    -H "accept: */*" \
    -H "accept-encoding: gzip, deflate, br, zstd" \
    -H "accept-language: en-US,en;q=0.9,zh;q=0.8" \
    -H "origin: https://overleaf.s3lab.io" \
    -H "referer: https://overleaf.s3lab.io/project/66ebe2622f3359beb0af1600" \
    -H "sec-fetch-dest: empty" \
    -H "sec-fetch-mode: cors" \
    -H "sec-fetch-site: same-origin" \
    -H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0" \
    -H "x-csrf-token: eB10mvg8-TkiEtF_ST6lY82k2XayIHuDQ5iA" \
    -F "relativePath=null" \
    -F "name=ol" \
    -F "type=application/octet-stream" \
    -F "qqfile=@ol" \
    -b cookie.txt

    echo "File upload completed."
}

# Function to clean up temporary files
clean_up() {
    echo "Cleaning up temporary files..."
    rm -f $COOKIE_FILE $ZIP_FILE changed_files.txt
}

# Option parser
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  download     Download the project ZIP from Overleaf and record commit hash"
    echo "  upload       Identify changed files and upload them to Overleaf"
    echo "  sync         Download, unzip, commit, identify changed files, and upload"
    echo "  clean        Clean up temporary files"
    exit 1
}

# Main script logic
case "$1" in
    download)
        login_to_overleaf
        download_zip
        unzip_and_overwrite
        record_last_commit
        ;;
    upload)
        login_to_overleaf
        identify_changed_files
        upload_files
        ;;
    sync)
        login_to_overleaf
        download_zip
        unzip_and_overwrite
        record_last_commit
        identify_changed_files
        upload_files
        ;;
    clean)
        clean_up
        ;;
    *)
        usage
        ;;
esac
