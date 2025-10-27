#!/bin/bash
# check param. must be a file existing
# check 2nd param must be a directory path
# 3. param optional "sqlite"
# 4. remote host-name
# 5. remote ssh sqlite eval hostname

# make rsync of file to transfer1:<2nd-param>
# if sqlite run on ssh server "mn5_gpp1" "module load sqlite3" and check integrity of new database
source util.sh

# Check if at least 4 parameters are provided
if [ $# -lt 4 ]; then
    echo "Error: Insufficient parameters provided (got $#, need at least 4)"
    echo "Usage: $0 <source_file> <destination_dir> [sqlite] <rsync_hostname> [ssh_hostname]"
    echo
    echo "Parameters provided:"
    [ $# -ge 1 ] && echo "  1. source_file: $1" || echo "  1. source_file: MISSING"
    [ $# -ge 2 ] && echo "  2. destination_dir: $2" || echo "  2. destination_dir: MISSING"
    [ $# -ge 3 ] && echo "  3. sqlite_check: $3" || echo "  3. sqlite_check: MISSING"
    [ $# -ge 4 ] && echo "  4. rsync_hostname: $4" || echo "  4. rsync_hostname: MISSING"
    [ $# -ge 5 ] && echo "  5. ssh_hostname: $5" || echo "  5. ssh_hostname: (optional)"
    echo
    echo "Parameter descriptions:"
    echo "  source_file: Path to existing file to backup"
    echo "  destination_dir: Remote directory path for backup"
    echo "  sqlite_check: 'sqlite' to enable SQLite integrity check, 'nosqlite' to skip"
    echo "  rsync_hostname: Remote hostname for rsync transfer"
    echo "  ssh_hostname: Optional hostname for SQLite integrity check (defaults to rsync_hostname)"
    exit 1
fi

# Get parameters
source_file="$1"
dest_dir="$2"
check_sqlite="$3"
rsync_hostname="$4"
ssh_hostname="${5:-$rsync_hostname}"  # Default to rsync_hostname if not provided

# Check if source file exists
if [ ! -f "$source_file" ]; then
    echo "Error: Source file does not exist: $source_file"
    exit 1
fi

echo -e "${BLUE}Starting backup process...${NC}"
echo -e "${BLUE}Source file: ${YELLOW}$source_file${NC}"
echo -e "${BLUE}Destination directory: ${YELLOW}$rsync_hostname:$dest_dir${NC}"
echo -e "${BLUE}SQLite check hostname: ${YELLOW}$ssh_hostname${NC}"

# Check if destination directory exists on remote server
#echo "Checking if destination directory exists on $rsync_hostname..."
#if ssh -n "$rsync_hostname" "[ -d '$dest_dir' ]"; then
#    echo "Destination directory exists: $dest_dir"
#else
#    echo "Error: Destination directory does not exist on $rsync_hostname: $dest_dir"
#    exit 1
#fi

# Perform rsync to remote server directory
echo -e "${GREEN}Performing rsync...${NC}"
# Use --itemize-changes to detect if file was actually transferred
rsync_output=$(rsync -avz --progress --itemize-changes "$source_file" "$rsync_hostname:$dest_dir/" 2>&1)
rsync_result=$?

if [ $rsync_result -eq 0 ]; then
    echo -e "${GREEN}Rsync completed successfully${NC}"

    # Check if file was actually transferred (look for >f or .f in itemize output)
    filename=$(basename "$source_file")
    if echo "$rsync_output" | grep -q "^>f\|^\.f.*$filename"; then
        file_was_updated=true
        echo -e "${GREEN}File was transferred/updated${NC}"
    else
        file_was_updated=false
        echo -e "${BLUE}File was not updated (already up to date)${NC}"
    fi
else
    echo -e "${RED}Error: Rsync failed${NC}"
    exit 1
fi

# If sqlite parameter is provided, check database integrity on remote server
if [ "$check_sqlite" = "sqlite" ]; then
    if [ "$file_was_updated" = true ]; then
        echo
        echo -e "${BLUE}SQLite integrity check requested (file was updated)...${NC}"

        # Extract filename from source_file path for remote check
        filename=$(basename "$source_file")
        remote_file_path="$dest_dir/$filename"

        # Run SQLite integrity check on remote server
        if ssh -n "$ssh_hostname" "module load sqlite3 && sqlite3 '$remote_file_path' 'PRAGMA integrity_check;'"; then
            echo -e "${GREEN}SQLite integrity check completed${NC}"
        else
            echo -e "${RED}Error: SQLite integrity check failed or could not connect to ${YELLOW}$ssh_hostname${NC}"
            exit 1
        fi
    else
        echo
        echo -e "${BLUE}Skipping SQLite integrity check (file was not updated)${NC}"
    fi
fi

echo
echo -e "${GREEN}Backup process completed successfully!${NC}"

