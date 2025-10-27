#!/bin/bash
# Batch Backup and Encryption Script
#
# USAGE:
#   ./batch_run.sh [OPTIONS]
#
# OPTIONS:
#   --backup, -b          Enable rsync backup phase
#   --encrypt, -e         Enable encryption phase
#   --both                Enable both backup and encryption phases
#   --backup-only         Enable only backup phase
#   --encrypt-only        Enable only encryption phase
#   --delete, -d          Delete original files after encryption (use with encrypt)
#   --help, -h            Show this help message
#
# EXAMPLES:
#   ./batch_run.sh --both --delete     # Do both backup and encryption, delete originals
#   ./batch_run.sh --backup-only       # Only backup, no encryption
#   ./batch_run.sh --encrypt-only -d   # Only encryption with deletion of originals
#   ./batch_run.sh -b -e -d            # Backup + encryption + delete using short flags
#   ./batch_run.sh --help              # Show help

# Start with strict mode for safety
set -euo pipefail
IFS=$'\n\t'

source config.sh
source util.sh

# Function to handle errors gracefully in loops
handle_error() {
    local exit_code=$1
    local operation=$2
    local file=$3

    if [ $exit_code -ne 0 ]; then
        echo -e "${RED}Failed: $operation for $file (exit code: $exit_code)${NC}"
        return 1
    fi
    return 0
}

# Function to show help
show_help() {
    echo -e "${BLUE}Batch Backup and Encryption Script${NC}"
    echo
    echo -e "${BLUE}USAGE:${NC}"
    echo "  $0 [OPTIONS]"
    echo
    echo -e "${BLUE}OPTIONS:${NC}"
    echo "  --backup, -b          Enable rsync backup phase"
    echo "  --encrypt, -e         Enable encryption phase"
    echo "  --both                Enable both backup and encryption phases"
    echo "  --backup-only         Enable only backup phase"
    echo "  --encrypt-only        Enable only encryption phase"
    echo "  --delete, -d          Delete original files after encryption (use with encrypt)"
    echo "  --help, -h            Show this help message"
    echo
    echo -e "${BLUE}EXAMPLES:${NC}"
    echo "  $0 --both --delete     # Do both backup and encryption, delete originals"
    echo "  $0 --backup-only       # Only backup, no encryption"
    echo "  $0 --encrypt-only -d   # Only encryption with deletion of originals"
    echo "  $0 -b -e -d            # Backup + encryption + delete using short flags"
    echo
    echo -e "${BLUE}DESCRIPTION:${NC}"
    echo "  This script processes files from the configured directory, optionally"
    echo "  backing them up via rsync and/or encrypting them on the remote server."
    echo "  Use the --delete flag to remove original files after encryption."
    exit 0
}

# Initialize variables
do_rsync="no"
do_encryption="no"
delete_originals="no"

# Parse command line arguments
if [ $# -eq 0 ]; then
    echo -e "${RED}Error: No options specified${NC}"
    echo -e "${BLUE}Use --help for usage information${NC}"
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --backup|-b)
            do_rsync="yes"
            shift
            ;;
        --encrypt|-e)
            do_encryption="yes"
            shift
            ;;
        --delete|-d)
            delete_originals="yes"
            shift
            ;;
        --both)
            do_rsync="yes"
            do_encryption="yes"
            shift
            ;;
        --backup-only)
            do_rsync="yes"
            do_encryption="no"
            shift
            ;;
        --encrypt-only)
            do_rsync="no"
            do_encryption="yes"
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo -e "${RED}Error: Unknown option '$1'${NC}"
            echo -e "${BLUE}Use --help for usage information${NC}"
            exit 1
            ;;
    esac
done

# Validate that at least one operation is enabled
if [[ "$do_rsync" == "no" && "$do_encryption" == "no" ]]; then
    echo -e "${RED}Error: At least one operation must be enabled${NC}"
    echo -e "${BLUE}Use --backup, --encrypt, or --both${NC}"
    exit 1
fi

# Validate that --delete is only used with encryption
if [[ "$delete_originals" == "yes" && "$do_encryption" == "no" ]]; then
    echo -e "${RED}Error: --delete can only be used with encryption enabled${NC}"
    echo -e "${BLUE}Use --encrypt or --both along with --delete${NC}"
    exit 1
fi

echo -e "${BLUE}=== Batch Processing Configuration ===${NC}"
echo -e "${BLUE}Rsync phase: $do_rsync${NC}"
echo -e "${BLUE}Encryption phase: $do_encryption${NC}"
echo -e "${BLUE}Delete originals: $delete_originals${NC}"
echo

# Arrays to track results
processed_files=()
failed_files=()

# Store all files to process in an array
echo -e "${BLUE}Finding files to process...${NC}"
files_to_process=()
while IFS= read -r -d '' file; do
    files_to_process+=("$file")
done < <(find "$BASE_VM_PHASE_1_DIR" -maxdepth 1 -type f -print0)

echo -e "${BLUE}Files to process (${#files_to_process[@]} total):${NC}"
for file in "${files_to_process[@]}"; do
    echo -e "  - ${YELLOW}$(basename "$file")${NC}"
done
echo

if [ "$do_rsync" = "yes" ]; then
    echo -e "${BLUE}=== PHASE 1: Backup files to remote server ===${NC}"

    # Check remote directory once before processing all files
    echo -e "${BLUE}Checking remote directory: ${YELLOW}${REMOTE_PHASE_1_DIR}${NC}"
    if ssh -n "${TRANSFER_NODE}" "[ -d '${REMOTE_PHASE_1_DIR}' ]"; then
        echo -e "${GREEN}Remote directory exists${NC}"
    else
        echo -e "${RED}Remote directory does not exist: ${YELLOW}${REMOTE_PHASE_1_DIR}${NC}"
        exit 1
    fi
    echo

    # Process each file
    for file in "${files_to_process[@]}"; do
        filename=$(basename "$file")
        echo -e "${BLUE}Backup: ${YELLOW}${filename}${NC}: ${YELLOW}$file${NC}"

        ./backup_dbs.sh "$file" "${REMOTE_PHASE_1_DIR}" sqlite "${TRANSFER_NODE}" "${GPFS_PROC_NODE}" 2>&1
        backup_result=$?
        echo -e "${BLUE}Backup result for ${YELLOW}$filename${NC}: $backup_result"

        if [ $backup_result -eq 0 ]; then
            processed_files+=("$filename")
            echo -e "${GREEN}Successfully backed up: ${YELLOW}$filename${NC}"
        else
            echo -e "${RED}Failed to backup: ${YELLOW}$filename${NC} (exit code: $backup_result)"
        fi

        echo
    done
else
    echo -e "${BLUE}=== PHASE 1: Skipping rsync backup ===${NC}"
    # If not doing rsync, assume all files should be processed for encryption
    for file in "${files_to_process[@]}"; do
        filename=$(basename "$file")
        processed_files+=("$filename")
    done
fi


if [ "$do_encryption" = "yes" ]; then
    echo -e "${BLUE}=== PHASE 2: Encrypt files on remote server ===${NC}"
    echo -e "${BLUE}Files to encrypt: ${#processed_files[@]}${NC}"
    echo

    if [ ${#processed_files[@]} -eq 0 ]; then
        echo -e "${BLUE}No files to encrypt${NC}"
    else
        # Create temporary password file on remote server
        REMOTE_PASSFILE="/tmp/.enc_pass_$$"

        echo -e "${BLUE}Enter encryption password once (input hidden):${NC}"
        read -rs encryption_password
        echo

        # Transfer password to remote server securely
        if ! ssh "$TRANSFER_NODE" "cat > '$REMOTE_PASSFILE' && chmod 600 '$REMOTE_PASSFILE'" <<< "$encryption_password"; then
            echo -e "${RED}Failed to setup password file on remote server${NC}"
            exit 1
        fi

        # Clear local password variable
        unset encryption_password

        echo -e "${GREEN}Password file created securely on remote server${NC}"
        echo

        # Encrypt all files using the password file
        successful_encryptions=0
        failed_encryptions=0

        for filename in "${processed_files[@]}"; do
            remote_file_path="${REMOTE_PHASE_1_DIR}/${filename}"
            echo -e "${BLUE}Encrypting on remote: ${YELLOW}$filename${NC}"

            # Use the encryption script with password file
            del_param=""
            if [ "$delete_originals" = "yes" ]; then
                del_param="del"
            fi

            if ssh -n "$TRANSFER_NODE" "cd '$REMOTE_SCRIPT_DIR' && ./check_i_enc.sh '$remote_file_path' $del_param --passfile '$REMOTE_PASSFILE'"; then
                if [ "$delete_originals" = "yes" ]; then
                    echo -e "${GREEN}✓ Successfully encrypted and deleted original: ${YELLOW}$filename${NC}"
                else
                    echo -e "${GREEN}✓ Successfully encrypted: ${YELLOW}$filename${NC}"
                fi
                ((successful_encryptions++))
            else
                echo -e "${RED}✗ Failed to encrypt: ${YELLOW}$filename${NC}"
                ((failed_encryptions++))
            fi
            echo
        done

        # Securely delete password file on remote server
        echo -e "${BLUE}Cleaning up password file on remote server...${NC}"
        if ssh "$TRANSFER_NODE" "shred -u '$REMOTE_PASSFILE' 2>/dev/null || rm -f '$REMOTE_PASSFILE'"; then
            echo -e "${GREEN}Password file securely deleted${NC}"
        else
            echo -e "${RED}Warning: Failed to delete password file on remote server${NC}"
        fi

        echo -e "${BLUE}Encryption summary:${NC}"
        echo -e "${GREEN}Successfully encrypted: $successful_encryptions${NC}"
        if [ $failed_encryptions -gt 0 ]; then
            echo -e "${RED}Failed encryptions: $failed_encryptions${NC}"
        fi
    fi
else
    echo -e "${BLUE}=== PHASE 2: Skipping encryption ===${NC}"
fi
#
echo -e "${GREEN}=== Batch processing completed ===${NC}"
echo -e "${BLUE}Total files processed: ${#processed_files[@]}${NC}"


