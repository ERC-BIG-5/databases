#!/bin/bash
# File Encryption Script
#
# USAGE:
#   Method 1 (interactive): ./check_i_enc.sh <file_path> [del]
#   Method 2 (password file): ./check_i_enc.sh <file_path> [del] --passfile /path/to/passfile
#
# PARAMETERS:
#   file_path    - Path to file to encrypt
#   del          - Optional: Delete original file after successful encryption
#   --passfile   - Optional: Read password from file (for batch operations)
#
# DESCRIPTION:
#   - Encrypts single file using AES-256-CTR with PBKDF2 (100,000 iterations)
#   - Encrypted file is saved with .enc extension next to original
#   - If "del" parameter is provided, original file is securely deleted after encryption

source util.sh

# Check if parameter is provided
if [ $# -eq 0 ]; then
    echo -e "${RED}Error: No file path provided${NC}"
    echo "Usage: $0 <file_path> [del] [--passfile /path/to/passfile]"
    exit 1
fi

# Get the file path from the first parameter
file_path="$1"

# Parse arguments
delete_original=""
passfile=""

shift  # Move past first argument
while [[ $# -gt 0 ]]; do
    case $1 in
        del)
            delete_original="true"
            echo -e "${BLUE}Delete mode enabled: Original file will be removed after encryption${NC}"
            shift
            ;;
        --passfile)
            passfile="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Check if file exists
if [ ! -f "$file_path" ]; then
    echo -e "${RED}Error: File does not exist: $file_path${NC}"
    exit 1
fi

# Validate passfile if provided
if [ -n "$passfile" ]; then
    if [ ! -f "$passfile" ]; then
        echo -e "${RED}Error: Password file does not exist: $passfile${NC}"
        exit 1
    fi
    if [ ! -r "$passfile" ]; then
        echo -e "${RED}Error: Cannot read password file: $passfile${NC}"
        exit 1
    fi
    # Check permissions - should be 600 or 400
    passfile_perms=$(stat -c %a "$passfile" 2>/dev/null || stat -f %A "$passfile" 2>/dev/null)
    if [[ ! "$passfile_perms" =~ ^[46]00$ ]]; then
        echo -e "${RED}Warning: Password file has insecure permissions: $passfile_perms${NC}"
        echo -e "${BLUE}Recommended: chmod 600 $passfile${NC}"
    fi
fi

enc_file="${file_path}.enc"

echo -e "${BLUE}Encrypting: ${YELLOW}$file_path${NC}"

# Determine password source
if [ -n "$passfile" ]; then
    echo -e "${BLUE}Using password from file: ${YELLOW}$passfile${NC}"
    pass_option="-pass file:$passfile"
else
    echo -e "${BLUE}Interactive mode: Enter password when prompted${NC}"
    pass_option="-pass stdin"
fi

# Encrypt the file using AES-256-CTR (faster for large files)
encryption_success=false

if [ -z "$passfile" ]; then
    # Interactive: read password securely (hidden input)
    echo -e "${BLUE}Enter encryption password (input will be hidden):${NC}"
    read -rs password
    echo
    if echo "$password" | openssl enc -aes-256-ctr -salt -pbkdf2 -iter 100000 \
        -in "$file_path" -out "$enc_file" -pass stdin; then
        encryption_success=true
    fi
    unset password  # Clear password from memory
else
    # Batch: read password from file
    if openssl enc -aes-256-ctr -salt -pbkdf2 -iter 100000 \
        -in "$file_path" -out "$enc_file" $pass_option; then
        encryption_success=true
    fi
fi

# Check if encryption was successful
if [ "$encryption_success" = true ] && [ -f "$enc_file" ]; then
    # Verify encrypted file is not empty
    if [ ! -s "$enc_file" ]; then
        echo -e "${RED}Error: Encrypted file is empty${NC}"
        rm -f "$enc_file"
        exit 1
    fi

    echo -e "${GREEN}Successfully encrypted: ${YELLOW}$file_path${NC} -> ${YELLOW}$enc_file${NC}"

    # Show file sizes for verification
    original_size=$(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null)
    encrypted_size=$(stat -c%s "$enc_file" 2>/dev/null || stat -f%z "$enc_file" 2>/dev/null)

    if command -v numfmt >/dev/null 2>&1; then
        echo -e "${BLUE}Original size: $(numfmt --to=iec-i --suffix=B $original_size)${NC}"
        echo -e "${BLUE}Encrypted size: $(numfmt --to=iec-i --suffix=B $encrypted_size)${NC}"
    else
        echo -e "${BLUE}Original size: $original_size bytes${NC}"
        echo -e "${BLUE}Encrypted size: $encrypted_size bytes${NC}"
    fi

    # Delete original file if delete mode is enabled
    if [ "$delete_original" = "true" ]; then
        # Use shred if available for secure deletion
        if command -v shred >/dev/null 2>&1; then
            if shred -u "$file_path" 2>/dev/null; then
                echo -e "${GREEN}Securely deleted original file: ${YELLOW}$file_path${NC}"
            else
                echo -e "${RED}Warning: shred failed, using rm instead${NC}"
                if rm "$file_path"; then
                    echo -e "${GREEN}Deleted original file: ${YELLOW}$file_path${NC} (not securely wiped)"
                else
                    echo -e "${RED}Warning: Failed to delete original file: ${YELLOW}$file_path${NC}"
                    exit 1
                fi
            fi
        else
            if rm "$file_path"; then
                echo -e "${GREEN}Deleted original file: ${YELLOW}$file_path${NC}"
                echo -e "${BLUE}Note: Install 'shred' for secure file deletion${NC}"
            else
                echo -e "${RED}Warning: Failed to delete original file: ${YELLOW}$file_path${NC}"
                exit 1
            fi
        fi
    fi

    echo -e "${GREEN}Encryption completed successfully!${NC}"
    exit 0
else
    echo -e "${RED}Error: Failed to encrypt ${YELLOW}$file_path${NC}"
    # Clean up failed encryption file
    [ -f "$enc_file" ] && rm -f "$enc_file"
    exit 1
fi