#!/bin/bash
# File Decryption Script
#
# DESCRIPTION:
#   - Decrypts AES-256-CTR encrypted files with PBKDF2 (100,000 iterations)
#   - Compatible with files encrypted by check_i_enc.sh
#   - Stops if destination exists unless --overwrite is specified

source util.sh

# Show help message
show_help() {
    # Use echo -e instead of cat for color support
    echo -e "${BLUE}File Decryption Script${NC}"
    echo
    echo -e "${YELLOW}USAGE:${NC}"
    echo -e "    $0 <encrypted_file> [destination_file] [OPTIONS]"
    echo
    echo -e "${YELLOW}PARAMETERS:${NC}"
    echo -e "    encrypted_file      Path to encrypted file (.enc file)"
    echo -e "    destination_file    Optional: Path where decrypted file will be saved"
    echo -e "                        (auto-derived by removing .enc extension if not provided)"
    echo
    echo -e "${YELLOW}OPTIONS:${NC}"
    echo -e "    --overwrite         Overwrite destination file if it exists"
    echo -e "    --passfile PATH     Read password from file (for batch operations)"
    echo -e "    -h, --help          Show this help message"
    echo
    echo -e "${YELLOW}EXAMPLES:${NC}"
    echo -e "    ${BLUE}# Interactive decryption (prompts for password)${NC}"
    echo -e "    $0 backup.db.enc"
    echo
    echo -e "    ${BLUE}# Specify custom destination${NC}"
    echo -e "    $0 backup.db.enc /tmp/restored.db"
    echo
    echo -e "    ${BLUE}# Overwrite existing file${NC}"
    echo -e "    $0 backup.db.enc backup.db --overwrite"
    echo
    echo -e "    ${BLUE}# Use password file (batch mode)${NC}"
    echo -e "    $0 backup.db.enc --passfile /secure/passfile"
    echo
    echo -e "    ${BLUE}# Combine options${NC}"
    echo -e "    $0 backup.db.enc restored.db --overwrite --passfile /secure/passfile"
    echo
    echo -e "${YELLOW}SECURITY NOTES:${NC}"
    echo -e "    - Interactive mode reads password securely (hidden input)"
    echo -e "    - Password files should have 600 or 400 permissions"
    echo -e "    - Compatible with files encrypted by check_i_enc.sh using AES-256-CTR"
    echo
    echo -e "${YELLOW}EXIT CODES:${NC}"
    echo -e "    0    Successful decryption"
    echo -e "    1    Error (file not found, wrong password, etc.)"
    echo
}

# Check if help is requested or no parameters
if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

# Get required parameters
encrypted_file="$1"
destination_file=""

# Check if second parameter is a destination file (not an option)
if [ $# -gt 1 ] && [[ "$2" != --* ]]; then
    destination_file="$2"
    shift 2  # Move past first two arguments
else
    shift 1  # Move past first argument only
fi

# Parse optional arguments
overwrite="no"
passfile=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --overwrite)
            overwrite="yes"
            echo -e "${BLUE}Overwrite mode enabled${NC}"
            shift
            ;;
        --passfile)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo -e "${RED}Error: --passfile requires a file path${NC}"
                exit 1
            fi
            passfile="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option '$1'${NC}"
            echo -e "${YELLOW}Use --help for usage information${NC}"
            exit 1
            ;;
    esac
done

# Check if encrypted file exists
if [ ! -f "$encrypted_file" ]; then
    echo -e "${RED}Error: Encrypted file does not exist: ${YELLOW}$encrypted_file${NC}"
    exit 1
fi

# Auto-derive destination file if not provided
if [ -z "$destination_file" ]; then
    if [[ "$encrypted_file" == *.enc ]]; then
        # Remove .enc extension
        destination_file="${encrypted_file%.enc}"
        echo -e "${BLUE}Auto-derived destination: ${YELLOW}$destination_file${NC}"

        # Check if auto-derived destination already exists
        if [ -f "$destination_file" ] && [ "$overwrite" = "no" ]; then
            echo -e "${RED}Error: Destination file already exists: ${YELLOW}$destination_file${NC}"
            echo -e "${BLUE}Use --overwrite to replace existing file${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Error: Cannot auto-derive destination file${NC}"
        echo -e "${BLUE}File doesn't have .enc extension: ${YELLOW}$encrypted_file${NC}"
        echo -e "${BLUE}Please specify destination file explicitly${NC}"
        exit 1
    fi
else
    # Destination was explicitly provided - check if it exists
    if [ -f "$destination_file" ] && [ "$overwrite" = "no" ]; then
        echo -e "${RED}Error: Destination file already exists: ${YELLOW}$destination_file${NC}"
        echo -e "${BLUE}Use --overwrite to replace existing file${NC}"
        exit 1
    fi

    # Check if encrypted file has .enc extension (optional warning)
    if [[ "$encrypted_file" != *.enc ]]; then
        echo -e "${BLUE}Warning: File doesn't have .enc extension: ${YELLOW}$encrypted_file${NC}"
    fi
fi

# Validate passfile if provided
if [ -n "$passfile" ]; then
    if [ ! -f "$passfile" ]; then
        echo -e "${RED}Error: Password file does not exist: ${YELLOW}$passfile${NC}"
        exit 1
    fi
    if [ ! -r "$passfile" ]; then
        echo -e "${RED}Error: Cannot read password file: ${YELLOW}$passfile${NC}"
        exit 1
    fi
    # Check permissions - should be 600 or 400
    passfile_perms=$(stat -c %a "$passfile" 2>/dev/null || stat -f %A "$passfile" 2>/dev/null)
    if [[ ! "$passfile_perms" =~ ^[46]00$ ]]; then
        echo -e "${RED}Warning: Password file has insecure permissions: $passfile_perms${NC}"
        echo -e "${BLUE}Recommended: chmod 600 ${YELLOW}$passfile${NC}"
    fi
fi

echo -e "${BLUE}Decrypting: ${YELLOW}$encrypted_file${NC} -> ${YELLOW}$destination_file${NC}"

# Determine password source
if [ -n "$passfile" ]; then
    echo -e "${BLUE}Using password from file: ${YELLOW}$passfile${NC}"
    pass_option="-pass file:$passfile"
else
    echo -e "${BLUE}Interactive mode: Enter password when prompted${NC}"
    pass_option="-pass stdin"
fi

# Decrypt the file using AES-256-CTR
decryption_success=false

if [ -z "$passfile" ]; then
    # Interactive: read password securely (hidden input)
    echo -e "${BLUE}Enter decryption password (input will be hidden):${NC}"
    read -rs password
    echo
    if echo "$password" | openssl enc -aes-256-ctr -d -salt -pbkdf2 -iter 100000 \
        -in "$encrypted_file" -out "$destination_file" -pass stdin; then
        decryption_success=true
    fi
    unset password  # Clear password from memory
else
    # Batch: read password from file
    if openssl enc -aes-256-ctr -d -salt -pbkdf2 -iter 100000 \
        -in "$encrypted_file" -out "$destination_file" $pass_option; then
        decryption_success=true
    fi
fi

# Check if decryption was successful
if [ "$decryption_success" = true ] && [ -f "$destination_file" ]; then
    # Verify decrypted file is not empty
    if [ ! -s "$destination_file" ]; then
        echo -e "${RED}Error: Decrypted file is empty${NC}"
        rm -f "$destination_file"
        exit 1
    fi

    # Validate the decrypted file is actually decrypted (not garbage)
    # Try to detect if it's still encrypted data or binary garbage
    echo -e "${BLUE}Validating decryption...${NC}"

    # Method 1: Check if file contains mostly binary/encrypted data
    # Get first 1024 bytes and count printable characters
    if command -v file >/dev/null 2>&1; then
        file_type=$(file -b "$destination_file" 2>/dev/null || echo "unknown")

        # If file command detects it as "data" (binary), it's likely still encrypted
        if [[ "$file_type" == *"data"* ]] && [[ "$file_type" != *"database"* ]] && [[ "$file_type" != *"SQLite"* ]]; then
            echo -e "${RED}Error: Decryption appears to have failed - output looks like encrypted data${NC}"
            echo -e "${BLUE}File type detected: $file_type${NC}"
            echo -e "${BLUE}This usually means incorrect password was provided${NC}"
            rm -f "$destination_file"
            exit 1
        fi
    fi

    # Method 2: For known file types, do basic validation
    case "${destination_file,,}" in
        *.db|*.sqlite|*.sqlite3)
            # SQLite files should start with "SQLite format 3"
            if ! head -c 16 "$destination_file" 2>/dev/null | grep -q "SQLite format 3"; then
                echo -e "${RED}Error: Decrypted file is not a valid SQLite database${NC}"
                echo -e "${BLUE}This usually means incorrect password was provided${NC}"
                rm -f "$destination_file"
                exit 1
            fi
            ;;
        *.txt|*.log|*.json|*.xml|*.csv)
            # Text files should have reasonable printable character ratio
            printable_chars=$(head -c 1024 "$destination_file" 2>/dev/null | tr -cd '[:print:][:space:]' | wc -c)
            total_chars=$(head -c 1024 "$destination_file" 2>/dev/null | wc -c)
            if [ "$total_chars" -gt 0 ]; then
                printable_ratio=$((printable_chars * 100 / total_chars))
                if [ "$printable_ratio" -lt 70 ]; then
                    echo -e "${RED}Error: Decrypted file has too much binary data (${printable_ratio}% printable)${NC}"
                    echo -e "${BLUE}This usually means incorrect password was provided${NC}"
                    rm -f "$destination_file"
                    exit 1
                fi
            fi
            ;;
    esac

    echo -e "${GREEN}Successfully decrypted: ${YELLOW}$encrypted_file${NC} -> ${YELLOW}$destination_file${NC}"

    # Show file sizes for verification
    encrypted_size=$(stat -c%s "$encrypted_file" 2>/dev/null || stat -f%z "$encrypted_file" 2>/dev/null)
    decrypted_size=$(stat -c%s "$destination_file" 2>/dev/null || stat -f%z "$destination_file" 2>/dev/null)

    if command -v numfmt >/dev/null 2>&1; then
        echo -e "${BLUE}Encrypted size: $(numfmt --to=iec-i --suffix=B $encrypted_size)${NC}"
        echo -e "${BLUE}Decrypted size: $(numfmt --to=iec-i --suffix=B $decrypted_size)${NC}"
    else
        echo -e "${BLUE}Encrypted size: $encrypted_size bytes${NC}"
        echo -e "${BLUE}Decrypted size: $decrypted_size bytes${NC}"
    fi

    echo -e "${GREEN}Decryption completed successfully!${NC}"
    exit 0
else
    echo -e "${RED}Error: Failed to decrypt ${YELLOW}$encrypted_file${NC}"
    # Clean up failed decryption file
    [ -f "$destination_file" ] && rm -f "$destination_file"
    exit 1
fi