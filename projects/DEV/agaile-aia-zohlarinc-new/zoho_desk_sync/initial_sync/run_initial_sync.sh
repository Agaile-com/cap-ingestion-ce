
#!/bin/bash
set -e
# # Ensure the AWS CLI is available and configured
# if ! aws sts get-caller-identity > /dev/null 2>&1; then
#     echo "AWS CLI not configured properly or not logged in."
#     exit 1
# else
#     echo "AWS CLI is configured."
# fi

# Define the path to your Python interpreter if not using the default
#PYTHON_PATH="python3"

# List of scripts in order to be executed
scripts=(
    "01_convert_vectordata_to_zohodata_format_v2.0.py"
    "02_normalize_formated_vectordata_v2.0.py"
    "03_create_zohodata_from_normalized_vectordata_v2.0.py"
    "04_get_zohodata_metadata_v2.0.py"
    "05_match_zohodata_metadata_with_vectordata_v2.0.py"
    "06_get_zohodata_v2.0.py"
    "07_convert_zohodata_to_vectordata_format_v2.0.py"
    "08_sync_zohodata_with_vectordata_v2.0.py"
    "09_upload_synced_vectordata_2_s3_v2.0.py"
)

# Execute each script sequentially
for script in "${scripts[@]}"
do
    echo "Running $script..."
    $PYTHON_PATH $script
    if [ $? -ne 0 ]; then
        echo "$script failed to execute successfully."
        exit 1
    fi
    echo "$script completed successfully."
done

echo "All scripts executed successfully."