#save current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

#increase the release (needs pip install bump)
git checkout main || exit 1
git pull || exit 1
bump

# Get the current version number
VERSION=$(python -c "from dbdemos import __version__; print(__version__)")
echo "New version: $VERSION"

#package
rm -rf ./dist/*
rm -rf ./dbdemos/bundles/.DS_Store
python3 setup.py clean --all bdist_wheel

echo "Package built under dist/ - updating pypi with new version..."
ls -alh ./dist
twine upload dist/*
echo "Upload ok - available as pip install dbdemos"

# Find the wheel file
WHL_FILE=$(find ./dist -name "*.whl" | head -n 1)
if [ -z "$WHL_FILE" ]; then
  echo "Error: No wheel file found in ./dist directory"
  exit 1
fi

echo "Found wheel file: $WHL_FILE"

# Extract version from wheel filename (format: dbdemos-0.6.12-py3-none-any.whl)
VERSION=$(basename "$WHL_FILE" | sed -E 's/dbdemos-([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
echo "Extracted version from wheel file: $VERSION"

TOOLS_FILE_NAME="./local_conf_tools.json"

# Extract GitHub token from local_conf_tools.json
GITHUB_TOKEN=$(grep -o '"github_token"[[:space:]]*:[[:space:]]*"[^"]*"' "$TOOLS_FILE_NAME" | sed 's/"github_token"[[:space:]]*:[[:space:]]*"\([^"]*\)"/\1/')

# Check for GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GitHub token not found in $TOOLS_FILE_NAME."
  echo "Please add a 'github_token' field to your $TOOLS_FILE_NAME file."
  exit 1
fi

# Function to create a release and upload asset
create_release_with_asset() {
  local repo=$1
  local tag_name="v$VERSION"
  local release_name="v$VERSION"
  
  echo "Creating release $release_name for $repo..."
  
  # Create the release
  release_response=$(curl -s -X POST \
    -H "Authorization: token $GITHUB_TOKEN" \
    -H "Accept: application/vnd.github.v3+json" \
    "https://api.github.com/repos/$repo/releases" \
    -d "{\"tag_name\":\"$tag_name\",\"name\":\"$release_name\",\"body\":\"Release version $VERSION\",\"draft\":false,\"prerelease\":false}")
  
  # Extract the upload URL from the response
  upload_url=$(echo "$release_response" | grep -o '"upload_url": "[^"]*' | cut -d'"' -f4 | sed 's/{?name,label}//')
  
  if [ -z "$upload_url" ]; then
    echo "Error creating release for $repo. Response:"
    echo "$release_response"
    return 1
  fi
  
  echo "Release created successfully. Uploading asset..."
  
  # Upload the asset
  asset_response=$(curl -s -X POST \
    -H "Authorization: token $GITHUB_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    -H "Accept: application/vnd.github.v3+json" \
    --data-binary @"$WHL_FILE" \
    "${upload_url}?name=$(basename $WHL_FILE)")
  
  # Check if asset was uploaded successfully
  asset_url=$(echo "$asset_response" | grep -o '"browser_download_url": "[^"]*' | cut -d'"' -f4)
  
  if [ -z "$asset_url" ]; then
    echo "Error uploading asset to $repo. Response:"
    echo "$asset_response"
    return 1
  fi
  
  echo "Asset uploaded successfully: $asset_url"
  return 0
}

# Create releases with assets on both repositories
echo "Creating releases for version v$VERSION..."
create_release_with_asset "databricks-demos/dbdemos"
create_release_with_asset "databricks-demos/dbdemos-notebooks"
create_release_with_asset "databricks-demos/dbdemos-dataset"
create_release_with_asset "databricks-demos/dbdemos-resources"

# Return to original branch
git checkout $CURRENT_BRANCH || exit 1

echo "Release process completed for v$VERSION!"
