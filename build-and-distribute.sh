# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed. Please install it first."
    echo "Visit: https://cli.github.com/"
    exit 1
fi

# Check authentication status
if ! gh auth status &> /dev/null; then
    echo "GitHub CLI not authenticated. Please login..."
    gh auth login
fi

# Check if active account is Enterprise Managed User (ends with _data)
ACTIVE_ACCOUNT=$(gh auth status | grep "Logged in to" | head -1 | sed 's/.*Logged in to github.com account \([^ ]*\).*/\1/')
if [[ "$ACTIVE_ACCOUNT" == *"_data" ]]; then
    echo "Warning: Active account '$ACTIVE_ACCOUNT' appears to be an Enterprise Managed User"
    echo "Switching to regular account..."
    
    # Get list of available accounts by parsing auth status output
    AVAILABLE_ACCOUNTS=$(gh auth status | grep "Logged in to" | sed 's/.*Logged in to github.com account \([^ ]*\).*/\1/')
    
    # Find first account that doesn't end with _data
    REGULAR_ACCOUNT=""
    while IFS= read -r account; do
        if [[ "$account" != *"_data" ]]; then
            REGULAR_ACCOUNT="$account"
            break
        fi
    done <<< "$AVAILABLE_ACCOUNTS"
    
    if [[ -n "$REGULAR_ACCOUNT" ]]; then
        echo "Switching to regular account: $REGULAR_ACCOUNT"
        gh auth switch --user "$REGULAR_ACCOUNT" || {
            echo "Error: Failed to switch to regular account"
            exit 1
        }
    else
        echo "Error: No regular account found. Please add a regular GitHub account:"
        echo "gh auth login"
        exit 1
    fi
fi

# Check access to required repositories
echo "Checking access to required repositories..."
REPOS=("databricks-demos/dbdemos" "databricks-demos/dbdemos-notebooks" "databricks-demos/dbdemos-dataset" "databricks-demos/dbdemos-resources")

for repo in "${REPOS[@]}"; do
    if ! gh api "repos/$repo" &> /dev/null; then
        echo "Error: No access to repository $repo"
        echo "Please ensure you have the necessary permissions or try logging in again:"
        echo "gh auth login"
        exit 1
    fi
    echo "✓ Access confirmed for $repo"
done

#save current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

#increase the release
git checkout main || exit 1
git pull || exit 1

# Get current version from setup.py
CURRENT_VERSION=$(grep "version=" setup.py | sed "s/.*version='\([^']*\)'.*/\1/")
echo "Current version: $CURRENT_VERSION"

# Bump version (patch increment)
IFS='.' read -ra VERSION_PARTS <<< "$CURRENT_VERSION"
NEW_PATCH=$((VERSION_PARTS[2] + 1))
NEW_VERSION="${VERSION_PARTS[0]}.${VERSION_PARTS[1]}.$NEW_PATCH"
echo "New version: $NEW_VERSION"

# Update version in setup.py
sed -i.bak "s/version='[^']*'/version='$NEW_VERSION'/" setup.py
rm setup.py.bak

# Update version in __init__.py
sed -i.bak "s/__version__ = \"[^\"]*\"/__version__ = \"$NEW_VERSION\"/" dbdemos/__init__.py
rm dbdemos/__init__.py.bak

# Use the version we just bumped
VERSION=$NEW_VERSION
echo "Using bumped version: $VERSION"

#package
rm -rf ./dist/*
rm -rf ./dbdemos/bundles/.DS_Store
python3 setup.py clean --all bdist_wheel

echo "Package built under dist/ - updating pypi with new version..."
ls -alh ./dist
if ! twine upload dist/*; then
    echo "Error: Failed to upload package to PyPI"
    exit 1
fi
echo "Upload ok - available as pip install dbdemos"

# Create or switch to release branch and commit the bumped version
echo "Creating/updating release branch with bumped version..."
git checkout -b release/v$VERSION 2>/dev/null || git checkout release/v$VERSION
git add setup.py dbdemos/__init__.py
git commit -m "Bump version to $VERSION"
git push origin release/v$VERSION

# Create PR to main branch
echo "Creating pull request to main branch..."
if gh pr create --title "Release v$VERSION" --body "Automated release for version $VERSION" --base main --head release/v$VERSION; then
    echo "Pull request created successfully"
else
    echo "Warning: Failed to create pull request (may already exist)"
fi
echo "Version bump committed to release branch and PR created"

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

# Function to create a release and upload asset using gh CLI
create_release_with_asset() {
  local repo=$1
  local tag_name="v$VERSION"
  local release_name="v$VERSION"
  
  echo "Creating release $release_name for $repo..."
  
  # Create the release using gh CLI
  if gh release create "$tag_name" "$WHL_FILE" --repo "$repo" --title "$release_name" --notes "Release version $VERSION"; then
    echo "Release created and asset uploaded successfully for $repo"
    return 0
  else
    echo "Error creating release for $repo"
    return 1
  fi
}

# Create releases with assets on all repositories
echo "Creating releases for version v$VERSION..."
create_release_with_asset "databricks-demos/dbdemos"
create_release_with_asset "databricks-demos/dbdemos-notebooks"
create_release_with_asset "databricks-demos/dbdemos-dataset"
create_release_with_asset "databricks-demos/dbdemos-resources"

# Return to original branch
git checkout $CURRENT_BRANCH || exit 1

echo "Release process completed for v$VERSION!"
