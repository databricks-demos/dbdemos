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
if ! twine upload dist/*; then
    echo "Error: Failed to upload package to PyPI"
    exit 1
fi
echo "Upload ok - available as pip install dbdemos"

# Commit and push the bumped version to main branch
echo "Committing and pushing bumped version to main branch..."
git add setup.py
git commit -m "Bump version to $VERSION"
git push origin main
echo "Version bump committed and pushed to main branch"

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
