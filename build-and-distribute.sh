#!/bin/bash

# Check for pending changes before doing anything
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Error: You have uncommitted changes."
    echo "Please commit and push your changes before running a release."
    echo ""
    git status --short
    exit 1
fi

if [ -n "$(git log origin/main..HEAD 2>/dev/null)" ]; then
    echo "Error: You have unpushed commits."
    echo "Please push your changes before running a release."
    echo ""
    git log origin/main..HEAD --oneline
    exit 1
fi

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo "Error: GitHub CLI (gh) is not installed. Please install it first."
    echo "Visit: https://cli.github.com/"
    exit 1
fi

# Check if pip-compile is installed (from pip-tools)
if ! command -v pip-compile &> /dev/null; then
    echo "Error: pip-compile is not installed. Please install pip-tools first."
    echo "Run: pip install pip-tools"
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

# Switch to main and pull latest
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

# Generate requirements.txt with hashes from trusted private index
echo "Generating requirements.txt with hashes..."

# Extract dependencies from setup.py and write to requirements.in
python3 -c "
import ast
import sys

with open('setup.py', 'r') as f:
    content = f.read()

# Parse the setup.py file
tree = ast.parse(content)

# Find the setup() call and extract install_requires
for node in ast.walk(tree):
    if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'setup':
        for keyword in node.keywords:
            if keyword.arg == 'install_requires':
                # Extract the list of dependencies
                deps = ast.literal_eval(compile(ast.Expression(keyword.value), '<string>', 'eval'))
                for dep in deps:
                    print(dep)
                sys.exit(0)

print('Error: Could not extract install_requires from setup.py', file=sys.stderr)
sys.exit(1)
" > requirements.in

if [ $? -ne 0 ]; then
    echo "Error: Failed to extract dependencies from setup.py"
    exit 1
fi

echo "Extracted dependencies:"
cat requirements.in

# Run pip-compile with private index to get trusted hashes
PRIVATE_INDEX="https://pypi-proxy.dev.databricks.com/simple/"
pip-compile --generate-hashes --index-url="$PRIVATE_INDEX" --output-file=requirements.txt requirements.in

if [ $? -ne 0 ]; then
    echo "Error: pip-compile failed"
    exit 1
fi

# Remove the private index URL from requirements.txt (keep hashes, they're content-based)
sed -i.bak '/^--index-url/d' requirements.txt
# Also clean up the comment that references the private index
sed -i.bak "s|--index-url=$PRIVATE_INDEX ||g" requirements.txt
rm requirements.txt.bak

echo "requirements.txt generated with hashes (private index removed)"

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
git add setup.py dbdemos/__init__.py requirements.in requirements.txt
git commit -m "Bump version to $VERSION"
git push origin release/v$VERSION

# Create PR to main branch
echo "Creating pull request to main branch..."
if gh pr create --title "Release v$VERSION" --body "Automated release for version $VERSION" --base main --head release/v$VERSION; then
    echo "Pull request created successfully"
else
    echo "Warning: Failed to create pull request (may already exist)"
fi

# Also update main with the version bump so it doesn't get lost
echo "Syncing version bump to main..."
git checkout main
git add setup.py dbdemos/__init__.py requirements.in requirements.txt
git commit -m "Bump version to $VERSION"
git push origin main

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

echo "Release process completed for v$VERSION!"
