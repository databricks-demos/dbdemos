#increase the release (needs pip install bump)
git checkout main || exit 1
git pull || exit 1
bump
#package
rm -rf ./dist/*
rm -rf ./dbdemos/bundles/.DS_Store
python3 setup.py clean --all bdist_wheel
echo "Package built under dist/ - updating pypi with new version..."
ls -alh ./dist
twine upload dist/*
echo "Upload ok - available as pip install dbdemos"
#TODO: should enforce commit before packaging and add a release tag in both dbdemos and dbdemos-notebooks
