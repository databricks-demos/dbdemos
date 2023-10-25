#increase the release (needs pip install bump)
bump
#package
rm -rf ./dist/*
rm -rf ./dbdemos/bundles/.DS_Store
python3 setup.py clean --all bdist_wheel
echo "Package built under dist/ - updating pypi with new version..."
ls -alh ./dist
twine upload dist/*
echo "Upload ok - available as pip install dbdemos"
