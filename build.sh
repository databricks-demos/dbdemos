python3 setup.py clean --all bdist_wheel
#conda activate test_dbdemos
#pip3 install dist/dbdemos-0.1-py3-none-any.whl --force
#python3 test_package.py

cp dist/dbdemos-0.1-py3-none-any.whl release/dbdemos-0.1-py3-none-any.whl


#curl --netrc -X POST \
#https://adb-984752964297111.11.azuredatabricks.net/api/2.0/dbfs/put \
#--form contents="@./dist/dbdemos-0.1-py3-none-any.whl" \
#--form path="/FileStore/quentin/dbdemos-0.1-py3-none-any.whl" \
#--form overwrite=true


