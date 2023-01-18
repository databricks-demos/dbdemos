# dbdemos

DBDemos is a toolkit to easily install Lakehouse demos for Databricks.

Simply deploy & share demos on any workspace. dbdemos is packaged with a list of demos:

- Lakehouse, end-to-end demos (ex: Lakehouse Retail Churn)
- Product demos (ex: Delta Live Table, CDC, ML, DBSQL Dashboard, MLOps...)

## Installation 
**Do not clone the repo, just pip install dbdemos wheel:**

```
%pip install dbdemos
```

## Usage within Databricks

See [demo video](https://drive.google.com/file/d/12Iu50r7hlawVN01eE_GoUKBQ4kvUrR56/view?usp=sharing) 
```
import dbdemos
dbdemos.help()
dbdemos.list_demos()

dbdemos.install('lakehouse-retail-c360', path='./', overwrite = True)
```

![Dbdemos install](https://github.com/databricks-demos/dbdemos/raw/main/resources/dbdemos-screenshot.png)

## Requirements

`dbdemos` requires the current user to have:
* Cluster creation permission
* DLT Pipeline creation permission 
* DBSQL dashboard & query creation permission
* For UC demos: Unity Catalog metastore must be available (demo will be installed but won't work) 

New with 0.2: dbdemos can import/export dahsboard without the import/export preview (using the dbsqlclone toolkit)

## Features

* Load demo notebooks (pre-run) to the given path
* Start job to load dataset based on demo requirement
* Start demo cluster customized for the demo & the current user
* Setup DLT pipelines
* Setup DBSQL dashboard
* Create ML Model
* Demo links are updated with resources created for an easy navigation

## Feedback

Demo not working? Can't use dbdemos? Please open a github issue. <br/>
Make sure you mention the name of the demo.

# DBDemos Developer options

Read the following if you want to add a new demo bundle.

## Packaging a demo with dbdemos

Your demo must contain a `_resources` folder where you include all initialization scripts and your bundle configuration file.

### Links & tags
DBdemos will dynamically override the link to point to the resources created.

**Always use links relative to the local path to support multi workspaces. Do not add the workspace id.**

#### DLT pipelines:
Your DLT pipeline must be added in the bundle file.
Within your notebook, to identify your pipeline with the une in the bundle file, specify the id `dbdemos-pipeline-id="<id>"`as following:

`<a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Delta Live Table pipeline</a>`

#### DBSQL dashboards:
DBSQL dashboard are automatically downloaded during the process. No need to add them in the bundle file. Simply use links as following:

` <a href="/sql/dashboards/19394330-2274-4b4b-90ce-d415a7ff2130" target="_blank">Churn Analysis Dashboard</a>`


### bundle_config
The demo must contain the a `./_resources/bundle_config` file containing your bundle definition.
This need to be a notebook & not a .json file (due to current api limitation).

```json
{
  "name": "<Demo name, used in dbdemos.install('xxx')>",
  "category": "<Category, like data-engineering>",
  "title": "<Title>.",
  "description": "<Description>",
  "bundle": <Will bundle when True, skip when False>,
  "tags": [{"dlt": "Delta Live Table"}],
  "notebooks": [
    {
      "path": "<notebbok path from the demo folder (ex: resources/00-load-data)>", 
      "pre_run": <Will start a job to run it before packaging to get the cells results>, 
      "publish_on_website": <Will add the notebook in the public website (with the results if it's pre_run=True)>, 
      "add_cluster_setup_cell": <if True, add a cell with the name of the demo cluster>,
      "title":  "<Title>", 
      "description": "<Description (will be in minisite also)>",
      "parameters": {"<key>": "<value. Will be sent to the pre_run job>"}
    }
  ],
  "init_job": {
    "settings": {
        "name": "demos_dlt_cdc_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/01-load-data-quality-dashboard",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ]
        .... Full standard job definition
    }
  },
  "pipelines": <list of DLT pipelines if any>
  [
    {
      "id": "dlt-cdc", <id, used in the notebook links to go to the generated notebook: <a dbdemos-pipeline-id="dlt-cdc" href="#joblist/pipelines/xxxx">installed DLT pipeline</a> >
      "run_after_creation": True,
      "definition": {
        ... Any DLT pipelineconfiguration...
        "libraries": [
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/_resources/00-Data_CDC_Generator"
                }
            }
        ],
        "name": "demos_dlt_cdc_{{CURRENT_USER_NAME}}",
        "storage": "/demos/dlt/cdc/{{CURRENT_USER_NAME}}",
        "target": "demos_dlt_cdc_{{CURRENT_USER_NAME}}"
      }
    }
  ]
}
```

dbdemos will replace the values defined as {{<KEY>}} based on who install the demo. Supported keys:
* TODAY
* CURRENT_USER (email)
* CURRENT_USER_NAME (derivated from email)
* DEMO_NAME
* DEMO_FOLDER


# DBDemo Installer configuration

The following describe how to package the demos created.

The installer needs to fetch data from a workspace & start jobs. To do so, it requires informations `local_conf.json`
```json
{
  "pat_token": "xxx",
  "username": "xx.xx@databricks.com",
  "url": "https://xxx.databricks.com",
  "repo_staging_path": "/Repos/xx.xx@databricks.com",
  "repo_name": "<field-demos_build>",
  "repo_url": "https://github.com/<repo containing demos to package>",
  "branch": "master",
  "current_folder": "<Used to mock the current folder outside of a notebook, ex: /Users/quentin.ambard@databricks.com/test_install_demo>"
}
```

### Creating the bundles:
```python
bundler = JobBundler(conf)
# the bundler will use a stating repo dir in the workspace to analyze & run content.
bundler.reset_staging_repo(skip_pull=False)
# Discover bundles from repo:
bundler.load_bundles_conf()
# Or manually add bundle to run faster:
#bundler.add_bundle("product_demos/Auto-Loader (cloudFiles)")

# Run the jobs (only if there is a new commit since the last time, or failure, or force execution)
bundler.start_and_wait_bundle_jobs(force_execution = False)

packager = Packager(conf, bundler)
packager.package_all()
```


## Licence
See LICENSE file.

## Data collection
To improve users experience and dbdemos asset quality, dbdemos sends report usage and capture views in the installed notebook (usually in the first cell) and dashboards. This information is captured for product improvement only and not for marketing purpose, and doesn't contain PII information. By using `dbdemos` and the assets it provides, you consent to this data collection. If you wish to disable it, you can set `Tracker.enable_tracker` to False in the `tracker.py` file.

## Resource creation
To simplify your experience, `dbdemos` will create and start for you resources. As example, a demo could start (not exhaustive):
- A cluster to run your demo
- A Delta Live Table Pipeline to ingest data
- A DBSQL endpoint to run DBSQL dashboard
- An ML model

While `dbdemos` does its best to limit the consumption and enforce resource auto-termination, you remain responsible for the resources created and the potential consumption associated.

## Support
Databricks does not offer official support for `dbdemos` and the associated assets.
For any issue with `dbdemos` or the demos installed, please open an issue and the demo team will have a look on a best effort basis.

