# Adding an AI-BI demo to dbdemos

*Note: Adding new content from external contributors required special terms approval. Please open an issue if you'd like to contribute and are not part of the Databricks team.*

*Note: if you're part of the Databricks team, please reach the demo team slack channel before starting the process for alignement and avoid duplicating work.*

## Fork dbdemos-notebooks
The actual AI-BI demo content is in the [dbdemos-notebooks repository](https://github.com/databricks-demos/dbdemos-notebooks.

Start by forking the repository and create a new branch there with your changes.

## Create the demo

Start by creating your dataset (must be all crafted/generated with DBRX to avoid any license issues), dashboard and genie space.

Once you're ready, add your dbdemos to the [aibi folder](https://github.com/databricks-demos/dbdemos-notebooks/tree/main/aibi).
For that, clone the the [aibi-marketing-campaign folder](https://github.com/databricks-demos/dbdemos-notebooks/tree/main/aibi/aibi-marketing-campaign) and replace the content with your own. 

Make sure the name of the folder has the similar pattern: `aibi-<use-case>`.

## Update the Main notebook
Update the notebook cloned from the folder above, with your use-case.
### Present the use-case
Rename & Update the main notebook, detailing your use-case, what is the data and the insights you want to show.

### Update tracking
Update the demo name in the first cell in the tracker pixel, and the notebook name.

### Update the dashboard link

<a dbdemos-dashboard-id="web-marketing" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">your dashboard</a>

the ID here `web-marketing` must match the ID in the bundle configuration:

```
"dashboards": [{"name": "[dbdemos] AI/BI - Marketing Campaign",       "id": "web-marketing"}]
```


### Update the dashboard link

<a dbdemos-genie-id="marketing-campaign" href='/genie/rooms/01ef775474091f7ba11a8a9d2075eb58' target="_blank">your genie space</a>

the ID here `marketing-campaign` must match the ID in the bundle configuration:

```
  "genie_rooms":[
    {
     "id": "marketing-campaign",
     "display_name": "DBDemos - AI/BI - Marketing Campaign",   
     ...
    }
  ]
```


## Update the bundle configuration

- Make sure the dashboard ID and genie room ID match your links as above. The dashboard ID must match the dashboard file name in the dashboards folder (see below).
- Keep the `default_catalog` to main, `default_schema` should follow the naming convention `dbdemos_aibi_<industry>_<use-case>`.
- Make sure you add a sql instruction in the genie room, with curated questions, descriptions and an instruction.
- Dataset folders must be the path in the databricks-datasets repository (see below).

```
{
  "name": "aibi-marketing-campaign",
  "category": "AI-BI",
  "title": "AI/BI: Marketing Campaign effectiveness",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_aibi_cme_marketing_campaign",
  "description": "Analyze your Marketing Campaign effectiveness leveraging AI/BI Dashboard. Deep dive into your data and metrics, asking plain question through Genie Room.",
  "bundle": True,
  "notebooks": [
    {
      "path": "AI-BI-Marketing-campaign", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "AI BI: Campaign effectiveness", 
      "description": "Discover Databricks Intelligence Data Platform capabilities."
    }
  ],
  "init_job": {},
  "cluster": {}, 
  "pipelines": [],
  "dashboards": [{"name": "[dbdemos] AI/BI - Marketing Campaign",       "id": "web-marketing"}
                ],
  "data_folders":[
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/prospects",              "source_format": "parquet", "target_table_name":"prospects",              "target_format":"delta"}  
    ],
  "genie_rooms":[
    {
     "id": "marketing-campaign",
     "display_name": "DBDemos - AI/BI - Marketing Campaign",     
     "description": "Analyze your Marketing Campaign effectiveness leveraging AI/BI Dashboard. Deep dive into your data and metrics.",
     "table_identifiers": ["{{CATALOG}}.{{SCHEMA}}.prospects"],
     "sql_instructions": [{"title": "Compute rolling metrics", "content": "SELECT <YOUR_LOGIC> from {{CATALOG}}.{{SCHEMA}}.prospects"}],
     "instructions": "If a customer ask a forecast, leverage the sql fonction ai_forecast",
     "curated_questions": [
       "What is the open rate?", 
       "What is the click-through rate (CTR)?"
       ]
    }
  ]
}
```

## Add your dashboard under the dashboards folder

- create json file in the dashboards folder. Make sure you format it correctly as it's easy to read/diff
- your catalog.schema in the queries must match the `default_catalog` and `default_schema` in the bundle configuration.
- your dashboard name must match the id in the bundle configuration.
- don't forget to update the dashboard tracking (add the tracker in the MD at the end of the dashboard, match the demo name)


## Add your dataset

**Your dataset must be entirely crafted with tools like faker / DBRX. Double check any dataset license. Add a NOTICE file in your dataset folder explaining where the data is coming from / how it was created.**

Datasets are stored in the [dbdemos-datasets repository](https://github.com/databricks-demos/dbdemos-datasets), and then mirrored in the dbdemos-dataset S3 bucket. Fork this repository and add your data in the `aibi` folder.


## Add your images

Images are stored in the [dbdemos-resources repository](https://github.com/databricks-demos/dbdemos-resources). 

To add an image, fork the repo and send a PR.

You need at least 2 images:
- the miniature for the demo list: `https://github.com/databricks-demos/dbdemos-resources/raw/main/icon/<demo_name>.jpg`
- the screenshot of the dashboard: `https://www.dbdemos.ai/assets/img/dbdemos/<demo_name>-dashboard-0.png` (1 per dashboard you add)

Reach out the demo team for a demo miniature


https://www.dbdemos.ai/assets/img/dbdemos/aibi-marketing-campaign-dashboard-0.png

https://github.com/databricks-demos/dbdemos-resources/raw/main/icon/aibi-marketing-campaign.jpg