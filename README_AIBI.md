# Adding an AI-BI demo to dbdemos

*Note: Adding new content from external contributors required special terms approval. Please open an issue if you'd like to contribute and are not part of the Databricks team.*

*Note: if you're part of the Databricks team, please reach the demo team slack channel before starting the process for alignement and avoid duplicating work.*

## Fork dbdemos-notebooks
The actual AI-BI demo content is in the [dbdemos-notebooks repository](https://github.com/databricks-demos/dbdemos-notebooks).

Start by forking the repository and create a new branch there with your changes.

## Create the demo

Start by creating your dataset (must be all crafted/generated with DBRX to avoid any license issues), dashboard and genie space.

Once you're ready, add your dbdemos to the [aibi folder](https://github.com/databricks-demos/dbdemos-notebooks/tree/main/aibi).
For that, clone the the [aibi-marketing-campaign folder](https://github.com/databricks-demos/dbdemos-notebooks/tree/main/aibi/aibi-marketing-campaign) and replace the content with your own. 

Make sure the name of the folder has the similar pattern: `aibi-<use-case>`.

## Data Transformation and Table Structure

### Start with your story first
Think about what would be a good Dashboard+Genie. Ideally you want to show some business outcome in the dashboard, and you see a spike somewhere. Then you open genie to ask a followup question.

### Dataset
Once your story is ready, work backward to generate your dataset. Think about the gold table required, and then the raw dataset that you'll clean to create these tables.

**Your dataset must be entirely crafted with tools like faker / DBRX. Double check any dataset license. Add a NOTICE file in your dataset folder explaining where the data is coming from / how it was created.**

Datasets are stored in the [dbdemos-datasets repository](https://github.com/databricks-demos/dbdemos-datasets), and then mirrored in the dbdemos-dataset S3 bucket. Fork this repository and add your data in the `aibi` folder.

### Defining the dbdemos genie room setup

All the configuration should go in the bundle file. See this example: [https://github.com/databricks-demos/dbdemos-notebooks/blob/main/aibi/aibi-marketing-campaign/_resources/bundle_config.py](https://github.com/databricks-demos/dbdemos-notebooks/blob/main/aibi/aibi-marketing-campaign/_resources/bundle_config.py)

Here is what your bundle should look like:

```json
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
    {"source_folder":"aibi/dbdemos_aibi_cme_marketing_campaign/raw_campaigns",              "source_format": "parquet", "target_volume_folder":"raw_campaigns",              "target_format":"parquet"}],
  "sql_queries": [
    [
        "CREATE OR REPLACE TABLE `{{CATALOG}}`.`{{SCHEMA}}`.raw_campaigns TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE ) COMMENT 'This is the bronze table for campaigns created from parquet files' AS SELECT * FROM read_files('/Volumes/{{CATALOG}}/{{SCHEMA}}/dbdemos_raw_data/raw_campaigns', format => 'parquet', pathGlobFilter => '*.parquet')"
      ],
      [
        "... queries in here will be executed in parallel", " ... (don't forget to add comments) on the table, and PK/FK"
      ],
      ["CREATE OR REPLACE FUNCTION {{CATALOG}}.{{SCHEMA}}.my_ai_forecast(input_table STRING, target_column STRING, time_column STRING, periods INT) RETURN TABLE ..."]
    ],
  "genie_rooms":[
    {
     "id": "marketing-campaign",
     "display_name": "DBDemos - AI/BI - Marketing Campaign",     
     "description": "Analyze your Marketing Campaign effectiveness leveraging AI/BI Dashboard. Deep dive into your data and metrics.",
     "table_identifiers": ["{{CATALOG}}.{{SCHEMA}}.campaigns", "..."],
     "sql_instructions": [
        {
            "title": "Compute rolling metrics",
            "content": "select date, unique_clicks, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS clicks_t7d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS delivered_t7d, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 27 PRECEDING AND CURRENT ROW) AS clicks_t28d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 27 PRECEDING AND CURRENT ROW) AS delivered_t28d, sum(unique_clicks) OVER (ORDER BY date RANGE BETWEEN 90 PRECEDING AND CURRENT ROW) AS clicks_t91d, sum(total_delivered) OVER (ORDER BY date RANGE BETWEEN 90 PRECEDING AND CURRENT ROW) AS delivered_t91d, unique_clicks / total_delivered as ctr, total_delivered / total_sent AS delivery_rate, total_optouts / total_delivered AS optout_rate, total_spam / total_delivered AS spam_rate, clicks_t7d / delivered_t7d as ctr_t7d, clicks_t28d / delivered_t28d as ctr_t28d, clicks_t91d / delivered_t91d as ctr_t91d from {{CATALOG}}.{{SCHEMA}}.metrics_daily_rolling"
        }
    ],
    "instructions": "If a customer ask a forecast, leverage the sql fonction ai_forecast",
    "function_names": [
          "{{CATALOG}}.{{SCHEMA}}.my_ai_forecast"
        ],    
    "curated_questions": [
        "How has the total number of emails sent, delivered, and the unique clicks evolved over the last six months?", "..."
       ]
    }
  ]
}
```

### Data Loading and Transformation
AIBI demos should start with raw data files in a volume and implement a few transformation steps to showcase data lineage. This helps demonstrate the end-to-end data workflow and provides a more comprehensive view of Databricks' capabilities.

**Important:** Avoid using Materialized Views (MVs) for transformations as they can slow down the dbdemos installation process. Instead, use standard SQL transformations in your demo for now (we'll revisit soon).

Example transformation flow:
1. Start with raw data in volume, typically 3+ sources
2. Create bronze table(s) directly from the volume files (~3+ tables)
3. [optional] Create silver table(s) with basic transformations (cleaning, type conversion, etc.)
4. Create gold table(s) with business-specific transformations and potentially a few joins (we want to keep at least 2 or 3 tables in the genie room)

### Gold Table Requirements
- Gold tables (used in the Genie room) should have PK and FK defined
- Gold tables should include comprehensive comments on all fields. This improves the Genie experience by providing context for each column and helps users understand the data model.

Example gold table creation with comments directly in the CREATE statement:
```sql
CREATE OR REPLACE TABLE {{CATALOG}}.{{SCHEMA}}.customer_gold (
  id STRING COMMENT 'Unique customer identifier' PRIMARY KEY,
  first_name STRING COMMENT 'Customer first name',
  last_name STRING COMMENT 'Customer last name',
  email STRING COMMENT 'Customer email address',
  signup_date DATE COMMENT 'Date when customer created their account',
  last_activity_date DATE COMMENT 'Most recent date of customer activity',
  customer_segment STRING COMMENT 'Customer segmentation category (New, Loyal, At-Risk, Churned)',
  lifetime_value DOUBLE COMMENT 'Calculated total customer spend in USD'
)
AS
SELECT
  id, first_name, last_name, email, signup_date, last_activity_date, customer_segment, lifetime_value FROM {{CATALOG}}.{{SCHEMA}}.customer_silver;
```

This approach is more concise and ensures all column comments are created in a single SQL statement.

## SQL AI Functions

We need help implementing SQL AI Functions in the installer_genie.py file and the JSON configuration. These functions enhance the AI capabilities of the Genie room and enable more sophisticated queries.

The AI functions should be added as part of the SQL statement. Don't forget to add comments on them (at the function level and function param level).

Once created, you can add them to the genie room under `"function_names": ["{{CATALOG}}.{{SCHEMA}}.ai_forecast", "..."]`

**Note: this isn't yet implemented. If you're interested in contributing to DBDemos, reach out to the demo team. The implementation should go in the `InstallerGenie` class to create these functions during demo installation and make them available in the Genie room.**

## Update the Main notebook
Update the notebook cloned from the folder above, with your use-case.

### Present the use-case
Rename & Update the main notebook, detailing your use-case, what is the data and the insights you want to show.

### Update tracking
Update the demo name in the first cell in the tracker pixel, and the notebook name.

### Update the dashboard link

Put your dashboard in the dashboards folder. In the dashboard json, make sure you use the same catalog and schema as the one you have in the bundle configuration file, typically `main.dbdemos_aibi_xxxxxx`

You can then reference the dashboard like this:

```html
<a dbdemos-dashboard-id="web-marketing" href='/sql/dashboardsv3/02ef00cc36721f9e1f2028ee75723cc1' target="_blank">your dashboard</a>
```

the ID here `web-marketing` must match the ID in the bundle configuration (and the dashboard file name):

```
"dashboards": [{"name": "[dbdemos] AI/BI - Marketing Campaign",       "id": "web-marketing"}]
```

### Update the Genie Room link

```html
<a dbdemos-genie-id="marketing-campaign" href='/genie/rooms/01ef775474091f7ba11a8a9d2075eb58' target="_blank">your genie space</a>
```
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

## Add your dashboard under the dashboards folder

- create json file in the dashboards folder. Make sure you format it correctly as it's easy to read/diff
- your catalog.schema in the queries must match the `default_catalog` and `default_schema` in the bundle configuration.
- your dashboard name must match the id in the bundle configuration.
- don't forget to update the dashboard tracking (add the tracker in the MD at the end of the dashboard, match the demo name)


## Add your images

Images are stored in the [dbdemos-resources repository](https://github.com/databricks-demos/dbdemos-resources). 

To add an image, fork the repo and send a PR.

You need at least 2 images:
- the miniature for the demo list: `https://github.com/databricks-demos/dbdemos-resources/raw/main/icon/<demo_name>.jpg`
- the screenshot of the dashboard: `https://www.dbdemos.ai/assets/img/dbdemos/<demo_name>-dashboard-0.png` (1 per dashboard you add)

Reach out the demo team for a demo miniature


https://www.dbdemos.ai/assets/img/dbdemos/aibi-marketing-campaign-dashboard-0.png

https://github.com/databricks-demos/dbdemos-resources/raw/main/icon/aibi-marketing-campaign.jpg


# Packaging & testing your demo

Open the `test_demo.py` file. Update the conf to match your databricks-notebooks repo fork/branch in the config json file.

dbdemos needs a workspace and a repo to package the demo, make sure you configure it in the conf json file (use your fork).

Make sure you update the bundle folder to match your demo:

```
    bundle(conf, "aibi/aibi-marketing-campaign")
```

