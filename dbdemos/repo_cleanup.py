import requests
import json
import time
import base64
#cleanup jobs


def get_headers():
    with open("./resources/local_conf.json", "r") as r:
        c = json.loads(r.read())
    return {"Authorization": "Bearer " + c['pat_token'], 'Content-type': 'application/json'}


from collections import defaultdict
repo_counts = defaultdict(lambda: 0)
user_count = defaultdict(lambda: 0)
count = 0
def get_repos(path, to_move=[], token=None):
    r = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/repos", headers = get_headers(), params={"path_prefix": path, "next_page_token": token}).json()
    for repo in r["repos"]:
        global count
        count = count + 1
        print(repo)
        if 'url' in repo:
            c = repo_counts[repo['url']]
            repo_counts[repo['url']] = c+1
            user = repo['path'][7:]
            user = user[:user.find("/")]
            c = user_count[user]
            user_count[user] = c+1
        else:
            c = repo_counts["unknown"]
            repo_counts["unknown"] = c+1
            print(repo)
            print(requests.delete(f"https://e2-demo-field-eng.cloud.databricks.com/api/2.0/repos/{repo['id']}", headers = get_headers()))


        if "flightschool" in repo['path'].lower() or "bootcamp" in repo['path'].lower() or "capstone" in repo['path'].lower():
            to_move.append(repo)
        if "data-engineering-with-databricks" in repo['path']:
            to_move.append(repo)
        if "advanced-data-engineering-with-databricks" in repo['path']:
            to_move.append(repo)
    if "next_page_token" in r:
        return to_move + get_repos(path, to_move, r["next_page_token"])
    return to_move

import re

to_delete = get_repos("/Repos")
print(dict(sorted(repo_counts.items(), key=lambda item: -item[1])))
print(dict(sorted(user_count.items(), key=lambda item: -item[1])))
print(count)

email_to_delete = set()
for repo in to_delete:
    emails = re.findall(r'\/([0-9a-z\.A-Z+]*@databricks.com)\/', repo['path'])
    for e in emails:
        email_to_delete.add(e)
print(email_to_delete)


#r = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/preview/scim/v2/Users", headers = get_headers(),
#                 params={ "filter": 'userName eq "quentin.ambard@databricks.com"'}).json()
#print(r)

#r = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/preview/scim/v2/Users/7644138420879474", headers = get_headers()).json()
#print(r)
#r = requests.get("https://e2-demo-field-eng.cloud.databricks.com/api/2.0/workspace/export", headers = get_headers(),
#                 params={ "path": "/Repos/chiayui.lee@databricks.com/data-engineering-with-databricks", "format": "DBC", "direct_download": True }).json()
#content = base64.b64decode(file['content']).decode('ascii')
#print(r)
#with open("./test.dbc", "w") as f:
#    f.write(r['content'])
#print("ok")

workspace_url = "https://e2-demo-field-eng.cloud.databricks.com"
def job_cleanup(offset = 0, limit = 25):
    print(f"searching job, offset={offset}")
    now = int(time.time()*1000)
    r_jobs = requests.get(workspace_url+"/api/2.1/jobs/list", headers = get_headers(), params={"limit": limit, "offset": offset}).json()
    if "jobs" in r_jobs:
        for job in r_jobs["jobs"]:
            delete_job = False
            created_since_days = (now - job['created_time']) / 1000 / 3600 / 24
            if created_since_days > 150:
                if "megacorp" in job['settings']['name'].lower() or "flightscool" in job['settings']['name'].lower():
                    print(f'job_id={job["job_id"]} created since {created_since_days} days flagged as megacorp')
                    delete_job = True
                else:
                    r = requests.get(workspace_url+"/api/2.1/jobs/runs/list", headers = get_headers(), params={"job_id": job["job_id"]}).json()
                    if "runs" not in r:
                        print(f'job_id={job["job_id"]} created since {created_since_days} days and does not contains any run {r}')
                        delete_job = True
                    else:
                        last_run = r['runs'][0]['start_time']
                        last_run_days = (now - last_run) / 1000 / 3600 / 24
                        #Job hasn't run in 1 year, we can delete it
                        if last_run_days > 360:
                            print(f'job_id={job["job_id"]} created since {created_since_days} days and last run was {last_run_days} days ago.')
                            delete_job = True
                if delete_job:
                    d = requests.post(workspace_url+"/api/2.1/jobs/delete", headers = get_headers(), json={"job_id": job["job_id"]}).json()
                    print(f'DELETING JOB ID {job["job_id"]}. Delete response: {d}')
                    offset = offset-1

        if len(r_jobs['jobs']) == limit:
            job_cleanup(offset+limit, limit)
#job_cleanup()