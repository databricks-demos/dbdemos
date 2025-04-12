import requests
import urllib.parse
import hashlib

class Tracker:
    #Set this value to false to disable dbdemo toolkit tracker.
    enable_tracker = True
    URL = "https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics"

    def __init__(self, org_id, uid, email = None):
        self.org_id = org_id
        self.uid = uid
        # This is aggregating user behavior within Databricks at the org level to better understand dbdemos usage and improve the product.
        # We are not collecting any email/PII data. Please reach out to the demo team if you have any questions.
        if email is not None and email.endswith("@databricks.com"):
            self.email = email
        else:
            self.email = None

    def track_install(self, category, demo_name):
        self.track(category, demo_name, "INSTALL")
    
    def track_create_cluster(self, category, demo_name):
        self.track(category, demo_name, "CREATE_CLUSTER")
    
    def track_list(self):
        self.track("list_demos", "list_demos", "LIST")

    def get_user_hash(self):
        if self.email is None or not self.email.endswith("@databricks.com"):
            return None
        return hashlib.sha256(self.email.encode()).hexdigest()

    def get_track_url(self, category, demo_name, event, notebook = ""):
        params = self.get_track_params(category, demo_name, event, notebook)
        return Tracker.URL+"?"+urllib.parse.urlencode(params)

    def get_track_params(self, category, demo_name, event, notebook =""):
        if not Tracker.enable_tracker:
            return {}
        if len(notebook) > 0:
            notebook = '/'+notebook
        params = {"category": category,
                  "org_id": self.org_id, #legacy "cid" -- ignore "uid": self.uid
                  "notebook": notebook,
                  "demo_name": demo_name,
                  "event": event,
                  "path": f"/_dbdemos/{category}/{demo_name}{notebook}", #legacy tracking "dp"
                  "version": 1}
        user_hash = self.get_user_hash()
        if user_hash is not None:
            params["user_hash"] = user_hash
        return params


    def track(self, category,  demo_name, event):
        if self.org_id == "1660015457675682":
            print("skipping tracker for test / dev")
        elif Tracker.enable_tracker:
            headers = {"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                        "accept-encoding": "gzip, deflate, br",
                        "accept-language": "en-US,en;q=0.9",
                        "cache-control": "max-age=0",
                        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
            try:
                with requests.post(Tracker.URL, json = self.get_track_params(category, demo_name, event), headers=headers, timeout=5) as t:
                    if t.status_code != 200:
                        print(f"Info - Usage report error (internet access not available?). See readme to disable it, you can ignore safely this. Details: {t.text}")
            except Exception as e:
                print("Usage report error. See readme to disable it. "+(str(e)))
