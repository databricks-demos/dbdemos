import requests

class Tracker:
    #Set this value to false to disable dbdemo toolkit tracker.
    enable_tracker = True
    TID = "UA-163989034-1"
    GTM = "GTM-NKQ8TT7"

    def __init__(self, org_id, uid):
        self.org_id = org_id
        self.uid = uid

    def track_install(self, category, demo_name):
        self.track(category, demo_name, "INSTALL")
    
    def track_create_cluster(self, category, demo_name):
        self.track(category, demo_name, "CREATE_CLUSTER")
    
    def track_list(self):
        self.track("list_demos", "list_demos", "LIST")

    def get_track_url(self, category, demo_name, event, notebook = ""):
        if not Tracker.enable_tracker:
            return ""
        if len(notebook) > 0:
            notebook = '/'+notebook
        params = {"v": 1, "gtm": Tracker.GTM, "tid": Tracker.TID, "cid": 555, "aip": 1, "t": "event",
                  "ec":"dbdemos", "ea":"display", "dp": f"/_dbdemos/{category}/{demo_name}{notebook}",
                  "cid": self.org_id, "uid": self.uid, "ea": event}
        return params

    def track(self, category,  demo_name, event):
        if Tracker.enable_tracker:
            headers = {"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                        "accept-encoding": "gzip, deflate, br",
                        "accept-language": "en-US,en;q=0.9",
                        "cache-control": "max-age=0",
                        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
            try:
                t = requests.get("https://www.google-analytics.com/collect", params = self.get_track_url(category, demo_name, event), headers=headers, timeout=5)
                if t.status_code != 200:
                    print(f"Usage report error. See readme to disable it. {t.text}")
            except Exception as e:
                print("Usage report error. See readme to disable it. "+(str(e)))
