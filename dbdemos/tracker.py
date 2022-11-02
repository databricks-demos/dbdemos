import requests
from urllib import parse

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
            notebook = parse.quote('/'+notebook, safe='')
        return f"https://www.google-analytics.com/collect?v=1&gtm={Tracker.GTM}&tid={Tracker.TID}&cid=555&aip=1&t=event&ec=dbdemos&ea=display&dp=%2F_dbdemos%2F{parse.quote(category, safe='')}%2F{parse.quote(demo_name, safe='')}{notebook}&cid={self.org_id}&uid={self.uid}&ea={event}"

    def track(self, category,  demo_name, event):
        if Tracker.enable_tracker:
            requests.get(self.get_track_url(category, demo_name, event))
