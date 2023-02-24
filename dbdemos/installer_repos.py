from .conf import DemoConf, merge_dict, ConfTemplate
import json
import time

class InstallerRepo:
    def __init__(self, installer):
        self.installer = installer
        self.db = installer.db

    #Start the init job if it exists
    def install_repos(self, demo_conf: DemoConf):
        repos = []
        if len(demo_conf.repos) > 0:
            print(f"    Loading demo repos")
            #We have an init jon
            for repo in demo_conf.repos:
                repo_id = self.update_or_create_repo(repo)
                #returns the path as id as that's what we'll change in the URL (#/workspace/<the/repo/path>/README.md)
                # see notebook_parser.replace_dynamic_links_repo for more details.
                repos.append({"uid": repo['path'], "id": repo['id'], "repo_id": repo_id})
        return repos

    def get_repos(self, path_prefix):
        assert len(path_prefix) > 2
        return self.db.get("/2.0/repos", {"path_prefix": path_prefix})


    def update_or_create_repo(self, repo):
        repo_path = repo['path']
        folder = repo_path[:repo_path.rfind('/')]
        r = self.get_repos(repo_path)
        #No repo, clone it
        if 'repos' not in r:
            if repo_path.endswith('/'):
                repo_path = repo_path[:-1]
            f = self.installer.db.post("/2.0/workspace/mkdirs", json = { "path": folder})
            data = {
                "url": repo['url'],
                "branch": repo['branch'],
                "provider": repo['provider'],
                "path": repo_path
            }
            r = self.db.post("/2.0/repos", data)
            if 'error_code' in r:
                error = f"ERROR - Could not clone the repo {repo['url']} under {repo_path}: {r}"
                raise Exception(error)
            r = self.get_repos(repo['url'])
            return r['id']
        repo_id = r['repos'][0]["id"]
        r = self.db.patch(f"/2.0/repos/{repo_id}", {"branch": repo["branch"]})
        if 'error_code' in r and r['error_code'] == 'GIT_CONFLICT':
            print(f"Error during repo pull {repo_path}: Git conflict. Please resolve manual conflict to get the last version.")
        return repo_id
