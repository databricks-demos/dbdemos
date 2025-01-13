import unittest
from dbdemos.job_bundler import JobBundler
from dbdemos.conf import Conf

class TestJobBundler(unittest.TestCase):
    def setUp(self):
        self.conf = Conf(username="test_user@test.com", 
                        workspace_url="https://test.cloud.databricks.com", 
                        org_id="1234567890", 
                        pat_token="test_token",
                        repo_url="https://github.com/databricks-demos/dbdemos-notebooks",
                        branch="main")
        self.job_bundler = JobBundler(self.conf)

    def test_get_changed_files_since_commit(self):
        # Test with a known repository and commit
        owner = "databricks-demos"
        repo = "dbdemos-notebooks"
        base_commit = "0652f84e30f3ea2e6802dbe5f36538a30f0d8aa1"  # or use a specific commit SHA
        last_commit = "b65796d63f628eacc32f7160033181c3477997dd"
        files = self.job_bundler.get_changed_files_since_commit(owner, repo, base_commit, last_commit)

        # Check that we got a list of files back
        self.assertIsInstance(files, list, "Should return a list of changed files")
        self.assertEqual(files, ['demo-FSI/lakehouse-fsi-smart-claims/02-Data-Science-ML/02.1-Model-Training.py'])
        
        # Check that file paths are strings
        if files:
            self.assertIsInstance(files[0], str, "File paths should be strings")

        # Test with last_commit = None to get changes since base commit up to HEAD
        files_to_head = self.job_bundler.get_changed_files_since_commit(owner, repo, base_commit)
        
        # Check that we got a non-empty list back
        self.assertIsInstance(files_to_head, list, "Should return a list of changed files")
        self.assertGreater(len(files_to_head), 0, "Should have at least one changed file")
        
        # Check that file paths are strings
        self.assertIsInstance(files_to_head[0], str, "File paths should be strings")

    def test_check_if_demo_file_changed_since_commit(self):
        from dbdemos.conf import DemoConf
        
        # Create a demo config for testing with required name field
        demo_conf = DemoConf("demo-FSI/lakehouse-fsi-smart-claims", {
            "name": "lakehouse-fsi-smart-claims", 
            "title": "FSI Smart Claims",
            "category": "test",
            "description": "description",
            "bundle": True
        })
        
        # Test with known commits where we know a file changed in that demo
        base_commit = "0652f84e30f3ea2e6802dbe5f36538a30f0d8aa1"
        last_commit = "b65796d63f628eacc32f7160033181c3477997dd"
        
        # This should return True as we know a file changed in this demo between these commits
        has_changes = self.job_bundler.check_if_demo_file_changed_since_commit(demo_conf, base_commit, last_commit)
        self.assertTrue(has_changes, "Should detect changes in demo files")
        
        # Test with a demo path that we know didn't change
        demo_conf_no_changes = DemoConf("demo-retail", {
            "name": "demo-retail",
            "title": "Demo Retail", 
            "category": "test",
            "description": "description",
            "bundle": True
        })
        has_changes = self.job_bundler.check_if_demo_file_changed_since_commit(demo_conf_no_changes, base_commit, last_commit)
        self.assertFalse(has_changes, "Should not detect changes in unmodified demo")

if __name__ == '__main__':
    unittest.main() 