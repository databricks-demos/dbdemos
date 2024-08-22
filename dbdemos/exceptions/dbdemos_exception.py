
class TokenException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class ClusterException(Exception):
    def __init__(self, message, cluster_conf, response):
        super().__init__(message)
        self.response = response
        self.cluster_conf = cluster_conf


class ClusterPermissionException(ClusterException):
    def __init__(self, message, cluster_conf, response):
        super().__init__(message, cluster_conf, response)


class ClusterCreationException(ClusterException):
    def __init__(self, message, cluster_conf, response):
        super().__init__(message, cluster_conf, response)


class GenieCreationException(Exception):
    def __init__(self, message, genie_conf, response):
        super().__init__(message)
        self.response = response
        self.genie_conf = genie_conf


class ExistingResourceException(Exception):
    def __init__(self, install_path, response):
        super().__init__(f"Folder {install_path} isn't empty.")
        self.install_path = install_path
        self.response = response

class DataLoaderException(Exception):
    def __init__(self, message):
        super().__init__(message)

class FolderDeletionException(Exception):
    def __init__(self, install_path, response):
        super().__init__(f"Can't delete folder {install_path}.")
        self.install_path = install_path
        self.response = response

class FolderCreationException(Exception):
    def __init__(self, install_path, response):
        super().__init__(f"Can't load notebook {install_path}.")
        self.install_path = install_path
        self.response = response



class DLTException(Exception):
    def __init__(self, message, description, pipeline_conf, response):
        super().__init__(message)
        self.description = description
        self.pipeline_conf = pipeline_conf
        self.response = response

class DLTNotAvailableException(DLTException):
    def __init__(self, message, pipeline_conf, response):
        super().__init__("DLT not available", message, pipeline_conf, response)

class DLTCreationException(DLTException):
    def __init__(self, message, pipeline_conf, response):
        super().__init__("DLT creation failure", message, pipeline_conf, response)

class WorkflowException(Exception):
    def __init__(self, message, details, job_config, response):
        super().__init__(message)
        self.details = details
        self.job_config = job_config
        self.response = response

