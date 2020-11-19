from os import path

from datahub.common.configuration import IamConfig

iam_configuration = IamConfig()
iam_configuration.add_config('google_iam', path.join(path.dirname(__file__), "google_iam/yaml/"))
iam_configuration.add_config('google_dataset_iam', path.join(path.dirname(__file__), "google_iam/yaml/"))
iam_config = iam_configuration.config
