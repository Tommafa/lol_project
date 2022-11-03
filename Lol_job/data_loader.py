import yaml
from resources import base_utils as bu
from resources.league_objects import *

import resources.riot_api_readers as riot_r
import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

from pyspark import SparkSession


def main(env_config):
    config_yaml_path = env_config["config_path"]

    if env_config["env"] == "development":
        api_key = bu.read_yaml(config_yaml_path)["api_key"]
    else:
        client = botocore.session.get_session().create_client('secretsmanager')
        cache_config = SecretCacheConfig()
        cache = SecretCache(config=cache_config, client=client)

        api_key = cache.get_secret_string('api_key')

    config = bu.read_yaml(config_yaml_path)["config"]
    spark = SparkSession.builder.appName(config["spark_session"]["name"]).getOrCreate()


    return True


if __name__ == "__main__":
    env_config = bu.read_yaml("resources/env_config/env_config.yaml")
    env_conf = env_config

    main(env_conf)
