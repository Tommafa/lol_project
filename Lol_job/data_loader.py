import yaml
from resources import base_utils as bu
from resources.league_objects import *

import resources.riot_api_readers as riot_r
import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

from pyspark import SparkSession


def main(env_config, db_connection_settings):
    config_yaml_path = env_config["config_path"]

    # TODO: setup connection to db
    if env_config["env"] == "development":
        header = env_config["header"]
    else:
        client = botocore.session.get_session().create_client('secretsmanager')
        cache_config = SecretCacheConfig()
        cache = SecretCache(config=cache_config, client=client)

        header = cache.get_secret_string('header')

    config = bu.read_yaml(config_yaml_path)["config"]
    summoners_list = load_list_of_summoners(config, header, False)
    summoner_empty_table = bu.build_table_structure_based_on_dict(summoners_list[0].dict())

    # TODO: add table writing function
    return True


if __name__ == "__main__":
    env_config = bu.read_yaml("resources/env_config/env_config.yaml")
    env_conf = env_config
    # TODO: define connection settings based on env
    main(env_conf, db_connection_settings=db_connection_settings)
