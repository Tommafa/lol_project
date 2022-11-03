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
        # if dev we can read the connection keys ( db and api) from a file, otherwise they are secrets
        header = env_config["header"]
    else:
        client = botocore.session.get_session().create_client('secretsmanager')
        cache_config = SecretCacheConfig()
        cache = SecretCache(config=cache_config, client=client)

        header = cache.get_secret_string('header')

    # used to obtain the lists of tiers and divisions
    config = bu.read_yaml(config_yaml_path)["leagues_structure"]

    # given a set of settings for LOL API structure and the header, produce the dict with keys that are going to be
    # our dataset's columns
    summoners_dict = load_list_of_summoners(config["summoners_reader"], header, False)

    # transform dict to pandas dataframe
    summoners_pd = pd.DataFrame.from_dict(summoners_dict)

    # TODO: add table writing function
    return summoners_pd


if __name__ == "__main__":

    # read env file to understand where to find information
    env_conf = bu.read_yaml("resources/env_config/env_config.yaml")

    # TODO: define connection settings based on env
    main(env_conf, db_connection_settings=db_connection_settings)
