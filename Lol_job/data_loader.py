import yaml
from resources import base_utils as bu
from resources.league_objects import *
import pandas as pd
import resources.riot_api_readers as riot_r
import botocore
import botocore.session
#from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
from sqlalchemy import create_engine, schema
import urllib
import pyodbc

def main(env_config):


    config_yaml_path = env_config["config_path"]

    if env_config["env"] == "development":
        # if dev we can read the connection keys ( db and api) from a file, otherwise they are secrets
        header = env_config["header"]
        db_settings = env_config["db_settings"]
    else:
        client = botocore.session.get_session().create_client('secretsmanager')
        cache_config = SecretCacheConfig()
        cache = SecretCache(config=cache_config, client=client)

        header = cache.get_secret_string('header')
        db_settings = cache.get_secret_string('db_settings')

    # used to obtain the lists of tiers and divisions
    league_structure = bu.read_yaml(config_yaml_path)["leagues_structure"]



    # setup connection
    quoted = urllib.parse.quote_plus(db_settings["connection_string"].format(db_settings["server"], db_settings["db_name"], db_settings["password"]))
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    # connection = pyodbc.connect(db_settings["connection_string"].format(db_settings["server"], db_settings["db_name"], db_settings["password"]))

    if db_settings["schema"] not in engine.dialect.get_schema_names(engine):
        engine.execute(schema.CreateSchema(db_settings["schema"]))

    # verify schema and create it eventually
    #bu.verify_schema(connection, db_settings)


    print("schema creato")
    # given a set of settings for LOL API structure and the header, produce the dict with keys that are going to be
    # our dataset's columns
    summoners_dict = riot_r.load_list_of_summoners(league_structure["summoners_reader"], header, verbose=True)# header is used for api consumption
    print("dati scaricati")

    # transform dict to pandas dataframe
    summoners_pd = pd.DataFrame.from_dict(summoners_dict)
    summoners_pd.drop("miniSeries", axis=1, inplace=True)
    # write data to table


    summoners_pd.to_sql("summoners", schema=db_settings["schema"],
                        con=engine, if_exists="replace", index=False,
                        index_label="summonerId")

    print("dati scritti su tabella")

    return summoners_pd


if __name__ == "__main__":

    # read env file to understand where to find information
    env_conf = bu.read_yaml("resources/env_config/env_config.yaml")

    # TODO: define connection settings based on env
    main(env_conf)
