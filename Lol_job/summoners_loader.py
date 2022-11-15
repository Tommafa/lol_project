from resources import base_utils as bu
import pandas as pd
import resources.riot_api_readers as riot_r
from sqlalchemy import create_engine, schema
import urllib
import os
from dotenv import load_dotenv
import logging


def main(logger, env_config, db_password, db_user, api_key, db_schema):
    config_yaml_path = env_config["config_path"]

    # if dev we can read the connection keys ( db and api) from a file,
    # otherwise they are secrets
    header = env_config["header"]
    header["X-Riot-Token"] = api_key

    db_settings = env_config["db_settings"]
    db_settings["username"] = db_user
    db_settings["password"] = db_password
    db_settings["schema"] = db_schema

    # used to obtain the lists of tiers and divisions
    league_structure = bu.read_yaml(config_yaml_path)["league_structure"]
    # db_actions = bu.read_yaml(config_yaml_path)["db_actions"]

    # setup connection
    quoted = urllib.parse.quote_plus(
        db_settings["connection_string"].format(
            db_settings["server"],
            db_settings["db_name"],
            db_settings["username"],
            db_settings["password"],
        )
    )
    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(quoted),
        fast_executemany=True,
    )

    if db_settings["schema"] not in engine.dialect.get_schema_names(engine):
        engine.execute(schema.CreateSchema(db_settings["schema"]))
        logger.info("schema creato")
    else:
        logger.info("schema già presente")
    # verify schema and create it eventually
    # bu.verify_schema(connection, db_settings)

    # given a set of settings for LOL API structure and the header,
    # produce the dict with keys that are going to be
    # our dataset's columns
    summoners_dict, mini_series_dict = riot_r.load_list_of_summoners(
        league_structure["summoners_reader"], header, verbose=True
    )
    # header is used for api consumption
    logger.info("dati scaricati")

    # merge dicts in a single one
    summoners_dict.update(mini_series_dict)

    # transform dict to pandas dataframe
    summoners_pd = pd.DataFrame.from_dict(summoners_dict)

    logger.info(f"the output columns are: {summoners_pd.columns}")
    summoners_pd.drop("miniSeries", axis=1, inplace=True)
    # write data to table

    summoners_pd.to_sql(
        "summoners",
        schema=db_settings["schema"],
        con=engine,
        if_exists="replace",
        index=False,
        index_label="summonerId",
        method="multi",
        chunksize=100,
    )

    """    with create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}',
                       fast_executemany=True) as engine:
        summoners_pd_from_sql = pd.read_sql(
            text(db_actions["retrieve_from_table"]
                 .format("*", db_settings["schema"],
                         db_settings["summoners_table_name"])),
            con=engine)


"""
    logger.info("dati scritti su tabella")

    return summoners_pd


if __name__ == "__main__":

    # read env file to understand where to find information
    try:
        env_conf = bu.read_yaml("resources/env_config.yaml")
        load_dotenv()

        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_USER = os.getenv("DB_USER")
        API_KEY = os.getenv("API_KEY")
        SCHEMA = os.getenv("SCHEMA")
        ENV = os.getenv("ENV")

        log_inst = logging.getLogger(f"{__file__[:-2]}.log")
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filemode="w",
        )
        if ENV == "development":
            log_inst.setLevel(logging.DEBUG)
        else:
            log_inst.setLevel(logging.WARNING)
        main(log_inst, env_conf, DB_PASSWORD, DB_USER, API_KEY, SCHEMA)
    except Exception as e:
        log_inst.error(f"mancano le variabili d'ambiente {e}", exc_info=True)
