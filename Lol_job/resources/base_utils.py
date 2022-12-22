import pandas as pd
import datatable as dt
import yaml
import logging
import requests
from time import time
from sqlalchemy import text


def fast_pandas_reader(filename: str):
    return dt.fread(filename).to_pandas()


pd.fast_pandas_reader = fast_pandas_reader


def read_yaml(file_path: str):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def make_request(
    link_for_request,
    header,
    verbose: bool,
    logger: logging.Logger,
    max_iter: int = 5,
    seconds_waited_for_retry: int = 120,
):
    """used to make requests to riot"""
    # used to repeat the api call if we have code different from 200
    problems_at_previous_iteration = True
    iteration = 0
    while problems_at_previous_iteration and iteration < max_iter:
        # repeat up to max_iter times
        try:

            response = requests.get(link_for_request, headers=header)
            status_code = response.status_code
            if verbose:
                msg = (
                    "Everything went well!"
                    if status_code == 200
                    else "There was an error when handling the "
                    "request for the following link: {}.".format(link_for_request)
                )

                logger.info(msg)
            if status_code == 200:
                print("done")
                problems_at_previous_iteration = False
                return response.content
            else:
                print("else")

                if verbose:
                    logger.info(response.content)

                # usage limit is n calls every 2 minutes -->
                # when I get an error I wait 2 minutes
                # as it is the reset time (check API docs)
                time.sleep(seconds_waited_for_retry)
                iteration += 1
        except Exception as e:
            iteration += 1
            logger.error("Unable to get url {} due to {}.".format(link_for_request, e))


def func_a(
    req,
    header,
    max_iter: int = 5,
):
    i = 5
    for j in range(i):
        r = requests.get(req, header)
        return r


def setup_db(
    connection,
    schema: str,
    game_table: str,
    game_index: str,
    summoner_table: str,
    summoner_index: str,
    champion_table: str,
    champion_index: str,
    champion_summoner_table: str,
    champion_summoner_clustered_index: str,
    champion_summoner_unclustered_index: str,
    game_summoner_table: str,
    game_summoner_clustered_index: str,
    game_summoner_unclustered_index: str,
):
    """
    This function creates a schema and all the tables we will use for this project.
    Before actually running it checks for existence.
    """

    create_schema_query = f"""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = {schema})
    BEGIN
    EXEC('CREATE SCHEMA {schema}')
    END
    """

    create_game_table = f"""
    IF OBJECT_ID(N'{schema}.{game_table}', N'U') IS NULL
    CREATE TABLE {schema}.{game_table} (
        internal_game_id smallint NOT NULL IDENTITY(1,1),
        gameCreation int,
        gameDuration int,
        matchId int,
        gameMode varchar(50),
        queueId smallint,
        gameEndedInEarlySurrender bit,
        gameEndedInSurrender bit,
        team100Win bit,
        team200Win bit,
        potential_t0 bit
        );
    If IndexProperty(Object_Id({schema}.{game_table}), {game_index}, 'IndexID') Is Null

    CREATE CLUSTERED INDEX {game_index}
    ON {schema}.{game_table} (gameCreation ASC)
    GO
    """

    create_summoner_table = f"""
    IF OBJECT_ID(N'{schema}.{summoner_table}', N'U') IS NULL
    CREATE TABLE {schema}.{summoner_table} (
        internal_player_id smallint NOT NULL IDENTITY(1,1),
        puuid char(78),
        encrypted_summoner_id varchar(63),
        tier varchar(20),
        rank varchar(3),
        wins smallint,
        losses smallint
    );
If IndexProperty(Object_Id({schema}.{summoner_table}),
{summoner_index}, 'IndexID') Is Null

    CREATE CLUSTERED INDEX {summoner_index}
    ON {schema}.{summoner_table} (internal_player_id ASC)
    GO
    """

    create_champion_table = f"""
        IF OBJECT_ID(N'{schema}.{champion_table}', N'U') IS NULL
        CREATE TABLE {schema}.{champion_table} (
            internal_champion_id smallint NOT NULL IDENTITY(1,1),
            champion_id int,
            champion_name varchar(63)
        );
        If IndexProperty(Object_Id({schema}.{champion_table}),
{champion_index}, 'IndexID') Is Null

        CREATE CLUSTERED INDEX {champion_index}
        ON {schema}.{champion_table} (internal_champion_id ASC)
        GO
        """

    create_champion_summoner_table = f"""
        IF OBJECT_ID(N'{schema}.{champion_summoner_table}', N'U') IS NULL
        CREATE TABLE {schema}.{champion_summoner_table} (
            internal_champion_id smallint NOT NULL,
            internal_player_id smallint NOT NULL,
            win_rate decimal(2,2),
            championLevel int
        );
        If IndexProperty(Object_Id({schema}.{champion_summoner_table}),
{champion_summoner_clustered_index}, 'IndexID') Is Null
        CREATE CLUSTERED INDEX {champion_summoner_clustered_index}
        ON {schema}.{champion_summoner_table} (internal_champion_id ASC)
        If IndexProperty(Object_Id({schema}.{champion_summoner_table}),
{champion_summoner_unclustered_index}, 'IndexID') Is Null
        CREATE NONCLUSTERED INDEX {champion_summoner_unclustered_index}
        ON {schema}.{champion_summoner_table} (internal_player_id)
        GO
        """

    create_game_summoner_table = f"""
        IF OBJECT_ID(N'{schema}.{game_summoner_table}', N'U') IS NULL
        CREATE TABLE {schema}.{game_summoner_table} (
            internal_game_id smallint NOT NULL,
            internal_player_id smallint NOT NULL,
            internal_champion_id smallint NOT NULL,
            championLevel int,
            kills tinyint,
            assists tinyint,
            deaths tinyint,
            individualPosition varchar(7),
            teamPosition varchar(7),
            killingSprees int,
            lane varchar(7),
            longestTimeSpentLiving int,
            objectivesStolen tinyint,
            role varchar(20),
            summoner1Id tinyint,
            summoner2Id tinyint,
            visionScore int,
            totalTimeSpentDead int
        );
        If IndexProperty(Object_Id({schema}.{game_summoner_table}),
{game_summoner_clustered_index}, 'IndexID') Is Null
        CREATE CLUSTERED INDEX {game_summoner_clustered_index}
        ON {schema}.{game_summoner_table} (internal_champion_id ASC)
        If IndexProperty(Object_Id({schema}.{game_summoner_table}),
{game_summoner_unclustered_index}, 'IndexID') Is Null
        CREATE NONCLUSTERED INDEX {game_summoner_unclustered_index}
        ON {schema}.{game_summoner_table} (internal_player_id)
        GO"""

    # create schema
    connection.execute(text(create_schema_query))

    # create game table with index
    connection.execute(text(create_game_table))

    # create summoner table with index
    connection.execute(text(create_summoner_table))

    # create champion table with index
    connection.execute(text(create_champion_table))

    # create champion-summoner table with index
    connection.execute(text(create_champion_summoner_table))

    # create game-summoner table with index
    connection.execute(text(create_game_summoner_table))


# TODO: change query using columns_of_interest, add
def build_business_query(connection, num, columns_of_interest):

    query = """
    with to_be_pivoted_query as
(select  t0_g.matchId as current_match, other_games.*

from

(select g.*,gs.internal_player_id,  RANK()
over (partition by gs.internal_player_id order by g.gameCreation asc) as game_order
from sk.game_summoner_table as gs
inner join sk.game_table as g on
gs.internal_game_id=g.internal_game_id) as other_games

inner join

(select g.matchId, gs.internal_player_id,  RANK()
over (partition by gs.internal_player_id order by g.gameCreation asc) as game_order
from sk.game_summoner_table as gs inner join

sk.game_table as g on gs.internal_game_id=g.internal_game_id
where g.potential_t0=1) as t0_g

on t0_g.internal_player_id=other_games.internal_player_id

where other_games.game_order>t0_g.game_order and
other_games.game_order<t0_g.game_order + 6 and other_games.matchId <>t0_g.matchId

select current_game

)"""

    for id in range(1, num + 1):
        query += f"""
                    ,internal_game_id_{id} =  max(case when
                    RN= {id} then A.internal_game_id end)
                    ,gameCreation_{id} =  max(case when
                    RN= {id} then A.gameCreation end)
                    ,gameDuration_{id} =  max(case when
                    RN= {id} then A.gameDuration end)
                    ,matchId_{id} =  max(case when
                    RN= {id} then A.matchId end)
                    ,gameMode_{id} =  max(case when
                    RN= {id} then A.gameMode_ end)
                    ,gameEndedInEarlySurrender_{id} =
                    max(case when RN= {id} then A.gameEndedInEarlySurrender end)
                    ,queueId_{id} =  max(case when
                    RN= {id} then A.queueId end)
                    ,gameEndedInSurrender_{id} =
                    max(case when RN= {id} then A.gameEndedInSurrender end)
                    ,team100Win_{id} =
                    max(case when RN= {id} then A.team100Win end)
                    ,team200Win_{id} =  max(case when
                     RN= {id} then A.team200Win end)
                    ,internal_player_id_{id} =  max(case when
                     RN= {id} then A.internal_player_id end)
                    """
    query += """  From  (
        Select *
              ,RN = row_number() over
              (partition by current_match order by (select null))
         from to_be_pivoted_query
       ) A
 Group By current_match"""
    return query
