from typing import List, Dict, Any

import requests
import pydantic
import urllib.request as urlreq
import urllib.parse as urlp
from resources.league_objects import *
import json
import time


# TODO: change this function to organize it better
def build_summoners_links_per_division(configurations_for_summoners_reader: dict, headers: dict,
                                       verbose: bool = True) -> List[dict]:
    """build list of tiers and divisions to load"""

    links_characteristics_list = []
    # define challenger dict
    challenger = {"base_link": configurations_for_summoners_reader["base_path"],
                  "queue_type": configurations_for_summoners_reader["queue_type"],
                  "tier": configurations_for_summoners_reader["tiers"][0],
                  "division": configurations_for_summoners_reader["divisions"][0],
                  "page": 1,
                  "headers": headers,
                  "verbose": verbose

                  }

    # define grandmaster dict
    grandmaster = {"base_link": configurations_for_summoners_reader["base_path"],
                   "queue_type": configurations_for_summoners_reader["queue_type"],
                   "tier": configurations_for_summoners_reader["tiers"][1],
                   "division": configurations_for_summoners_reader["divisions"][0],
                   "page": 1,
                   "headers": headers,
                   "verbose": verbose

                   }

    # define master dict
    master = {"base_link": configurations_for_summoners_reader["base_path"],
              "queue_type": configurations_for_summoners_reader["queue_type"],
              "tier": configurations_for_summoners_reader["tiers"][2],
              "division": configurations_for_summoners_reader["divisions"][0],
              "page": 1,
              "headers": headers,
              "verbose": verbose

              }

    links_characteristics_list.extend([challenger, grandmaster, master])

    # define dicts for all the other leagues
    links_characteristics_list.extend([{"base_link": configurations_for_summoners_reader["base_path"],
                                        "queue_type": configurations_for_summoners_reader["queue_type"],
                                        "tier": tier,
                                        "division": division,
                                        "page": 1,
                                        "headers": headers,
                                        "verbose": verbose
                                        }
                                       for i, tier in enumerate(configurations_for_summoners_reader["tiers"][3:])
                                       for j, division in enumerate(configurations_for_summoners_reader["divisions"])])

    return links_characteristics_list


def load_summoners_from_riot_api(base_link: str, queue_type: str, tier: str, division: str, page: int,
                                 headers: dict,
                                 verbose: bool = True) -> str:
    """load a list of summoners ordered by rank"""

    # fill link structure with input params
    link_for_request = "{}{}/{}/{}?page={}".format(base_link, queue_type, tier, division, str(page))

    # used to repeat the api call if we have code different from 200
    problems_at_previous_iteration = True
    iteration = 0
    while problems_at_previous_iteration and iteration < 5:# repeat up to 5 times
        try:

            response = requests.get(link_for_request, headers=headers)
            status_code = response.status_code
            if verbose:
                msg = "Everything went well!" if status_code == 200 else "There was an error when handling the request for the following link: {}.".format(
                    link_for_request)

                print(msg)
            if status_code == 200:
                problems_at_previous_iteration = False
                return response.content
            else:
                if verbose:
                    print(response.content)
                # usage limit is n calls every 2 minutes --> when I get an error I wait 2 minutes as it will reset
                time.sleep(120)

        except Exception as e:
            iteration += 1
            print("Unable to get url {} due to {}.".format(link_for_request, e.__class__))


def load_list_of_summoners(configurations_for_summoners_reader: dict, header: dict, verbose: bool = True) -> Dict[str,list]:
    """Retrieves a dataframe with values of each summoner"""
    # prepare dict for pandas df creation
    table_structure_summoners = {}
    for key in LeagueEntryDTO.schema()["properties"].keys():
        table_structure_summoners[key] = []


        # create full list of leagues to load
        links_per_division = build_summoners_links_per_division(configurations_for_summoners_reader, header, verbose)
    print("table structure created")
    # use accessory list comprehension to: call the API, decode the bytes to string, create the dict to append to our
    # initial pandas structure ( the LeagueEntryDTO is necessary as sometimes the miniSeries is absent..)
    [table_structure_summoners[key].append(LeagueEntryDTO(**summoner).dict()[key]) for summoners in links_per_division
     for summoner in eval(
        load_summoners_from_riot_api(**summoners).decode("utf-8").replace("true", "True").replace("false", "False"))
     for key in table_structure_summoners.keys()]
    return table_structure_summoners
