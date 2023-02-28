from typing import List
from pydantic import BaseModel


class MiniSeriesDTO(BaseModel):
    losses: int
    progress: str
    target: int
    wins: int


class LeagueEntryDTO(BaseModel):
    leagueId: str
    summonerId: str
    summonerName: str
    queueType: str
    tier: str
    rank: str
    leaguePoints: int
    wins: int
    losses: int
    hotStreak: bool
    veteran: bool
    freshBlood: bool
    inactive: bool
    miniSeries: MiniSeriesDTO = MiniSeriesDTO(
        **{"target": 0, "wins": 0, "losses": 0, "progress": "NNNNN"}
    )

    def full_dict(self, puuid):
        d = {
            "leagueId": self.leagueId,
            "summonerId": self.summonerId,
            "summonerName": self.summonerName,
            "queueType": self.queueType,
            "tier": self.tier,
            "rank": self.rank,
            "leaguePoints": self.leaguePoints,
            "wins": self.wins,
            "losses": self.losses,
            "hotStreak": self.hotStreak,
            "veteran": self.veteran,
            "freshBlood": self.freshBlood,
            "inactive": self.inactive,
            "miniSeries": str(self.miniSeries.dict()),
            "puuid": puuid,
        }
        return d


class MetadataDto(BaseModel):
    dataVersion: str
    matchId: str
    participants: List[str]


class PerkStyleSelectionDto(BaseModel):
    perk: int
    var1: int
    var2: int
    var3: int


class PerkStyleDto(BaseModel):
    description: str
    selections: List[PerkStyleSelectionDto]
    style: int


class PerkStatsDto(BaseModel):
    defense: int
    flex: int
    offense: int


class PerksDto(BaseModel):
    statPerks: PerkStatsDto
    styles: List[PerkStyleDto]


class PartecipantDto(BaseModel):
    assists: int
    baronKills: int
    bountyLevel: int
    champExperience: int
    champLevel: int
    championId: int
    championName: str
    championTransform: int
    consumablesPurchased: int
    damageDealtToBuildings: int
    damageDealtToObjectives: int
    damageDealtToTurrets: int
    damageSelfMitigated: int
    deaths: int
    detectorWardsPlaced: int
    doubleKills: int
    dragonKills: int
    firstBloodAssist: bool
    firstBloodKill: bool
    firstTowerAssist: bool
    firstTowerKill: bool
    gameEndedInEarlySurrender: bool
    gameEndedInSurrender: bool
    goldEarned: int
    goldSpent: int
    individualPosition: str
    inhibitorKills: int
    inhibitorTakedowns: int
    inhibitorsLost: int
    item0: int
    item1: int
    item2: int
    item3: int
    item4: int
    item5: int
    item6: int
    itemsPurchased: int
    killingSprees: int
    kills: int
    lane: str
    largestCriticalStrike: int
    largestKillingSpree: int
    largestMultiKill: int
    longestTimeSpentLiving: int
    magicDamageDealt: int
    magicDamageDealtToChampions: int
    magicDamageTaken: int
    neutralMinionsKilled: int
    nexusKills: int
    nexusTakedowns: int
    nexusLost: int
    objectivesStolen: int
    objectivesStolenAssists: int
    participantId: int
    pentaKills: int
    perks: PerksDto
    physicalDamageDealt: int
    physicalDamageDealtToChampions: int
    physicalDamageTaken: int
    profileIcon: int
    puuid: str
    quadraKills: int
    riotIdName: str
    riotIdTagline: str
    role: str
    sightWardsBoughtInGame: int
    spell1Casts: int
    spell2Casts: int
    spell3Casts: int
    spell4Casts: int
    summoner1Casts: int
    summoner1Id: int
    summoner2Casts: int
    summoner2Id: int
    summonerId: str
    summonerLevel: int
    summonerName: str
    teamEarlySurrendered: bool
    teamId: int
    teamPosition: str
    timeCCingOthers: int
    timePlayed: int
    totalDamageDealt: int
    totalDamageDealtToChampions: int
    totalDamageShieldedOnTeammates: int
    totalDamageTaken: int
    totalHeal: int
    totalHealsOnTeammates: int
    totalMinionsKilled: int
    totalTimeCCDealt: int
    totalTimeSpentDead: int
    totalUnitsHealed: int
    tripleKills: int
    trueDamageDealt: int
    trueDamageDealtToChampions: int
    trueDamageTaken: int
    turretKills: int
    turretTakedowns: int
    turretsLost: int
    unrealKills: int
    visionScore: int
    visionWardsBoughtInGame: int
    wardsKilled: int
    wardsPlaced: int
    win: bool


class ObjectiveDto(BaseModel):
    first: bool
    kills: int


class ObjectivesDto(BaseModel):
    baron: ObjectiveDto
    champion: ObjectiveDto
    dragon: ObjectiveDto
    inhibitor: ObjectiveDto
    riftHerald: ObjectiveDto
    tower: ObjectiveDto


class BanDto(BaseModel):
    championId: int
    pickTurn: int


class TeamDto(BaseModel):
    bans: List[BanDto]
    objectives: ObjectivesDto
    teamId: int
    win: bool


class InfoDto(BaseModel):
    gameCreation: float
    gameDuration: float
    gameEndTimestamp: float
    gameId: float
    gameMode: str
    gameName: str
    gameStartTimestamp: float
    gameType: str
    gameVersion: str
    mapId: int
    participants: List[PartecipantDto]
    platformId: str
    queueId: int
    teams: List[TeamDto]
    tournamentCode: str


class MatchDto(BaseModel):
    metadata: MetadataDto
    info: InfoDto

    def players_list(self):
        return self.metadata.participants

    def get_game_dict(self):
        match_data = {
            "gameCreation": self.info.gameCreation,
            "gameDuration": self.info.gameDuration,
            "matchId": self.metadata.matchId,
            "gameMode": self.info.gameMode,
            "queueId": self.info.queueId,
            "gameEndedInEarlySurrender": self.info.participants[
                0
            ].gameEndedInEarlySurrender,
            "gameEndedInSurrender": self.info.participants[0].gameEndedInSurrender,
        }
        for team in self.info.teams:
            if team.teamId == 100:
                team100_win = team.win
            if team.teamId == 200:
                team200_win = team.win
        match_data["team100Win"] = team100_win
        match_data["team200Win"] = team200_win
        return match_data

    def get_game_summoner_dict(self):
        game_details_players = []
        for i, partecipant in enumerate(self.players_list()):
            participant_stats = {
                "matchId": self.metadata.matchId,
                "puuid": self.info.participants[i].puuid,
                "championLevel": self.info.participants[i].champLevel,
                "kills": self.info.participants[i].kills,
                "assists": self.info.participants[i].assists,
                "deaths": self.info.participants[i].deaths,
                "individualPosition": self.info.participants[i].individualPosition,
                "teamPosition": self.info.participants[i].role,
                "killingSprees": self.info.participants[i].killingSprees,
                "lane": self.info.participants[i].lane,
                "longestTimeSpentLiving": self.info.participants[
                    i
                ].longestTimeSpentLiving,
                "objectivesStolen": self.info.participants[i].objectivesStolen,
                "role": self.info.participants[i].role,
                "summoner1Id": self.info.participants[i].summoner1Id,
                "summoner2Id": self.info.participants[i].summoner2Id,
                "visionScore": self.info.participants[i].visionScore,
                "totalTimeSpentDead": self.info.participants[i].totalTimeSpentDead,
                "championId": self.info.participants[i].championId,
                "win": self.info.participants[i].win,
            }
            game_details_players.append(participant_stats)
        return game_details_players
