from typing import Optional, List
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
    miniSeries: MiniSeriesDTO = MiniSeriesDTO(**{'target': 0, 'wins': 0, 'losses': 0, 'progress': 'NNNNN'})

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






