import time
import traceback

import pymongo.errors
import requests
from enum import Enum
import json
from pymongo import MongoClient
import datetime
import asyncio
import aiohttp

'''
Разделение рангов
Илья - Immortal
Фарид - Легенды
Саша - Эншинт
Искандер - Дивайн
'''


class RankSpread(Enum):
    LEGEND_LOW = 50
    LEGEND_TOP = 55
    ANCIENT_LOW = 60
    ANCIENT_TOP = 65
    DIVINE_LOW = 70
    DIVINE_TOP = 75
    IMMORTAL_LOW = 80
    IMMORTAL_TOP = 85


'''
Соединение монго, можете это не трогать, 
только измените локалку, если у вас порт закрыт
'''


def create_mongo_connection():
    try:
        # Менять соединение с монгой
        client = MongoClient("localhost", 27017)
        db = client.dota_db
        return client, db
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)


# Метод для разделение батчей, можно не трогать
def create_batches(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# Поменяйте на свой токен стратз
local_stratz_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJTdWJqZWN0IjoiYjJjNjQyM2EtZjlmNS00YmI1LWI2MmUtOWRiYzAyZDc2YzQ5IiwiU3RlYW1JZCI6IjM2NzY1MzgzNCIsIm5iZiI6MTY5OTk3MjM5MCwiZXhwIjoxNzMxNTA4MzkwLCJpYXQiOjE2OTk5NzIzOTAsImlzcyI6Imh0dHBzOi8vYXBpLnN0cmF0ei5jb20ifQ.-8nipaADxb1dGUcyKm9aEKtAcE9MS_MVY0khHC3ZhfE"
# Нижний предел ранга поменяйте на свой
local_spread_bot = RankSpread.IMMORTAL_LOW.value
# Верхний предел поменять на свой
local_spread_top = RankSpread.IMMORTAL_TOP.value
# Количество запросов для id матча (default = 99)
remain_opendota_requests = 2
# Количество запросов для данных матча (default = 9900)
remain_stratz_requests = 200
# Количество одновременных запросов в stratz (default = 5)
batch_size = 5
# Ставьте тру, если запускаете первый раз.
# После выполнения скрипта первый раз, ставьте false.
first_run = False
# Id для первого матча. Залезть в dotabuff
# Найдите id какого нить матча, который был вчера и вставьте сюда
# Лучше искать по своему рангу, потому что не гарантирую что ранг другого матча прокатит
less_then_match = 7441443374

mongo_client, mongo_db = create_mongo_connection()
'''
Коллекции монги:
match_id_collection - коллекция id
match_info_collection - обобщенная инфа по матчу
full_match_collection - полная инфа по матчу (гигантский json)
'''
match_ids_coll = mongo_db.match_id_collection
match_info = mongo_db.match_info_collection
full_match = mongo_db.full_match_collection


def opendota_request_match_ids(less_than_match_id):
    # Сюда парсим id
    match_ids = list()

    i = remain_opendota_requests
    try:

        while i > 0:
            # Запрос в опен апи
            req = str(
                'https://api.opendota.com/api/publicMatches?'
                'less_than_match_id=' + str(less_than_match_id) +
                '&max_rank='
                + str(local_spread_top) + '&min_rank=' + str(local_spread_bot))

            public_matches = requests.get(req)
            # вносим id в бд с краткой инфой по матчу
            result_info = match_info.insert_many(public_matches.json())
            # проверяем что все окей
            if result_info is not None:
                print(datetime.datetime.now(), ": matches left: ", i, 'log: ', result_info)

            # Чтобы запросы не блочило
            time.sleep(1)
            # парсим ответ и забираем айдишники
            decoded = json.loads(public_matches.text)

            match_ids.extend((k['match_id'] for k in decoded))
            # последний айди предыдущего листа, с него делаем следующий запрос
            less_than_match_id = match_ids[-1]

            i -= 1

    except requests.ConnectionError as e:
        print(datetime.datetime.now(), ": Connection error occurred in opendota_request_match_ids method")

        print(e)
    except requests.RequestException as e:
        print(datetime.datetime.now(), ": Request error occurred in opendota_request_match_ids method")

        print(e)
    except pymongo.errors.ConnectionFailure as e:
        print(datetime.datetime.now(), ": PyMongo connection error occurred in opendota_request_match_ids method")

        print(e)
    except pymongo.errors.BulkWriteError as e:
        print(datetime.datetime.now(), ": Inserting data error occurred in opendota_request_match_ids method")

        print(e)
    except Exception as e:
        print(datetime.datetime.now(), ": Something went wrong in opendota_request_match_ids method")

        print(e)

    return match_ids


url = 'https://api.stratz.com/graphql'

headers = {"Authorization": f"Bearer {local_stratz_token}"}


async def insert_match_ids_into_db(match_ids):
    try:
        # Формируем айдишники для бд
        if first_run:
            insert_ids = [{'id': m_id,
                           'insert_time': datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=3,
                                                                                                               days=-1)}
                          for m_id in match_ids]
        else:
            insert_ids = [{'id': m_id,
                           'insert_time': datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=3)}
                          for m_id in match_ids]
        # Вносим айдищники в бд через update
        for i in insert_ids:
            update = {"$set": i}
            result_ids = match_ids_coll.update_one(i, update, upsert=True)
            # проверяем на успех
            if result_ids is not None:
                print(datetime.datetime.now(), ": match[id] = ", i.get('id'), "successfully inserted "
                                                                              "into db: @match_id_collection")
            else:
                print(datetime.datetime.now(), ": match[id] = ", i.get('id'), "insertion error occurred")

    except pymongo.errors.BulkWriteError as e:
        print(datetime.datetime.now(), ": Incorrect data insertion into db @match_id_collection")
        print(e)
    except Exception as e:
        print(datetime.datetime.now(), ": Что то пошло не так при внесении в бд айди матчей")
        print(e)

        return False
    return True


def get_yesterday_matches_from_mongo():
    try:

        mongo_client.admin.command('ping')
        # Если вы один день не парсили матчи, поменяйте days на 2
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1, hours=1)
        # для запроса: ниже получаем данные из бд между вчера и сегодня
        today = datetime.datetime.now()

        result = match_ids_coll.find({"insert_time": {"$gte": yesterday, "$lt": today}})

        if result is not None:
            print(datetime.datetime.now(), ": match ids successfully received from @match_id_collection")
        # Отбираем только айдишники
        m_ids = [_id.get('id') for _id in result]
        return m_ids

    except pymongo.errors.ConnectionFailure as e:
        print(datetime.datetime.now(), ": Cant connect to mongodb collection: @match_id_collection")
        print(e)
    except pymongo.errors.InvalidOperation as e:
        print(datetime.datetime.now(), ": Invalid mongodb operation")
        print(e)
    except Exception as e:
        print(datetime.datetime.now(), ": an error occurred in get_yesterday_matches_from_mongo method")
        print(e)


'''
Ниже я создаю батч из 5 queries для запроса в стратз
'''


def create_queries(match_id_batch):
    query_batch = [{'query': '''{
            match(id: %d) {
                id
                didRadiantWin
                durationSeconds
                startDateTime
                endDateTime
                towerStatusRadiant
                towerStatusDire
                barracksStatusRadiant
                barracksStatusDire
                clusterId
                firstBloodTime
                lobbyType
                numHumanPlayers
                gameMode
                replaySalt
                isStats
                actualRank
                averageRank
                averageImp
                parsedDateTime
                statsDateTime
                gameVersionId
                regionId
                sequenceNum
                rank   
                bracket
                analysisOutcome
                predictedOutcomeWeight
                regionId
                sequenceNum
                rank
                bracket
                radiantNetworthLeads
                radiantExperienceLeads
                radiantKills
                direKills
                pickBans{
                  isPick
                  heroId
                  order
                  bannedHeroId
                  isRadiant
                  playerIndex
                  wasBannedSuccessfully
                  isCaptain
                  baseWinRate
                  adjustedWinRate
                  letter
                }
                towerStatus{
                  towers{
                    npcId
                    hp
                  }
                  outposts{
                    isControlledByRadiant
                    npcId
                    isRadiantSide
                  }
                }
                laneReport{
                  radiant{
                    midLane{
                      meleeCount
                      rangeCount
                      siegeCount
                      denyCount
                      neutralCount
                    }
                    offLane{
                      meleeCount
                      rangeCount
                      siegeCount
                      denyCount
                      neutralCount
                    }
                    safeLane{
                      meleeCount
                      rangeCount
                      siegeCount
                      denyCount
                      neutralCount
                    }
                  }
                  dire{
                    midLane{
                      meleeCount
                      rangeCount
                      siegeCount
                      denyCount
                      neutralCount
                    }
                    offLane{
                      meleeCount
                      rangeCount
                      siegeCount
                      denyCount
                      neutralCount
                    }
                    safeLane{
                      meleeCount
                      rangeCount
                      siegeCount
                      denyCount
                      neutralCount
                    }
                  }
                }
                winRates
                predictedWinRates
                chatEvents{
                  time
                  type
                  fromHeroId
                  toHeroId
                  value
                  pausedTick
                  isRadiant
                }
                towerDeaths{
                  time
                  npcId
                  isRadiant
                  attacker
                }
                bottomLaneOutcome
                topLaneOutcome
                midLaneOutcome
                players{
                  playerSlot
                  steamAccount{
                    id
                    profileUri
                    realName
                    name
                    timeCreated
                    countryCode
                    stateCode
                    cityId
                    lastLogOff
                    isDotaPlusSubscriber
                    dotaAccountLevel
                    rankShift
                    isAnonymous
                    isStratzPublic
                    seasonRank
                    seasonLeaderboardRank
                    activity{
                      activity
                    }
                    smurfFlag

                  }
                  isRadiant
                  hero{
                    id
                    name
                    shortName
                    abilities{
                      ability{
                        id
                        name
                        uri
                        stat{
                          abilityId
                          type
                          behavior
                          unitTargetType
                          unitTargetTeam
                          unitTargetFlags
                          unitDamageType
                          spellImmunity
                          modifierSupportValue
                          modifierSupportBonus
                          isOnCastbar
                          isOnLearnbar
                          isGrantedByScepter
                          hasScepterUpgrade
                          maxLevel
                          levelsBetweenUpgrades
                          requiredLevel
                          castRange
                          castRangeBuffer
                          castPoint
                          channelTime
                          cooldown
                          damage
                          manaCost
                          isUltimate
                          duration
                          charges
                          chargeRestoreTime
                          hasShardUpgrade
                          isGrantedByShard
                          dispellable
                          linkedAbilityId
                        }
                        attributes{
                          name
                          value
                          linkedSpecialBonusAbilityId
                          requiresScepter
                        }
                        isTalent
                      }
                    }
                    roles{
                      roleId
                    }
                    talents{
                      abilityId
                    }
                    stats{
                      attackType
                      startingArmor
                      startingDamageMin
                      startingDamageMax
                      attackRate
                      attackAnimationPoint
                      attackAcquisitionRange
                      attackRange
                      primaryAttribute
                      strengthBase
                      strengthGain
                      intelligenceBase
                      intelligenceGain
                      agilityBase
                      agilityGain
                      hpRegen
                      mpRegen
                      moveSpeed
                      hpBarOffset
                      visionDaytimeRange
                      visionNighttimeRange
                      complexity
                      primaryAttributeEnum
                    }
                  }

                  kills
                  deaths
                  assists
                  leaverStatus
                  numLastHits
                  numDenies
                  goldPerMinute
                  networth
                  experiencePerMinute
                  level
                  gold
                  goldSpent
                  heroDamage
                  towerDamage
                  heroHealing
                  isRandom
                  lane
                  position
                  streakPrediction
                  intentionalFeeding
                  role
                  roleBasic
                  imp
                  award
                  item0Id
                  item1Id
                  item2Id
                  item3Id
                  item4Id
                  item5Id
                  backpack0Id
                  backpack1Id
                  backpack2Id
                  neutral0Id
                  behavior
                  stats{
                    killEvents{
                      time
                      target
                      byAbility
                      byItem
                      gold
                      xp
                      positionX
                      positionY
                      assist
                      isSolo
                      isGank
                      isSmoke
                      isInvisible
                      isTpRecently
                    }
                    deathEvents{
                      time
                      attacker
                      target
                      byAbility
                      byItem
                      goldFed
                      goldLost
                      xpFed
                      timeDead
                      positionX
                      positionY
                      assist
                      isWardWalkThrough
                      isBurst
                      isDieBack
                      isTracked
                      isAttemptTpOut
                      isEngagedOnDeath
                      hasHealAvailable
                    }
                    assistEvents{
                      time
                      target
                      gold
                      xp
                      positionX
                      positionY
                    }
                    allTalks{
                      time
                      message
                      pausedTick
                    }
                    lastHitsPerMinute
                    goldPerMinute
                    experiencePerMinute
                    healPerMinute
                    heroDamagePerMinute
                    towerDamagePerMinute
                    towerDamageReport{
                      npcId
                      damage
                      damageCreeps
                      damageFromAbility
                    }
                    courierKills{
                      time
                      positionX
                      positionY
                    }
                   wards{
                    time
                    type
                    positionX
                    positionY
                   }
                   itemPurchases{
                    time
                    itemId
                  }
                   itemUsed{
                    itemId
                    count
                  }
                    chatWheels{
                      time
                      chatWheelId
                      pauseTick
                    }
                    actionsPerMinute
                    actionReport{
                      moveToTarget
                      moveToPosition
                      attackPosition
                      attackTarget
                      castPosition
                      castTarget
                      castNoTarget
                      heldPosition
                      glyphCast
                      scanUsed
                      pingUsed
                    }
                    locationReport{
                      positionX
                      positionY
                    }
                    farmDistributionReport{
                      creepType{
                        id
                        count
                        gold
                        xp
                      }
                      creepLocation{
                        id
                        count
                        gold
                        xp
                      }
                      neutralLocation{
                        id
                        count
                        gold
                        xp
                      }
                      ancientLocation{
                        id
                        count
                        gold
                        xp
                      }
                      buildings{
                        id
                        count
                        gold
                        xp
                      }
                      buyBackGold
                      abandonGold
                      bountyGold{
                        id
                        count
                        gold
                        xp
                      }
                      other{
                        id
                        count
                        gold
                        xp
                      }

                    }
                    runes{
                      time
                      rune
                      action
                      gold
                      positionX
                      positionY
                    }
                    abilityCastReport{
                      abilityId
                      count
                      targets{
                        target
                        count
                        damage
                        duration
                      }
                    }
                    heroDamageReport{
                      dealtTotal{
                        physicalDamage
                        magicalDamage
                        pureDamage
                        selfHeal
                        allyHeal
                        stunCount
                        stunDuration
                        disableCount
                        disableDuration
                        slowCount
                        slowDuration
                      }
                     receivedTotal{
                      physicalDamage
                      magicalDamage
                      pureDamage
                      heal
                      stunCount
                      stunDuration
                      disableCount
                      disableDuration
                      slowCount
                      slowDuration
                    }
                    }
                    inventoryReport{
                      item0{
                        itemId
                        charges
                        secondaryCharges
                      }
                      item1{
                        itemId
                        charges
                        secondaryCharges
                      }

                      item2{
                        itemId
                        charges
                        secondaryCharges

                      }
                      item3{
                        itemId
                        charges
                        secondaryCharges

                      }
                      item4{
                        itemId
                        charges
                        secondaryCharges
                      }

                      item5{
                        itemId
                        charges
                        secondaryCharges
                      }
                      backPack0{
                        itemId
                        charges
                        secondaryCharges
                      }

                      backPack1{
                        itemId
                        charges
                        secondaryCharges
                      }
                      backPack2{
                        itemId
                        charges
                        secondaryCharges
                      }
                      neutral0{
                        itemId
                        charges
                        secondaryCharges
                      }

                    }
                    networthPerMinute
                    campStack
                    matchPlayerBuffEvent{
                      time
                      abilityId
                      itemId
                      stackCount
                    }
                    deniesPerMinute
                    impPerMinute
                    wardDestruction{
                      time
                      gold
                      experience
                      isWard
                    }
                  }
                  playbackData{
                    abilityLearnEvents{
                      time
                      abilityId
                      levelObtained
                      level
                      isUltimate
                      isTalent
                      isMaxLevel
                    }
                    abilityUsedEvents{
                      time
                      abilityId
                      attacker
                      target
                    }
                    abilityActiveLists{
                      time
                      ability0
                      ability1
                      ability2
                      ability3
                      ability4
                      ability5
                      ability6
                      ability7
                    }
                    itemUsedEvents{
                      time
                      itemId
                      attacker
                      target
                    }
                    playerUpdatePositionEvents{
                      time
                      x
                      y
                    }
                  }
                  heroAverage{
                    heroId
                    week
                    time
                    kills
                    deaths
                    kDAAverage
                  }
                  abilities{
                    abilityId
                    time
                    level
                    abilityType{
                      name
                      id
                      language{
                        displayName
                      }
                      isTalent
                    }

                  }
                  invisibleSeconds
                }
                playbackData{
                  courierEvents{
                    id
                    ownerHero
                    isRadiant
                    events{
                      time
                      positionX
                      positionY
                      hp
                      isFlying
                      respawnTime
                      didCastBoost
                      item0Id
                      item1Id
                      item2Id
                      item3Id
                      item4Id
                      item5Id
                    }
                  }
                        runeEvents{
                    indexId
                    time
                    positionX
                    positionY
                    location
                    rune
                    action
                  }
                        wardEvents{
                    indexId
                    time
                    positionX
                    positionY
                    fromPlayer
                    wardType
                    action
                    playerDestroyed
                  }
                        buildingEvents{
                    time
                    indexId
                    type
                    hp
                    maxHp
                    positionX
                    positionY
                    isRadiant
                    npcId
                    didShrineActivate
                  }
                        towerDeathEvents{
                    time
                    radiant
                    dire
                  }
                        roshanEvents{
                    time
                    hp
                    maxHp
                    createTime
                    x
                    y
                    totalDamageTaken
                    item0
                    item1
                    item2
                    item3
                    item4
                    item5
                  }
                        radiantCaptainHeroId
                        direCaptainHeroId
                }
            }


        }''' % _id} for _id in match_id_batch]
    return query_batch


async def get_stratz_data(match_ids):
    batch_index = 0
    stratz_index = 0
    # батч из айдишников, по умолчанию - список из 5 айдишников
    ids_batch = list(create_batches(match_ids, batch_size))
    try:
        # цикл для получения всех запросов из стратза
        while (stratz_index < remain_stratz_requests) & (batch_index < len(ids_batch)):
            # создаем батч из queries
            queries_batch = create_queries(ids_batch[batch_index])
            # засекаем время
            start = datetime.datetime.now()
            # дожидаемся запросов
            match_data = await stratz_api_calls(queries_batch)
            # проверяем данные
            if match_data is None:
                continue
            for match in match_data:
                # проверяем конкретный матч
                if match['data']['match'] is None:
                    print(datetime.datetime.now(), 'Match is None')
                    continue

                else:
                    # проверяем матч на то что обработан стратзом
                    if match['data']['match'].get('parsedDateTime') is None:
                        print(datetime.datetime.now(), 'Match[id]: ', match['data']['match'].get('id'),
                              'not parsed by Stratz yet')
                    else:
                        update = {"$set": {'match': match['data']['match'], 'insert_date': datetime.datetime.now()}}
                        result = full_match.update_one({'match': match['data']['match'],
                                                        'insert_date': datetime.datetime.now()}, update, upsert=True)

                        if result is not None:
                            print(datetime.datetime.now(), ": Current match id: ",
                                  match['data']['match'].get('id'), "successfully inserted "
                                                                    "into db: @full_match_collection", result)
                        else:
                            print(datetime.datetime.now(), 'Error occurred: current match is None: '
                                  , match['data']['match'].get('id'))
            # перестаем засекать время
            end = datetime.datetime.now()
            # если не прошло 6 секунд, засыпаем чтобы в сумме было 6 секунд
            if (end - start).seconds < 6:
                time.sleep(6 - (end - start).seconds)
            print(datetime.datetime.now(), ': Batch[i] = ', batch_index, 'successfully inserted. ',
                  'Batches remain: ', str(len(ids_batch) - batch_index - 1))
            # увеличиваем индексы пробежки массива
            batch_index += 1
            stratz_index += batch_size

    except pymongo.errors.BulkWriteError as e:
        print(e)
    except IndexError as e:
        print(e)
    except Exception as e:
        print(datetime.datetime.now(), ": Что то пошло не так при отправке стратз "
                                       "запроса в бд в батче из следующих матчей: ", ids_batch[batch_index])
        print(traceback.format_exc())


'''
Фомируем 1 запрос в стратз
'''


async def fetch(session, data):
    async with session.post(url, json=data, headers=headers) as response:
        return await response.json()


async def stratz_api_calls(query_batch):
    try:
        async with aiohttp.ClientSession() as session:
            # формируем таски из батчей запросов
            tasks = [asyncio.ensure_future(fetch(session, data)) for data in query_batch]
            # выполняем таски, получая запросы в респонс
            responses = await asyncio.gather(*tasks)
            return responses
    except Exception as e:
        print(datetime.datetime.now(), ": Что то пошло не так при парсинге стратз батча")
        print(e)


async def main():
    if first_run:
        # получаем айдишники в первый раз
        ids = opendota_request_match_ids(less_then_match)
        # отправляем айди и матчи в бд
        success = await insert_match_ids_into_db(ids)

    else:
        # получаем вчерашние айдишники
        yesterday_matches = get_yesterday_matches_from_mongo()
        # запрашиваем матчи за вчера
        ids = opendota_request_match_ids("")
        # вносим айдишники матчей в бд
        success = await insert_match_ids_into_db(ids)
        # получаем ответ от стратза и вносим в бд
        await get_stratz_data(yesterday_matches)


if __name__ == '__main__':
    asyncio.run(main())
