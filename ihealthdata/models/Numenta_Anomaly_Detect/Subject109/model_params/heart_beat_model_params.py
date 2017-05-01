MODEL_PARAMS = \
{ 'aggregationInfo': { 'days': 0,
                       'fields': [],
                       'hours': 0,
                       'microseconds': 0,
                       'milliseconds': 0,
                       'minutes': 0,
                       'months': 0,
                       'seconds': 0,
                       'weeks': 0,
                       'years': 0},
  'model': 'CLA',
  'modelParams': { 'anomalyParams': { u'anomalyCacheRecords': None,
                                      u'autoDetectThreshold': None,
                                      u'autoDetectWaitRecords': None},
                   'clParams': { 'alpha': 0.050050000000000004,
                                 'regionName': 'SDRClassifierRegion',
                                 'steps': '1',
                                 'verbosity': 0},
                   'inferenceType': 'TemporalAnomaly',
                   'sensorParams': { 'encoders': { u'V10': None,
                                                   u'V11': None,
                                                   u'V12': None,
                                                   u'V13': None,
                                                   u'V14': None,
                                                   u'V15': None,
                                                   u'V16': None,
                                                   u'V17': None,
                                                   u'V18': None,
                                                   u'V19': None,
                                                   u'V20': None,
                                                   u'V21': None,
                                                   u'V22': None,
                                                   u'V23': None,
                                                   u'V24': None,
                                                   u'V25': None,
                                                   u'V26': None,
                                                   u'V27': None,
                                                   u'V28': None,
                                                   u'V29': None,
                                                   u'V3': None,
                                                   u'V30': None,
                                                   u'V31': None,
                                                   u'V32': None,
                                                   u'V33': None,
                                                   u'V34': None,
                                                   u'V35': None,
                                                   u'V36': None,
                                                   u'V37': None,
                                                   u'V38': None,
                                                   u'V39': None,
                                                   u'V4': None,
                                                   u'V40': None,
                                                   u'V41': None,
                                                   u'V42': None,
                                                   u'V43': None,
                                                   u'V44': None,
                                                   u'V45': None,
                                                   u'V46': None,
                                                   u'V47': None,
                                                   u'V48': None,
                                                   u'V49': None,
                                                   u'V5': None,
                                                   u'V50': None,
                                                   u'V51': None,
                                                   u'V52': None,
                                                   u'V53': None,
                                                   u'V54': None,
                                                   u'V6': None,
                                                   u'V7': None,
                                                   u'V8': None,
                                                   u'V9': None,
                                                   u'heartbeat': { 'clipInput': True,
                                                                   'fieldname': 'heartbeat',
                                                                   'maxval': 154,
                                                                   'minval': 138,
                                                                   'n': 272,
                                                                   'name': 'heartbeat',
                                                                   'type': 'ScalarEncoder',
                                                                   'w': 21},
                                                   u'timestamp_dayOfWeek': None,
                                                   u'timestamp_timeOfDay': None,
                                                   u'timestamp_weekend': None},
                                     'sensorAutoReset': None,
                                     'verbosity': 0},
                   'spEnable': True,
                   'spParams': { 'columnCount': 2048,
                                 'globalInhibition': 1,
                                 'inputWidth': 0,
                                 'maxBoost': 1.0,
                                 'numActiveColumnsPerInhArea': 40,
                                 'potentialPct': 0.8,
                                 'seed': 1956,
                                 'spVerbosity': 0,
                                 'spatialImp': 'cpp',
                                 'synPermActiveInc': 0.05,
                                 'synPermConnected': 0.1,
                                 'synPermInactiveDec': 0.05015},
                   'tpEnable': True,
                   'tpParams': { 'activationThreshold': 14,
                                 'cellsPerColumn': 32,
                                 'columnCount': 2048,
                                 'globalDecay': 0.0,
                                 'initialPerm': 0.21,
                                 'inputWidth': 2048,
                                 'maxAge': 0,
                                 'maxSegmentsPerCell': 128,
                                 'maxSynapsesPerSegment': 32,
                                 'minThreshold': 11,
                                 'newSynapseCount': 20,
                                 'outputType': 'normal',
                                 'pamLength': 3,
                                 'permanenceDec': 0.1,
                                 'permanenceInc': 0.1,
                                 'seed': 1960,
                                 'temporalImp': 'cpp',
                                 'verbosity': 0},
                   'trainSPNetOnlyIfRequested': False},
  'predictAheadTime': None,
  'version': 1}