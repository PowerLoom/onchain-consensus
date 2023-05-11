def get_epoch_submissions_htable_key(project_id, epoch_end):
    """
    projectid_epoch_submissions_htable -> instance ID - > JSON {snapshotCID:,  submittedTS:}
    }
    """
    return f'projectID:{project_id}:{epoch_end}:centralizedConsensus:epochSubmissions'


def get_epoch_submission_schedule_key(project_id, epoch_end):
    # {'start': , 'end': }
    return f'projectID:{project_id}:{epoch_end}:centralizedConsensus:submissionSchedule'


def get_project_finalized_epoch_cids_htable(project_id):
    return f'projectID:{project_id}:centralizedConsensus:finalizedEpochs'


def get_project_registered_peers_set_key(project_id):
    return f'projectID:{project_id}:centralizedConsensus:peers'


def get_project_epoch_specific_accepted_peers_key(project_id, epoch_end):
    return f'projectID:{project_id}:{epoch_end}:centralizedConsensus:acceptedPeers'


def get_epoch_generator_last_epoch():
    return 'epochGenerator:lastEpoch'


def get_epoch_generator_epoch_history():
    return 'epochGenerator:epochHistory'


def get_snapshotter_issues_reported_key(snapshotter_id):
    return f'snapshotterData:{snapshotter_id}:issuesReported'


# string key:value pair that holds jsonified metadata on UUID peers
def get_snapshotter_info_key(alias):
    return f'snapshotterInfo:{alias}'


# SET of allowed snapshotters by UUID
def get_snapshotter_info_allowed_snapshotters_key():
    return 'snapshotterInfo:allowedSnapshotters'


# HTABLE mapping snapshotter UUID -> snapshotter alias
def get_snapshotter_info_snapshotter_mapping_key():
    return 'snapshotterInfo:snapshotterMapping'


rpc_json_rpc_calls = (
    'rpc:jsonRpc:calls'
)

rpc_get_event_logs_calls = (
    'rpc:eventLogsCount:calls'
)

rpc_web3_calls = (
    'rpc:web3:calls'
)

rpc_blocknumber_calls = (
    'rpc:blocknumber:calls'
)
