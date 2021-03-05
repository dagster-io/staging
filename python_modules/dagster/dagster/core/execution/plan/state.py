from collections import defaultdict
from typing import Dict, List, NamedTuple

from dagster import check
from dagster.core.events.log import EventRecord
from dagster.core.execution.retries import RetryState
from dagster.serdes import whitelist_for_serdes


@whitelist_for_serdes
class KnownExecutionState(
    NamedTuple(
        "_KnownExecutionState",
        [
            # step_key -> count
            ("previous_retry_attempts", Dict[str, int]),
            # step_key -> output_name -> mapping_keys
            ("dynamic_mappings", Dict[str, Dict[str, List[str]]]),
        ],
    )
):
    """
    A snapshot for the parts of an on going execution that need to be handed down when delegating
    step execution to another machine/process. This includes things like previous retries and
    resolved dynamic outputs.
    """

    def __new__(cls, previous_retry_attempts, dynamic_mappings):

        return super(KnownExecutionState, cls).__new__(
            cls,
            check.dict_param(
                previous_retry_attempts, "previous_retry_attempts", key_type=str, value_type=int
            ),
            check.dict_param(dynamic_mappings, "dynamic_mappings", key_type=str, value_type=dict),
        )

    def get_retry_state(self):
        return RetryState(self.previous_retry_attempts)

    @staticmethod
    def derive_from_logs(logs: List[EventRecord]):
        previous_retry_attempts: Dict[str, int] = defaultdict(int)
        dynamic_outputs: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        successful_dynamic_mappings: Dict[str, Dict[str, List[str]]] = {}

        for log in logs:
            if not log.is_dagster_event:
                continue
            event = log.get_dagster_event()

            if event.is_successful_output and event.step_output_data.mapping_key:
                dynamic_outputs[event.step_key][event.step_output_data.output_name].append(
                    event.step_output_data.mapping_key
                )

            if event.is_step_up_for_retry:
                # tally up retry attempt
                previous_retry_attempts[event.step_key] += 1

                # clear any existing tracked mapping keys
                if event.step_key in dynamic_outputs:
                    for mapping_list in dynamic_outputs[event.step_key].values():
                        mapping_list.clear()

            if event.is_step_success and event.step_key in dynamic_outputs:
                successful_dynamic_mappings[event.step_key] = dict(dynamic_outputs[event.step_key])

        return KnownExecutionState(
            dict(previous_retry_attempts),
            successful_dynamic_mappings,
        )
