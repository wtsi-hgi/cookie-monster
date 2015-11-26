from typing import List, Callable, Any

from cookiemonster.common.models import Notification
from cookiemonster.rules._collections import RuleCollection
from cookiemonster.rules.processor import Processor, RuleProcessingQueue


class SimpleProcessor(Processor):
    """
    Simple processor for a single file update.
    """
    def process(self, work: Any, rules: RuleCollection, on_complete: Callable[[List[Notification]], None]):
        rule_processing_queue = RuleProcessingQueue(rules)

        notifications = []
        terminate = False

        while not terminate and rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_unprocessed()
            if rule.matching_criteria(work):
                rule_action = rule.action_generator()
                notifications += rule_action.notifications
                terminate = rule_action.terminate_processing
            rule_processing_queue.mark_as_processed(rule)

        on_complete(notifications)
