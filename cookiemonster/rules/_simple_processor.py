from typing import List, Callable, Set, Optional

from cookiemonster.common.models import Notification, CookieProcessState
from cookiemonster.rules._models import Rule
from cookiemonster.rules.processor import Processor, RuleProcessingQueue


class SimpleProcessor(Processor):
    """
    Simple processor for a single file update.
    """
    def process(self, job: CookieProcessState, rules: Set[Rule],
                on_complete: Callable[[bool, Optional[Set[Notification]]], None]):
        rule_processing_queue = RuleProcessingQueue(rules)

        notifications = set()
        terminate = False

        while not terminate and rule_processing_queue.has_unprocessed_rules():
            rule = rule_processing_queue.get_next_unprocessed()
            if rule.matching_criteria(job):
                rule_action = rule.action_generator(job)
                notifications = notifications.union(rule_action.notifications)
                terminate = rule_action.terminate_processing
            rule_processing_queue.mark_as_processed(rule)

        at_least_one_rule_matched = terminate or len(notifications) > 0
        on_complete(at_least_one_rule_matched, notifications)
