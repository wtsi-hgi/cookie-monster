from cookiemonster.processor.models import RuleApplicationLog
from hgijson import JsonPropertyMapping

from hgijson.json.builders import MappingJSONEncoderClassBuilder, MappingJSONDecoderClassBuilder

_rule_appplication_json_mappings = [
    JsonPropertyMapping("rule_id", "rule_id", "rule_id"),
    JsonPropertyMapping("terminated_processing", "terminated_processing", "terminated_processing")
]
RuleApplicationLogJSONEncoder = MappingJSONEncoderClassBuilder(RuleApplicationLog, _rule_appplication_json_mappings).build()
RuleApplicationLogJSONDecoder = MappingJSONDecoderClassBuilder(RuleApplicationLog, _rule_appplication_json_mappings).build()
