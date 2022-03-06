import datetime
from typing import List, Optional
import requests
from model.model.common.user import User

from dqc.adhoc.constants import RuleCode
from dqc.adhoc.rule_template import RuleTemplate
from dqc.common.constants import MONITOR_RULES
from dqc.config.config import settings
from dqc.database.find_storage_template import find_storage_template
from model.model.topic.topic import Topic
from watchmen_boot.storage.model.data_source import DataSource

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleResult
from dqc.sdk.common.common_sdk import InstanceRequest, import_instance
from dqc.sdk.utils.index import build_headers


class RuleService:

    def __init__(self, topic_name: str, current_user: User, process_date: Optional[datetime.datetime] = None):
        self.topic_name = topic_name
        self.current_user = current_user
        self.process_date = process_date
        self.storage_engine = find_storage_template()

    def rules_execution(self):
        topic = self.get_topic()
        datasource = self.get_datasource(topic)
        rules = self.get_rules(topic.topicId)
        for item in rules:
            if item.factorId:
                self.single_rule_execution(topic, datasource, item)
            else:
                if self.is_pure_topic_rule(item):
                    self.single_rule_execution(topic, datasource, item)
                else:
                    for factor in topic.factors:
                        item.factorId = factor.factorId
                        self.single_rule_execution(topic, datasource, item)

    def single_rule_execution(self, topic: Topic, datasource: DataSource, rule: MonitorRule):
        need_another_topic = self.need_another_topic(rule)
        if need_another_topic:
            rt_ = RuleTemplate(topic, datasource, rule, self.process_date, need_another_topic)
        else:
            rt_ = RuleTemplate(topic, datasource, rule, self.process_date)
        result = rt_.execute_rule()
        self.save_rule_result(result)

    def get_topic(self) -> Topic:
        headers = build_headers(self.current_user)
        response = requests.get(settings.WATCHMEN_HOST + "topic/name/tenant",
                                params={"name": self.topic_name, "tenant_id": self.current_user.tenantId},
                                headers=headers)
        result = Topic.parse_obj(response.json())
        return result

    def get_another_topic(self, another_topic_id) -> Topic:
        headers = build_headers(self.current_user)
        response = requests.get(settings.WATCHMEN_HOST + "topic",
                                params={"topic_id": another_topic_id, "tenant_id": self.current_user.tenantId},
                                headers=headers)
        result = Topic.parse_obj(response.json())
        return result

    def get_datasource(self, topic: Topic) -> DataSource:
        headers = build_headers(self.current_user)
        response = requests.get(settings.WATCHMEN_HOST + "datasource/id",
                                params={"datasource_id": topic.dataSourceId},
                                headers=headers)
        result = DataSource.parse_obj(response.json())
        return result

    def get_rules(self, topic_id: str) -> List[MonitorRule]:
        results = self.storage_engine.find_({"topicid": topic_id, "enabled": 1}, MonitorRule, MONITOR_RULES)
        return results

    def save_rule_result(self, result: RuleResult):
        import_instance(InstanceRequest(code="system_rule_result", data=result,
                                        tenantId=self.current_user.tenantId), self.current_user)

    def need_another_topic(self, rule: MonitorRule) -> Optional[Topic]:
        if rule.code == RuleCode.ROWS_COUNT_MISMATCH_AND_ANOTHER and rule.params.topicId:
            return self.get_another_topic(rule.params.topicId)

    @staticmethod
    def is_pure_topic_rule(rule) -> bool:
        return rule.code in [
            RuleCode.FACTOR_MISMATCH_ENUM,
            RuleCode.ROWS_NOT_EXISTS,
            RuleCode.ROWS_NO_CHANGE,
            RuleCode.ROWS_COUNT_MISMATCH_AND_ANOTHER
        ]