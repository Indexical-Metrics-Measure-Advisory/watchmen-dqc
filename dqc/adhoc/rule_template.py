import datetime
from typing import Optional

from model.model.topic.topic import Topic
from watchmen_boot.storage.model.data_source import DataSource

from dqc.adhoc.constants import RuleCode
from dqc.adhoc.rule.factor_and_another import FactorAndAnother
from dqc.adhoc.rule.factor_avg_not_in_range import FactorAvgNotInRange
from dqc.adhoc.rule.factor_common_value_not_in_range import FactorCommonValueNotInRange
from dqc.adhoc.rule.factor_common_value_over_coverage import FactorCommonValueOverCoverage
from dqc.adhoc.rule.factor_empty_over_coverage import FactorEmptyOverCoverage
from dqc.adhoc.rule.factor_is_blank import FactorIsBlank
from dqc.adhoc.rule.factor_is_empty import FactorIsEmpty
from dqc.adhoc.rule.factor_match_regexp import FactorMatchRegexp
from dqc.adhoc.rule.factor_max_not_in_range import FactorMaxNotInRange
from dqc.adhoc.rule.factor_median_not_in_range import FactorMedianNotInRange
from dqc.adhoc.rule.factor_min_not_in_range import FactorMinNotInRange
from dqc.adhoc.rule.factor_mismatch_date_type import FactorMismatchDateType
from dqc.adhoc.rule.factor_mismatch_enum import FactorMismatchEnum
from dqc.adhoc.rule.factor_mismatch_regexp import FactorMismatchRegexp
from dqc.adhoc.rule.factor_mismatch_type import FactorMismatchType
from dqc.adhoc.rule.factor_not_in_range import FactorNotInRange
from dqc.adhoc.rule.factor_quantile_not_in_range import FactorQuantileNotInRange
from dqc.adhoc.rule.factor_stdev_not_in_range import FactorStdevNotInRange
from dqc.adhoc.rule.factor_string_length_mismatch import FactorStringLengthMismatch
from dqc.adhoc.rule.factor_string_length_not_in_range import FactorStringLengthNotInRange
from dqc.adhoc.rule.factor_use_cast import FactorUseCast
from dqc.adhoc.rule.rows_count_mismatch_and_another import RowsCountMismatchAndAnother
from dqc.adhoc.rule.rows_no_change import RowsNoChange
from dqc.adhoc.rule.rows_not_exists import RowsNotExists
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleResult


class RuleTemplate:

    def __init__(self,
                 topic: Topic,
                 datasource: DataSource,
                 rule: MonitorRule,
                 process_date: Optional[datetime.datetime] = None,
                 another_topic: Optional[Topic] = None):
        self.topic = topic
        self.datasource = datasource
        self.rule = rule
        self.process_date = process_date
        self.another_topic = another_topic

    def execute_rule(self) -> RuleResult:
        if self.rule.code == RuleCode.FACTOR_COMMON_VALUE_OVER_COVERAGE:
            return self.rule_result(self.factor_common_value_over_coverage())
        elif self.rule.code == RuleCode.FACTOR_COMMON_VALUE_NOT_IN_RANGE:
            return self.rule_result(self.factor_common_value_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_MISMATCH_TYPE:
            return self.rule_result(self.factor_mismatch_type())
        elif self.rule.code == RuleCode.FACTOR_MISMATCH_ENUM:
            return self.rule_result(self.factor_mismatch_enum())
        elif self.rule.code == RuleCode.FACTOR_MISMATCH_DATE_TYPE:
            return self.rule_result(self.factor_mismatch_date_type())
        elif self.rule.code == RuleCode.ROWS_NOT_EXISTS:
            return self.rule_result(self.rows_not_exists())
        elif self.rule.code == RuleCode.ROWS_NO_CHANGE:
            return self.rule_result(self.rows_no_change())
        elif self.rule.code == RuleCode.ROWS_COUNT_MISMATCH_AND_ANOTHER:
            return self.rule_result(self.rows_count_mismatch_and_another())
        elif self.rule.code == RuleCode.FACTOR_IS_EMPTY:
            return self.rule_result(self.factor_is_empty())
        elif self.rule.code == RuleCode.FACTOR_USE_CAST:
            return self.rule_result(self.factor_use_cast())
        elif self.rule.code == RuleCode.FACTOR_EMPTY_OVER_COVERAGE:
            return self.rule_result(self.factor_empty_over_coverage())
        elif self.rule.code == RuleCode.FACTOR_IS_BLANK:
            return self.rule_result(self.factor_is_blank())
        elif self.rule.code == RuleCode.FACTOR_STRING_LENGTH_MISMATCH:
            return self.rule_result(self.factor_string_length_mismatch())
        elif self.rule.code == RuleCode.FACTOR_STRING_LENGTH_NOT_IN_RANGE:
            return self.rule_result(self.factor_string_length_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_MATCH_REGEXP:
            return self.rule_result(self.factor_match_regexp())
        elif self.rule.code == RuleCode.FACTOR_MISMATCH_REGEXP:
            return self.rule_result(self.factor_mismatch_regexp())
        elif self.rule.code == RuleCode.FACTOR_AND_ANOTHER:
            return self.rule_result(self.factor_and_another())
        elif self.rule.code == RuleCode.FACTOR_NOT_IN_RANGE:
            return self.rule_result(self.factor_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_AVG_NOT_IN_RANGE:
            return self.rule_result(self.factor_avg_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_MIN_NOT_IN_RANGE:
            return self.rule_result(self.factor_min_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_MAX_NOT_IN_RANGE:
            return self.rule_result(self.factor_max_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_MEDIAN_NOT_IN_RANGE:
            return self.rule_result(self.factor_median_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_QUANTILE_NOT_IN_RANGE:
            return self.rule_result(self.factor_quantile_not_in_range())
        elif self.rule.code == RuleCode.FACTOR_STDEV_NOT_IN_RANGE:
            return self.rule_result(self.factor_stdev_not_in_range())
        else:
            return self.rule_result(False)

    def factor_common_value_over_coverage(self) -> bool:
        rule_entity = FactorCommonValueOverCoverage(self.get_schema(),
                                                    self.topic,
                                                    self.rule,
                                                    self.get_factor(self.topic, self.rule.factorId),
                                                    self.process_date)
        return rule_entity.execute()

    def factor_common_value_not_in_range(self) -> bool:
        rule_entity = FactorCommonValueNotInRange(self.get_schema(),
                                                  self.topic,
                                                  self.rule,
                                                  self.get_factor(self.topic, self.rule.factorId),
                                                  self.process_date)
        return rule_entity.execute()

    def factor_mismatch_type(self) -> bool:
        rule_entity = FactorMismatchType(self.get_schema(),
                                         self.topic,
                                         self.rule,
                                         self.get_factor(self.topic, self.rule.factorId),
                                         self.process_date)
        return rule_entity.execute()

    def factor_mismatch_enum(self) -> bool:
        rule_entity = FactorMismatchEnum(self.get_schema(),
                                         self.topic,
                                         self.rule,
                                         self.get_factor(self.topic, self.rule.factorId),
                                         self.process_date)
        return rule_entity.execute()

    def factor_mismatch_date_type(self) -> bool:
        rule_entity = FactorMismatchDateType(self.get_schema(),
                                             self.topic,
                                             self.rule,
                                             self.get_factor(self.topic, self.rule.factorId),
                                             self.process_date)
        return rule_entity.execute()

    def rows_not_exists(self) -> bool:
        rule_entity = RowsNotExists(self.get_schema(),
                                    self.topic,
                                    self.rule,
                                    self.get_factor(self.topic, self.rule.factorId),
                                    self.process_date)
        return rule_entity.execute()

    def rows_no_change(self) -> bool:
        rule_entity = RowsNoChange(self.get_schema(),
                                   self.topic,
                                   self.rule,
                                   self.get_factor(self.topic, self.rule.factorId),
                                   self.process_date)
        return rule_entity.execute()

    def rows_count_mismatch_and_another(self) -> bool:
        rule_entity = RowsCountMismatchAndAnother(self.get_schema(),
                                                  self.topic,
                                                  self.rule,
                                                  self.another_topic,
                                                  self.get_factor(self.topic, self.rule.factorId),
                                                  self.process_date)
        return rule_entity.execute()

    def factor_is_empty(self) -> bool:
        rule_entity = FactorIsEmpty(self.get_schema(),
                                    self.topic,
                                    self.rule,
                                    self.get_factor(self.topic, self.rule.factorId),
                                    self.process_date)
        return rule_entity.execute()

    def factor_use_cast(self) -> bool:
        rule_entity = FactorUseCast(self.get_schema(),
                                    self.topic,
                                    self.rule,
                                    self.get_factor(self.topic, self.rule.factorId),
                                    self.process_date)
        return rule_entity.execute()

    def factor_empty_over_coverage(self) -> bool:
        rule_entity = FactorEmptyOverCoverage(self.get_schema(),
                                              self.topic,
                                              self.rule,
                                              self.get_factor(self.topic, self.rule.factorId),
                                              self.process_date)
        return rule_entity.execute()

    def factor_is_blank(self) -> bool:
        rule_entity = FactorIsBlank(self.get_schema(),
                                    self.topic,
                                    self.rule,
                                    self.get_factor(self.topic, self.rule.factorId),
                                    self.process_date)
        return rule_entity.execute()

    def factor_string_length_mismatch(self) -> bool:
        rule_entity = FactorStringLengthMismatch(self.get_schema(),
                                                 self.topic,
                                                 self.rule,
                                                 self.get_factor(self.topic, self.rule.factorId),
                                                 self.process_date)
        return rule_entity.execute()

    def factor_string_length_not_in_range(self) -> bool:
        rule_entity = FactorStringLengthNotInRange(self.get_schema(),
                                                   self.topic,
                                                   self.rule,
                                                   self.get_factor(self.topic, self.rule.factorId),
                                                   self.process_date)
        return rule_entity.execute()

    def factor_match_regexp(self) -> bool:
        rule_entity = FactorMatchRegexp(self.get_schema(),
                                        self.topic,
                                        self.rule,
                                        self.get_factor(self.topic, self.rule.factorId),
                                        self.process_date)
        return rule_entity.execute()

    def factor_mismatch_regexp(self) -> bool:
        rule_entity = FactorMismatchRegexp(self.get_schema(),
                                           self.topic,
                                           self.rule,
                                           self.get_factor(self.topic, self.rule.factorId),
                                           self.process_date)
        return rule_entity.execute()

    def factor_and_another(self) -> bool:
        rule_entity = FactorAndAnother(self.get_schema(),
                                       self.topic,
                                       self.rule,
                                       self.get_factor(self.topic, self.rule.factorId),
                                       self.process_date)
        return rule_entity.execute()

    def factor_not_in_range(self) -> bool:
        rule_entity = FactorNotInRange(self.get_schema(),
                                       self.topic,
                                       self.rule,
                                       self.get_factor(self.topic, self.rule.factorId),
                                       self.process_date)
        return rule_entity.execute()

    def factor_avg_not_in_range(self) -> bool:
        rule_entity = FactorAvgNotInRange(self.get_schema(),
                                          self.topic,
                                          self.rule,
                                          self.get_factor(self.topic, self.rule.factorId),
                                          self.process_date)
        return rule_entity.execute()

    def factor_min_not_in_range(self) -> bool:
        rule_entity = FactorMinNotInRange(self.get_schema(),
                                          self.topic,
                                          self.rule,
                                          self.get_factor(self.topic, self.rule.factorId),
                                          self.process_date)
        return rule_entity.execute()

    def factor_max_not_in_range(self) -> bool:
        rule_entity = FactorMaxNotInRange(self.get_schema(),
                                          self.topic,
                                          self.rule,
                                          self.get_factor(self.topic, self.rule.factorId),
                                          self.process_date)
        return rule_entity.execute()

    def factor_median_not_in_range(self) -> bool:
        rule_entity = FactorMedianNotInRange(self.get_schema(),
                                             self.topic,
                                             self.rule,
                                             self.get_factor(self.topic, self.rule.factorId),
                                             self.process_date)
        return rule_entity.execute()

    def factor_quantile_not_in_range(self) -> bool:
        rule_entity = FactorQuantileNotInRange(self.get_schema(),
                                               self.topic,
                                               self.rule,
                                               self.get_factor(self.topic, self.rule.factorId),
                                               self.process_date)
        return rule_entity.execute()

    def factor_stdev_not_in_range(self) -> bool:
        rule_entity = FactorStdevNotInRange(self.get_schema(),
                                            self.topic,
                                            self.rule,
                                            self.get_factor(self.topic, self.rule.factorId),
                                            self.process_date)
        return rule_entity.execute()

    def get_schema(self) -> str:
        catalog_name = self.datasource.dataSourceCode
        schema_name = self.datasource.name
        return "{parent}.{schema}".format(
            parent=catalog_name,
            schema=schema_name
        )

    @staticmethod
    def get_factor(topic: Topic, factor_id: str):
        for factor in topic.factors:
            if factor.factorId == factor_id:
                return factor

    def rule_result(self, result: bool) -> RuleResult:
        factor = self.get_factor(self.topic, self.rule.factorId)
        rr_ = RuleResult(topicId=self.topic.topicId,
                         topicName=self.topic.name,
                         factorId=factor.factorId if factor else None,
                         factorName=factor.name if factor else None,
                         ruleCode=self.rule.code,
                         result=result,
                         severity=self.rule.severity,
                         tenant_id_=self.rule.tenantId,
                         params=self.rule.params if self.rule.params else []
                         )
        return rr_
