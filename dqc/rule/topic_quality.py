def load_default_rules():
    expect_rules = []
    # expect_rule = ExpectRuleTopic()
    # expect_rule.expectRuleId = "123"
    # expect_rule_column = ExpectRuleColumn()
    # expect_rule_column.expectColumnRule = "expect_table_row_count_to_equal"
    # expect_rule_column.ruleCondition = [200]
    # expect_rule.expectColumnRules.append(expect_rule_column)
    # expect_rules.append(expect_rule)
    return expect_rules


def find_match_rule(topic_name, expect_rules):
    return expect_rules[0]


def run_expect_rule(df_ge, expect_rule):
    results = []
    for rule_column in expect_rule.expectColumnRules:
        func = getattr(df_ge, rule_column.expectColumnRule)
        results.append(func(*rule_column.ruleCondition))

    return results


# def run_topic_quality_report(topic_name, expect_rules, from_date, to_date):
#     expect_rules = load_default_rules()
#     data_frame = query_topic_data_by_datetime(topic_name, from_date, to_date)
#     df_ge: Dataset = ge.dataset.PandasDataset(data_frame)
#     expect_rule = find_match_rule(topic_name, expect_rules)
#     results = run_expect_rule(df_ge, expect_rule)
#     print(results)


def run_topic_list_quality_report(list, expectation_suite_id):
    pass
