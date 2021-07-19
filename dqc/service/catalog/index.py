# import arrow
#
# from dqc.integration.admin_sdk import fetch_all_topics, fetch_all_pipelines
# from dqc.model.analysis.topic_summary import TopicSummary
# from dqc.model.catalog.data_source import DataSource
# from dqc.model.catalog.factor import Factor
# from dqc.model.catalog.topic import Topic
# from dqc.service.graph.index import GraphBuilder
#
#
# def load_watchmen_instance_list():
#     return []
#
#
# def __process_factors_list(factors, topic_data: Topic):
#     for factor in factors:
#         factor_data = Factor()
#         factor_data.factorId = factor["factorId"]
#         factor_data.name = factor["name"]
#         factor_data.factorType = factor["type"]
#         topic_data.factor_dict[factor["name"]] = factor_data
#
#
# def __process_topic_list(topic_list):
#     topic_data_list = []
#     for topic in topic_list:
#         topic_data = Topic()
#         topic_data.name = topic["name"]
#         topic_data.topicType = topic["type"]
#         topic_data.topicKind = topic["kind"]
#         topic_data.lastUpdated = arrow.get(topic["lastModified"]).datetime.replace(tzinfo=None)
#         topic_data_list.append(topic_data)
#         topic_summary = TopicSummary()
#         topic_summary.topicId = topic["topicId"]
#         topic_summary.factorCount = len(topic["factors"])
#         topic_data.topicSummary = topic_summary
#         __process_factors_list(topic["factors"], topic_data)
#
#     return topic_data_list
#
#
# def __build_data_source(site):
#     data_source = DataSource()
#     data_source.name = site.name
#     return data_source
#
#
# def fetch_from_watchmen_instance(site):
#     builder = GraphBuilder()
#     # watchmen_request_list = load_watchmen_instance_list()
#
#     # for watchmen_request in watchmen_request_list:
#     data_source = __build_data_source(site)
#     ## fetch topic data
#     topic_list = fetch_all_topics(site)
#     builder.build_topics_and_factors(topic_list)
#
#     # topic_dict = __process_topic_list(topic_list)
#
#     # build_factor_topic graph
#     # data_source.topic_dict = topic_dict
#
#     pipeline_list = fetch_all_pipelines(site)
#     builder.build_pipelines(pipeline_list)
#     # builder.print_graph()
#     # print(builder.get_graph().ecount())
#     path = "./graph_ml/" + site.name + ".graphml"
#     builder.save_graph_ml(path)
#     data_source.mlPath = path
#     return data_source
#
#     # print(builder.find(node_type="pipeline",node_name="Raw_policy_mapping"))
#
#     # g_sub = builder.get_graph().subgraph([0, 1,2,3,4])
#     #
#     # builder.save_pdf("./test.pdf",g_sub)
#
#     # print(builder.get_graph().es.find(relType="mapping").neighbors())
#     #
#     # print(builder.get_graph().vcount())
#     # vips =  len(builder.get_graph().es)
#
#     # #
#     # print(vips)
#
#     # print(builder.knn())
#
#     # ## fetch pipeline data
#     # ## load all the pipeline
#     #
#     #
#     #
#     #
#     # build_factor_pipeline_node_and_relationship(pipeline_list,node_list)
#
#     ## build factor graph
#
#     # print(pipeline_list)
#
#     ## build topic and factor graph with pipeline
#
#     ## fetch space
#     ## fetch report
#     ## fetch dataset
#     ## fetch connect space
#     ## fetch access log pipeline
#     ## fetch access log for query
#
#     ## build graph
#     ## save data source
#
#     return data_source
