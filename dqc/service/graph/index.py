# import igraph as ig
# from igraph import Graph
#
# from dqc.common.simpleflake import get_next_id
# from dqc.model.graph.graph_node import GraphNode
# from dqc.model.graph.node_type import TOPIC, FACTOR
# from dqc.model.graph.relationship import Relationship
#
#
# class GraphBuilder(object):
#
#     def __init__(self):
#         self.g = Graph()
#
#     def clear_graph(self):
#         self.g.clear()
#         return self
#
#     def print_graph(self):
#         print(self.g)
#
#     def get_graph(self):
#         return self.g
#
#     def save_graph_ml(self, path):
#         self.g.write_graphml(path)
#
#     def find(self, node_type: str, node_name: str):
#         return self.g.vs.find(nodeName=node_name, type=node_type)
#
#     def build_topics_and_factors(self, topic_list):
#         node_list = []
#         relationship_list = []
#         for topic in topic_list:
#             topic_node = GraphNode(
#                 nodeId=get_next_id(),
#                 name=topic["name"],
#                 nodeType=TOPIC,
#                 nodeRefId=topic["topicId"],
#                 topicType=topic["type"],
#                 topicKind=topic["kind"]
#             )
#
#             node_list.append(topic_node)
#             for factor in topic["factors"]:
#                 factor_node = GraphNode(
#                     nodeId=get_next_id(),
#                     name=factor["name"],
#                     nodeType=FACTOR,
#                     nodeRefId=factor["factorId"],
#                     factorType=factor["type"]
#                 )
#                 node_list.append(factor_node)
#                 relationship = Relationship(
#                     relationshipId=get_next_id(),
#                     fromId=factor_node.nodeRefId,
#                     toId=topic_node.nodeRefId
#                 )
#                 relationship_list.append(relationship)
#
#         for node in node_list:
#             self.g.add_vertex(name=node.nodeRefId, type=node.nodeType, refId=node.nodeRefId, nodeName=node.name)
#
#         for relationship in relationship_list:
#             self.g.add_edge(source=relationship.fromId, target=relationship.toId)
#
#         return self
#
#     def knn(self):
#         return self.g.knn()
#
#     def save_pdf(self, path, sub_g=None):
#         layout = sub_g.layout("auto")
#         ig.plot(sub_g or self.g, layout=layout, target=path, vertex_label=sub_g.vs["nodeName"])
#
#     def save_to_file(self, path):
#         self.g.write_graphml(path)
#
#     def load_from_file(self, path):
#         self.g = Graph.Read_GraphML(path)
#
#     def build_pipelines(self, pipeline_list):
#         for pipeline_node in pipeline_list:
#
#             self.g.add_vertex(name=pipeline_node["pipelineId"], type="pipeline", refId=pipeline_node["pipelineId"],
#                               nodeName=pipeline_node["name"])
#             if "on" in pipeline_node and pipeline_node["on"] is not None and pipeline_node["on"]["filters"]:
#                 pass  ## TODO on
#
#             if "stages" in pipeline_node:
#                 for stage in pipeline_node["stages"]:
#                     self.g.add_vertex(name=stage["stageId"], type="stage", refId=stage["stageId"],
#                                       nodeName=stage["name"])
#
#                     ## process on
#                     self.g.add_edge(source=pipeline_node["pipelineId"], target=stage["stageId"])
#
#                     if "units" in stage and stage["units"]:
#                         for unit in stage["units"]:
#                             self.g.add_vertex(name=unit["unitId"], type="unit", refId=unit["unitId"],
#                                               nodeName=unit["name"])
#                             self.g.add_edge(source=stage["stageId"], target=unit["unitId"])
#
#                             ## process on
#                             if "do" in unit and unit["do"]:
#                                 for action in unit["do"]:
#                                     unit_name = unit["name"] or "unit"
#                                     self.g.add_vertex(name=action["actionId"], type="action", refId=action["actionId"],
#                                                       nodeName=unit_name + "-" + action["type"])
#                                     self.g.add_edge(source=unit["unitId"], target=action["actionId"])
#                                     source_topic_id = action["topicId"]
#                                     if action["type"] == "insert-or-merge-row" or action["type"] == "merge-row" or \
#                                             action["type"] == "insert-row":
#                                         if "mapping" in action and action["mapping"]:
#                                             for mapping in action["mapping"]:
#                                                 if "source" in mapping and mapping["source"] is not None:
#                                                     source = mapping["source"]
#                                                     if source["kind"] == "topic":
#                                                         self.g.add_edge(source=action["actionId"],
#                                                                         target=source["factorId"], relType="mapping")
#                                                     elif source["kind"] == "computed":
#                                                         pass
#                                                 self.g.add_edge(source=action["actionId"], target=mapping["factorId"],
#                                                                 relType="mapping")
#
#                                     elif action["type"] == "read-factor":
#                                         pass
#                                     else:
#                                         pass
