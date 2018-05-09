# /usr/bin/env python3
import json
import os
import git
import subprocess

from ground.common.model.core.node import Node
from ground.common.model.core.node_version import NodeVersion
from ground.common.model.core.edge import Edge
from ground.common.model.core.edge_version import EdgeVersion
from ground.common.model.core.graph import Graph
from ground.common.model.core.graph_version import GraphVersion
from ground.common.model.core.structure import Structure
from ground.common.model.core.structure_version import StructureVersion
from ground.common.model.usage.lineage_edge import LineageEdge
from ground.common.model.usage.lineage_edge_version import LineageEdgeVersion
from ground.common.model.usage.lineage_graph import LineageGraph
from ground.common.model.usage.lineage_graph_version import LineageGraphVersion
from ground.common.model.version.tag import Tag

class GitImplementation():
    names = {'edge': Edge.__name__, 'edgeVersion': EdgeVersion.__name__, 'node': Node.__name__,
             'nodeVersion': NodeVersion.__name__, 'graph': Graph.__name__, 'graphVersion': GraphVersion.__name__,
             'structure': Structure.__name__, 'structureVersion': StructureVersion.__name__,
             'lineageEdge': LineageEdge.__name__, 'lineageEdgeVersion': LineageEdgeVersion.__name__,
             'lineageGraph': LineageGraph.__name__, 'lineageGraphVersion': LineageGraphVersion.__name__}
    def __init__(self):
        self.initialized = False
        self.path = "ground_git_dir/"

    def _get_rich_version_json(self, item_type, reference, reference_parameters,
                               tags, structure_version_id, parent_ids):
        item_id = self._gen_id()
        body = {"id": item_id, "class": item_type}
        if reference:
            body["reference"] = reference

        if reference_parameters:
            body["referenceParameters"] = reference_parameters

        if tags:
            body["tags"] = tags

        if structure_version_id > 0:
            body["structureVersionId"] = structure_version_id

        if parent_ids:
            body["parentIds"] = parent_ids

        return body

    def _deconstruct_rich_version_json(self, body):
        bodyRet = dict(body)
        if bodyRet["tags"]:
            bodyTags = {}
            for key, value in list((bodyRet["tags"]).items()):
                if isinstance(value, Tag):
                    bodyTags[key] = {'id': value.get_id(), 'key': value.get_key(), 'value': value.get_value()}
            bodyRet["tags"] = bodyTags

        return bodyRet

    def _create_item(self, item_type, source_key, name, tags):
        item_id = self._gen_id()
        body = {"sourceKey": source_key, "name": name, "class": item_type, "id": item_id}

        if tags:
            body["tags"] = tags

        return body

    def _deconstruct_item(self, item):
        body = {"id": item.get_id(), "class": type(item).__name__, "name": item.get_name(),
                "sourceKey": item.get_source_key()}

        if item.get_tags():
            bodyTags = {}
            for key, value in list((item.get_tags()).items()):
                if isinstance(value, Tag):
                    bodyTags[key] = {'id': value.get_id(), 'key': value.get_key(), 'value': value.get_value()}
            body["tags"] = bodyTags

        return body

    def _gen_id(self):
        with open(self.path + 'ids.json', 'r') as f:
            ids = json.loads(f.read())
        newid = (ids['running_id'])['latest_id'] + 1
        ids['running_id']['latest_id'] = newid
        self._write_files('ids', ids)
        return newid

    def _write_files(self, id, body):
        with open(self.path + str(id) + '.json', 'w') as f:
            f.write(json.dumps(body))
        if(id != 'ids' and ("Version" not in body["class"])):
            with open(self.path + 'ids.json', 'r') as f_two:
                ids = json.loads(f_two.read())
            sourceKey = body["sourceKey"]
            bodyClass = body["class"]
            ids["sourceId"][bodyClass][sourceKey] = id
            with open(self.path + 'ids.json', 'w') as f_three:
                f_three.write(json.dumps(ids))
        elif(id != 'ids' and ("Version" in body["class"])):
            with open(self.path + 'ids.json', 'r') as f_four:
                ids = json.loads(f_four.read())
            bodyClass = body["class"]
            specId = ""
            if(bodyClass == self.names['nodeVersion']):
                specId = body['nodeId']
            elif (bodyClass == self.names['edgeVersion']):
                specId = body['edgeId']
            elif (bodyClass == self.names['graphVersion']):
                specId = body['graphId']
            elif (bodyClass == self.names['structureVersion']):
                specId = body['structureId']
            elif (bodyClass == self.names['lineageEdgeVersion']):
                specId = body['lineageEdgeId']
            elif (bodyClass == self.names['lineageGraphVersion']):
                specId = body['lineageGraphId']

            with open(self.path + str(specId) + '.json', 'r') as f_five:
                base = json.loads(f_five.read())
            sourceKey = base["sourceKey"]

            if(bodyClass == self.names['nodeVersion']):
                ids["adjacentLineageEdgeVersions"][id] = []
            if (bodyClass == self.names['lineageEdgeVersion']):
                toRich = body["toRichVersionId"]
                fromRich = body["fromRichVersionId"]
                if(str(toRich) in ids["adjacentLineageEdgeVersions"]):
                    ids["adjacentLineageEdgeVersions"][str(toRich)].append(id)
                if(str(fromRich) in ids["adjacentLineageEdgeVersions"]):
                    ids["adjacentLineageEdgeVersions"][str(fromRich)].append(id)
            
            if(sourceKey not in ids["latestVersion"][bodyClass]):
                ids["latestVersion"][bodyClass][sourceKey] = [id]
            else:
                ids["latestVersion"][bodyClass][sourceKey].append(id)
                if("parentIds" in body):
                    parentIds = body["parentIds"]
                    latestIdsSet = set(ids["latestVersion"][bodyClass][sourceKey])
                    for parentId in parentIds:
                        if(parentId in latestIdsSet):
                            ids["latestVersion"][bodyClass][sourceKey].remove(parentId)

            if(sourceKey not in ids["history"][bodyClass]):
                ids["history"][bodyClass][sourceKey] = {}
            sourceHistory = ids["history"][bodyClass][sourceKey]
            if (not sourceHistory):
                ids["history"][bodyClass][sourceKey][str(specId)] = id
            if("parentIds"in body):
                parentIds = body["parentIds"]
                for parentId in parentIds:
                    ids["history"][bodyClass][sourceKey][str(parentId)] = id

            with open(self.path + 'ids.json', 'w') as f_six:
                f_six.write(json.dumps(ids))

    def _read_files(self, sourceKey, className):
        with open(self.path + 'ids.json', 'r') as idFile:
            ids = json.loads(idFile.read())
        fileId = ids["sourceId"][className][sourceKey]
        with open(self.path + str(fileId) + '.json', 'r') as f:
            object = json.loads(f.read())
        return object

    def _read_version(self, id, className):
        with open(self.path + str(id) + '.json', 'r') as f:
            fileDict = json.loads(f.read())
        return fileDict

    def _read_latest_versions(self, sourceKey, className):
        with open(self.path + 'ids.json', 'r') as f:
            ids = json.loads(f.read())
        latestVersions = ids["latestVersion"][className][sourceKey]
        return [self._read_version(id, className) for id in latestVersions]

    def _read_history(self, sourceKey, className):
        with open(self.path + 'ids.json', 'r') as f:
            ids = json.loads(f.read())
        history = ids["history"][className][sourceKey]
        return history

    def _read_all_lineage_edge_versions(self, nodeVersionId):
        with open(self.path + 'ids.json', 'r') as f:
            ids = json.loads(f.read())
        adjacentVersions = ids["adjacentLineageEdgeVersions"][str(nodeVersionId)]
        return [self._read_version(id, self.names['lineageEdgeVersion']) for id in adjacentVersions]

    def _find_file(self, sourceKey, className):
        with open(self.path + 'ids.json', 'r') as f:
            ids = json.loads(f.read())
        if(sourceKey in ids["sourceId"][className]):
            return True
        return False

    def __run_proc__(self, bashCommand):
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        return str(output, 'UTF-8')

    def _check_init(self):
        if(not self.initialized):
            raise ValueError('Ground GitImplementation instance must call .init() to initialize git in directory')

    def init(self):
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        if not os.path.exists(self.path + 'ids.json'):
            with open(self.path + 'ids.json', 'w') as f:
                f.write(json.dumps({'running_id': {'latest_id': 0},
                                    'sourceId' : {self.names['node']: {}, self.names['edge']: {},
                                                  self.names['graph']: {}, self.names['structure']: {},
                                                  self.names['lineageEdge']: {}, self.names['lineageGraph']: {}},
                                    'adjacentLineageEdgeVersions': {},
                                    'latestVersion': {self.names['nodeVersion']: {}, self.names['edgeVersion']: {},
                                                       self.names['graphVersion']: {},
                                                       self.names['structureVersion']: {},
                                                       self.names['lineageEdgeVersion']: {},
                                                       self.names['lineageGraphVersion']: {}},
                                    'history': {self.names['nodeVersion']: {}, self.names['edgeVersion']: {},
                                                      self.names['graphVersion']: {},
                                                      self.names['structureVersion']: {},
                                                      self.names['lineageEdgeVersion']: {},
                                                      self.names['lineageGraphVersion']: {}}}))
        self.repo = git.Repo.init(self.path)
        if not os.path.exists(self.path + '.gitignore'):
            with open(self.path + '.gitignore', 'w') as f:
                f.write('ids.json')
            self.repo.index.add([os.getcwd() + '/' + self.path + '.gitignore'])
            self.repo.index.commit("Initialize Ground GitImplementation repository")
        self.initialized = True

    def _commit(self, id, className):
        totFile = os.getcwd() + '/' + self.path + str(id) + '.json'
        self.repo.index.add([totFile])
        self.repo.index.commit("id: " + str(id) + ", class: " + className)

        ### EDGES ###
    def createEdge(self, sourceKey, fromNodeId, toNodeId, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, self.names['edge']):
            body = self._create_item(self.names['edge'], sourceKey, name, tags)
            body["fromNodeId"] = fromNodeId
            body["toNodeId"] = toNodeId
            edge = Edge(body)
            edgeId = edge.get_id()
            write = self._deconstruct_item(edge)
            write["fromNodeId"] = edge.get_from_node_id()
            write["toNodeId"] = edge.get_to_node_id()
            self._write_files(edgeId, write)
            self._commit(edgeId, self.names['edge'])
        else:
            edge = self._read_files(sourceKey, self.names['edge'])
            edgeId = edge['id']

        return edgeId

    def createEdgeVersion(self, edgeId, toNodeVersionStartId, fromNodeVersionStartId, toNodeVersionEndId=None,
                          fromNodeVersionEndId=None, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(self.names['edgeVersion'], reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["edgeId"] = edgeId
        body["toNodeVersionStartId"] = toNodeVersionStartId
        body["fromNodeVersionStartId"] = fromNodeVersionStartId

        if toNodeVersionEndId > 0:
            body["toNodeVersionEndId"] = toNodeVersionEndId

        if fromNodeVersionEndId > 0:
            body["fromNodeVersionEndId"] = fromNodeVersionEndId

        edgeVersion = EdgeVersion(body)
        edgeVersionId = edgeVersion.get_id()

        #self.edgeVersions[edgeVersionId] = edgeVersion

        write = self._deconstruct_rich_version_json(body)
        self._write_files(edgeVersionId, write)
        self._commit(edgeVersionId, self.names['edgeVersion'])

        return edgeVersionId

    def getEdge(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, self.names['edge'])


    def getEdgeLatestVersions(self, sourceKey):
        self._check_init()
        latest = self._read_latest_versions(sourceKey, self.names['edgeVersion'])
        return latest

    def getEdgeHistory(self, sourceKey):
        self._check_init()
        history = self._read_history(sourceKey, self.names['edgeVersion'])
        return history

    def getEdgeVersion(self, edgeVersionId):
        self._check_init()
        return self._read_version(edgeVersionId, self.names['edgeVersion'])

    ### NODES ###
    def createNode(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, self.names['node']):
            body = self._create_item(self.names['node'], sourceKey, name, tags)
            node = Node(body)
            nodeId = node.get_item_id()
            write = self._deconstruct_item(node)
            self._write_files(nodeId, write)
            self._commit(nodeId, self.names['node'])
        else:
            node = self._read_files(sourceKey, self.names['node'])
            nodeId = node['id']

        return nodeId

    def createNodeVersion(self, nodeId, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(self.names['nodeVersion'], reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["nodeId"] = nodeId

        nodeVersion = NodeVersion(body)
        nodeVersionId = nodeVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(nodeVersionId, write)
        self._commit(nodeVersionId, self.names['nodeVersion'])

        return nodeVersionId


    def getNode(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, self.names['node'])

    def getNodeLatestVersions(self, sourceKey):
        self._check_init()
        latest = self._read_latest_versions(sourceKey, self.names['nodeVersion'])
        return latest

    def getNodeHistory(self, sourceKey):
        self._check_init()
        history = self._read_history(sourceKey, self.names['nodeVersion'])
        return history

    def getNodeVersion(self, nodeVersionId):
        self._check_init()
        return self._read_version(nodeVersionId, self.names['nodeVersion'])


    def getNodeVersionAdjacentLineage(self, nodeVersionId):
        self._check_init()
        adjacentLineage = self._read_all_lineage_edge_versions(nodeVersionId)
        return adjacentLineage

    ### GRAPHS ###
    def createGraph(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, self.names['graph']):
            body = self._create_item(self.names['graph'], sourceKey, name, tags)
            graph = Graph(body)
            graphId = graph.get_item_id()
            write = self._deconstruct_item(graph)
            self._write_files(graphId, write)
            self._commit(graphId, self.names['graph'])
        else:
            graph = self._read_files(sourceKey, self.names['graph'])
            graphId = graph['id']

        return graphId


    def createGraphVersion(self, graphId, edgeVersionIds, reference=None,
                           referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(self.names['graphVersion'], reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["graphId"] = graphId
        body["edgeVersionIds"] = edgeVersionIds

        graphVersion = GraphVersion(body)
        graphVersionId = graphVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(graphVersionId, write)
        self._commit(graphVersionId, self.names['graphVersion'])

        return graphVersionId

    def getGraph(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, self.names['graph'])

    def getGraphLatestVersions(self, sourceKey):
        self._check_init()
        latest = self._read_latest_versions(sourceKey, self.names['graphVersion'])
        return latest

    def getGraphHistory(self, sourceKey):
        self._check_init()
        history = self._read_history(sourceKey, self.names['graphVersion'])
        return history

    def getGraphVersion(self, graphVersionId):
        self._check_init()
        return self._read_version(graphVersionId, self.names['graphVersion'])

    ### STRUCTURES ###
    def createStructure(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, self.names['structure']):
            body = self._create_item(self.names['structure'], sourceKey, name, tags)
            structure = Structure(body)
            structureId = structure.get_item_id()
            write = self._deconstruct_item(structure)
            self._write_files(structureId, write)
            self._commit(structureId, self.names['structure'])
        else:
            structure = self._read_files(sourceKey, self.names['structure'])
            structureId = structure['id']

        return structureId


    def createStructureVersion(self, structureId, attributes, parentIds=None):
        self._check_init()
        body = {
            "id": self._gen_id(),
            "class":self.names['structureVersion'],
            "structureId": structureId,
            "attributes": attributes
        }

        if parentIds:
            body["parentIds"] = parentIds

        structureVersion = StructureVersion(body)
        structureVersionId = structureVersion.get_id()

        write = dict(body)
        self._write_files(structureVersionId, write)
        self._commit(structureVersionId, self.names['structureVersion'])

        return structureVersionId

    def getStructure(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, self.names['structure'])

    def getStructureLatestVersions(self, sourceKey):
        self._check_init()
        latest = self._read_latest_versions(sourceKey, self.names['structureVersion'])
        return latest

    def getStructureHistory(self, sourceKey):
        self._check_init()
        history = self._read_history(sourceKey, self.names['structureVersion'])
        return history

    def getStructureVersion(self, structureVersionId):
        self._check_init()
        return self._read_version(structureVersionId, self.names['structureVersion'])


    ### LINEAGE EDGES ###
    def createLineageEdge(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, self.names['lineageEdge']):
            body = self._create_item(self.names['lineageEdge'], sourceKey, name, tags)
            lineageEdge = LineageEdge(body)
            lineageEdgeId = lineageEdge.get_id()
            write = self._deconstruct_item(lineageEdge)
            self._write_files(lineageEdgeId, write)
            self._commit(lineageEdgeId, self.names['lineageEdge'])
        else:
            lineageEdge = self._read_files(sourceKey, self.names['lineageEdge'])
            lineageEdgeId = lineageEdge['id']

        return lineageEdgeId


    def createLineageEdgeVersion(self, lineageEdgeId, toRichVersionId, fromRichVersionId, reference=None,
                                 referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(self.names['lineageEdgeVersion'], reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["lineageEdgeId"] = lineageEdgeId
        body["toRichVersionId"] = toRichVersionId
        body["fromRichVersionId"] = fromRichVersionId

        lineageEdgeVersion = LineageEdgeVersion(body)
        lineageEdgeVersionId = lineageEdgeVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(lineageEdgeVersionId, write)
        self._commit(lineageEdgeVersionId, self.names['lineageEdgeVersion'])

        return lineageEdgeVersionId

    def getLineageEdge(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, self.names['lineageEdge'])

    def getLineageEdgeLatestVersions(self, sourceKey):
        self._check_init()
        latest = self._read_latest_versions(sourceKey, self.names['lineageEdgeVersion'])
        return latest

    def getLineageEdgeHistory(self, sourceKey):
        self._check_init()
        history = self._read_history(sourceKey, self.names['lineageEdgeVersion'])
        return history

    def getLineageEdgeVersion(self, lineageEdgeVersionId):
        self._check_init()
        return self._read_version(lineageEdgeVersionId, self.names['lineageEdgeVersion'])

    ### LINEAGE GRAPHS ###
    def createLineageGraph(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, self.names['lineageGraph']):
            body = self._create_item(self.names['lineageGraph'], sourceKey, name, tags)
            lineageGraph = LineageGraph(body)
            lineageGraphId = lineageGraph.get_id()
            write = self._deconstruct_item(lineageGraph)
            self._write_files(lineageGraphId, write)
            self._commit(lineageGraphId, self.names['lineageGraph'])
        else:
            lineageGraph = self._read_files(sourceKey, self.names['lineageGraph'])
            lineageGraphId = lineageGraph['id']

        return lineageGraphId


    def createLineageGraphVersion(self, lineageGraphId, lineageEdgeVersionIds, reference=None,
                                  referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(self.names['lineageGraphVersion'], reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["lineageGraphId"] = lineageGraphId
        body["lineageEdgeVersionIds"] = lineageEdgeVersionIds

        lineageGraphVersion = LineageGraphVersion(body)
        lineageGraphVersionId = lineageGraphVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(lineageGraphVersionId, write)
        self._commit(lineageGraphVersionId, self.names['lineageGraphVersion'])

        return lineageGraphVersionId

    def getLineageGraph(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, self.names['lineageGraph'])

    def getLineageGraphLatestVersions(self, sourceKey):
        self._check_init()
        latest = self._read_latest_versions(sourceKey, self.names['lineageGraphVersion'])
        return latest

    def getLineageGraphHistory(self, sourceKey):
        self._check_init()
        history = self._read_history(sourceKey, self.names['lineageGraphVersion'])
        return history

    def getLineageGraphVersion(self, lineageGraphVersionId):
        self._check_init()
        return self._read_version(lineageGraphVersionId, self.names['lineageGraphVersion'])