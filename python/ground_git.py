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
        newid = len(ids)
        ids[newid] = newid
        with open(self.path + 'ids.json', 'w') as f2:
            f2.write(json.dumps(ids))
        return newid

    def _write_files(self, id, body):
        with open(self.path + str(id) + '.json', 'w') as f:
            f.write(json.dumps(body))

    def _read_files(self, sourceKey, className):
        files = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
        for file in files:
            filename = file.split('.')
            if (filename[-1] == 'json') and (filename[0] != 'ids'):
                with open(self.path + file, 'r') as f:
                    fileDict = json.loads(f.read())
                    if (('sourceKey' in fileDict) and (fileDict['sourceKey'] == sourceKey)
                        and (fileDict['class'] == className)):
                        return fileDict

    def _read_version(self, id, className):
        files = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
        for file in files:
            filename = file.split('.')
            if (filename[-1] == 'json') and (filename[0] == str(id)):
                with open(self.path + file, 'r') as f:
                    fileDict = json.loads(f.read())
                    if (fileDict['class'] == className):
                        return fileDict

    def _read_all_version(self, sourceKey, className, baseClassName):
        baseId = (self._read_files(sourceKey, baseClassName))['id']
        baseIdName = baseClassName[:1].lower() + baseClassName[1:] + "Id"

        versions = {}
        files = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
        for file in files:
            filename = file.split('.')
            if (filename[-1] == 'json') and (filename[0] != 'ids'):
                with open(self.path + file, 'r') as f:
                    fileDict = json.loads(f.read())
                    if ((baseIdName in fileDict) and (fileDict[baseIdName] == baseId)
                        and (fileDict['class'] == className)):
                        versions[fileDict['id']] = fileDict
        return versions

    def _read_all_version_ever(self, className):
        versions = {}
        files = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
        for file in files:
            filename = file.split('.')
            if (filename[-1] == 'json') and (filename[0] != 'ids'):
                with open(self.path + file, 'r') as f:
                    fileDict = json.loads(f.read())
                    if (fileDict['class'] == className):
                        versions[fileDict['id']] = fileDict
        return versions

    def _find_file(self, sourceKey, className):
        files = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f))]
        for file in files:
            filename = file.split('.')
            if (filename[-1] == 'json') and (filename[0] != 'ids'):
                with open(self.path + file, 'r') as f:
                    fileDict = json.loads(f.read())
                    if (('sourceKey' in fileDict) and (fileDict['sourceKey'] == sourceKey)
                        and (fileDict['class'] == className)):
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
                f.write(json.dumps({}))
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
        if not self._find_file(sourceKey, Edge.__name__):
            body = self._create_item(Edge.__name__, sourceKey, name, tags)
            body["fromNodeId"] = fromNodeId
            body["toNodeId"] = toNodeId
            edge = Edge(body)
            edgeId = edge.get_id()
            write = self._deconstruct_item(edge)
            write["fromNodeId"] = edge.get_from_node_id()
            write["toNodeId"] = edge.get_to_node_id()
            self._write_files(edgeId, write)
            self._commit(edgeId, Edge.__name__)
        else:
            edge = self._read_files(sourceKey, Edge.__name__)
            edgeId = edge['id']

        return edgeId

    def createEdgeVersion(self, edgeId, toNodeVersionStartId, fromNodeVersionStartId, toNodeVersionEndId=None,
                          fromNodeVersionEndId=None, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(EdgeVersion.__name__, reference, referenceParameters,
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
        self._commit(edgeVersionId, EdgeVersion.__name__)

        return edgeVersionId

    def getEdge(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, Edge.__name__)


    def getEdgeLatestVersions(self, sourceKey):
        self._check_init()
        edgeVersionMap = self._read_all_version(sourceKey, EdgeVersion.__name__, Edge.__name__)
        edgeVersions = set(list(edgeVersionMap.keys()))
        is_parent = set([])
        for evId in edgeVersions:
            ev = edgeVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    is_parent |= {parentId, }
        return [edgeVersionMap[Id] for Id in list(edgeVersions - is_parent)]

    def getEdgeHistory(self, sourceKey):
        self._check_init()
        edgeVersionMap = self._read_all_version(sourceKey, EdgeVersion.__name__, Edge.__name__)
        edgeVersions = set(list(edgeVersionMap.keys()))
        parentChild = {}
        for evId in edgeVersions:
            ev = edgeVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    if not parentChild:
                        edgeId = ev['edgeId']
                        parentChild[str(edgeId)] = parentId
                    parentChild[str(parentId)] = ev['id']
        return parentChild

    def getEdgeVersion(self, edgeVersionId):
        self._check_init()
        return self._read_version(edgeVersionId, EdgeVersion.__name__)

    ### NODES ###
    def createNode(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, Node.__name__):
            body = self._create_item(Node.__name__, sourceKey, name, tags)
            node = Node(body)
            nodeId = node.get_item_id()
            write = self._deconstruct_item(node)
            self._write_files(nodeId, write)
            self._commit(nodeId, Node.__name__)
        else:
            node = self._read_files(sourceKey, Node.__name__)
            nodeId = node['id']

        return nodeId

    def createNodeVersion(self, nodeId, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(NodeVersion.__name__, reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["nodeId"] = nodeId

        nodeVersion = NodeVersion(body)
        nodeVersionId = nodeVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(nodeVersionId, write)
        self._commit(nodeVersionId, NodeVersion.__name__)

        return nodeVersionId


    def getNode(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, Node.__name__)

    def getNodeLatestVersions(self, sourceKey):
        self._check_init()
        nodeVersionMap = self._read_all_version(sourceKey, NodeVersion.__name__, Node.__name__)
        nodeVersions = set(list(nodeVersionMap.keys()))
        is_parent = set([])
        for evId in nodeVersions:
            ev = nodeVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    is_parent |= {parentId, }
        return [nodeVersionMap[Id] for Id in list(nodeVersions - is_parent)]

    def getNodeHistory(self, sourceKey):
        self._check_init()
        nodeVersionMap = self._read_all_version(sourceKey, NodeVersion.__name__, Node.__name__)
        nodeVersions = set(list(nodeVersionMap.keys()))
        parentChild = {}
        for evId in nodeVersions:
            ev = nodeVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    if not parentChild:
                        nodeId = ev['nodeId']
                        parentChild[str(nodeId)] = parentId
                    parentChild[str(parentId)] = ev['id']
        return parentChild

    def getNodeVersion(self, nodeVersionId):
        self._check_init()
        return self._read_version(nodeVersionId, NodeVersion.__name__)


    def getNodeVersionAdjacentLineage(self, nodeVersionId):
        self._check_init()
        lineageEdgeVersionMap = self._read_all_version_ever(LineageEdgeVersion.__name__)
        lineageEdgeVersions = set(list(lineageEdgeVersionMap.keys()))
        adjacent = []
        for levId in lineageEdgeVersions:
            lev = lineageEdgeVersionMap[levId]
            if ((nodeVersionId == lev['toRichVersionId']) or (nodeVersionId == lev['fromRichVersionId'])):
                adjacent.append(lev)
        return adjacent


    ### GRAPHS ###
    def createGraph(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, Graph.__name__):
            body = self._create_item(Graph.__name__, sourceKey, name, tags)
            graph = Graph(body)
            graphId = graph.get_item_id()
            write = self._deconstruct_item(graph)
            self._write_files(graphId, write)
            self._commit(graphId, Graph.__name__)
        else:
            graph = self._read_files(sourceKey, Graph.__name__)
            graphId = graph['id']

        return graphId


    def createGraphVersion(self, graphId, edgeVersionIds, reference=None,
                           referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(GraphVersion.__name__, reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["graphId"] = graphId
        body["edgeVersionIds"] = edgeVersionIds

        graphVersion = GraphVersion(body)
        graphVersionId = graphVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(graphVersionId, write)
        self._commit(graphVersionId, GraphVersion.__name__)

        return graphVersionId

    def getGraph(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, Graph.__name__)

    def getGraphLatestVersions(self, sourceKey):
        self._check_init()
        graphVersionMap = self._read_all_version(sourceKey, GraphVersion.__name__, Graph.__name__)
        graphVersions = set(list(graphVersionMap.keys()))
        is_parent = set([])
        for evId in graphVersions:
            ev = graphVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    is_parent |= {parentId, }
        return [graphVersionMap[Id] for Id in list(graphVersions - is_parent)]

    def getGraphHistory(self, sourceKey):
        self._check_init()
        graphVersionMap = self._read_all_version(sourceKey, GraphVersion.__name__, Graph.__name__)
        graphVersions = set(list(graphVersionMap.keys()))
        parentChild = {}
        for evId in graphVersions:
            ev = graphVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    if not parentChild:
                        graphId = ev['graphId']
                        parentChild[str(graphId)] = parentId
                    parentChild[str(parentId)] = ev['id']
        return parentChild

    def getGraphVersion(self, graphVersionId):
        self._check_init()
        return self._read_version(graphVersionId, GraphVersion.__name__)

    ### STRUCTURES ###
    def createStructure(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, Structure.__name__):
            body = self._create_item(Structure.__name__, sourceKey, name, tags)
            structure = Structure(body)
            structureId = structure.get_item_id()
            write = self._deconstruct_item(structure)
            self._write_files(structureId, write)
            self._commit(structureId, Structure.__name__)
        else:
            structure = self._read_files(sourceKey, Structure.__name__)
            structureId = structure['id']

        return structureId


    def createStructureVersion(self, structureId, attributes, parentIds=None):
        self._check_init()
        body = {
            "id": self._gen_id(),
            "class":StructureVersion.__name__,
            "structureId": structureId,
            "attributes": attributes
        }

        if parentIds:
            body["parentIds"] = parentIds

        structureVersion = StructureVersion(body)
        structureVersionId = structureVersion.get_id()

        write = dict(body)
        self._write_files(structureVersionId, write)
        self._commit(structureVersionId, StructureVersion.__name__)

        return structureVersionId

    def getStructure(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, Structure.__name__)

    def getStructureLatestVersions(self, sourceKey):
        self._check_init()
        structureVersionMap = self._read_all_version(sourceKey, StructureVersion.__name__, Structure.__name__)
        structureVersions = set(list(structureVersionMap.keys()))
        is_parent = set([])
        for evId in structureVersions:
            ev = structureVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    is_parent |= {parentId, }
        return [structureVersionMap[Id] for Id in list(structureVersions - is_parent)]

    def getStructureHistory(self, sourceKey):
        self._check_init()
        structureVersionMap = self._read_all_version(sourceKey, StructureVersion.__name__, Structure.__name__)
        structureVersions = set(list(structureVersionMap.keys()))
        parentChild = {}
        for evId in structureVersions:
            ev = structureVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    if not parentChild:
                        structureId = ev['structureId']
                        parentChild[str(structureId)] = parentId
                    parentChild[str(parentId)] = ev['id']
        return parentChild

    def getStructureVersion(self, structureVersionId):
        self._check_init()
        return self._read_version(structureVersionId, StructureVersion.__name__)


    ### LINEAGE EDGES ###
    def createLineageEdge(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, LineageEdge.__name__):
            body = self._create_item(LineageEdge.__name__, sourceKey, name, tags)
            lineageEdge = LineageEdge(body)
            lineageEdgeId = lineageEdge.get_id()
            write = self._deconstruct_item(lineageEdge)
            self._write_files(lineageEdgeId, write)
            self._commit(lineageEdgeId, LineageEdge.__name__)
        else:
            lineageEdge = self._read_files(sourceKey, LineageEdge.__name__)
            lineageEdgeId = lineageEdge['id']

        return lineageEdgeId


    def createLineageEdgeVersion(self, lineageEdgeId, toRichVersionId, fromRichVersionId, reference=None,
                                 referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(LineageEdgeVersion.__name__, reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["lineageEdgeId"] = lineageEdgeId
        body["toRichVersionId"] = toRichVersionId
        body["fromRichVersionId"] = fromRichVersionId

        lineageEdgeVersion = LineageEdgeVersion(body)
        lineageEdgeVersionId = lineageEdgeVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(lineageEdgeVersionId, write)
        self._commit(lineageEdgeVersionId, LineageEdgeVersion.__name__)

        return lineageEdgeVersionId

    def getLineageEdge(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, LineageEdge.__name__)

    def getLineageEdgeLatestVersions(self, sourceKey):
        self._check_init()
        lineageEdgeVersionMap = self._read_all_version(sourceKey, LineageEdgeVersion.__name__, LineageEdge.__name__)
        lineageEdgeVersions = set(list(lineageEdgeVersionMap.keys()))
        is_parent = set([])
        for evId in lineageEdgeVersions:
            ev = lineageEdgeVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    is_parent |= {parentId, }
        return [lineageEdgeVersionMap[Id] for Id in list(lineageEdgeVersions - is_parent)]

    def getLineageEdgeHistory(self, sourceKey):
        self._check_init()
        lineageEdgeVersionMap = self._read_all_version(sourceKey, LineageEdgeVersion.__name__, LineageEdge.__name__)
        lineageEdgeVersions = set(list(lineageEdgeVersionMap.keys()))
        parentChild = {}
        for evId in lineageEdgeVersions:
            ev = lineageEdgeVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    if not parentChild:
                        lineageEdgeId = ev['lineageEdgeId']
                        parentChild[str(lineageEdgeId)] = parentId
                    parentChild[str(parentId)] = ev['id']
        return parentChild

    def getLineageEdgeVersion(self, lineageEdgeVersionId):
        self._check_init()
        return self._read_version(lineageEdgeVersionId, LineageEdgeVersion.__name__)

    ### LINEAGE GRAPHS ###
    def createLineageGraph(self, sourceKey, name="null", tags=None):
        self._check_init()
        if not self._find_file(sourceKey, LineageGraph.__name__):
            body = self._create_item(LineageGraph.__name__, sourceKey, name, tags)
            lineageGraph = LineageGraph(body)
            lineageGraphId = lineageGraph.get_id()
            write = self._deconstruct_item(lineageGraph)
            self._write_files(lineageGraphId, write)
            self._commit(lineageGraphId, LineageGraph.__name__)
        else:
            lineageGraph = self._read_files(sourceKey, LineageGraph.__name__)
            lineageGraphId = lineageGraph['id']

        return lineageGraphId


    def createLineageGraphVersion(self, lineageGraphId, lineageEdgeVersionIds, reference=None,
                                  referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self._check_init()
        body = self._get_rich_version_json(LineageGraphVersion.__name__, reference, referenceParameters,
                                           tags, structureVersionId, parentIds)

        body["lineageGraphId"] = lineageGraphId
        body["lineageEdgeVersionIds"] = lineageEdgeVersionIds

        lineageGraphVersion = LineageGraphVersion(body)
        lineageGraphVersionId = lineageGraphVersion.get_id()

        write = self._deconstruct_rich_version_json(body)
        self._write_files(lineageGraphVersionId, write)
        self._commit(lineageGraphVersionId, LineageGraphVersion.__name__)

        return lineageGraphVersionId

    def getLineageGraph(self, sourceKey):
        self._check_init()
        return self._read_files(sourceKey, LineageGraph.__name__)

    def getLineageGraphLatestVersions(self, sourceKey):
        self._check_init()
        lineageGraphVersionMap = self._read_all_version(sourceKey, LineageGraphVersion.__name__, LineageGraph.__name__)
        lineageGraphVersions = set(list(lineageGraphVersionMap.keys()))
        is_parent = set([])
        for evId in lineageGraphVersions:
            ev = lineageGraphVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    is_parent |= {parentId, }
        return [lineageGraphVersionMap[Id] for Id in list(lineageGraphVersions - is_parent)]

    def getLineageGraphHistory(self, sourceKey):
        self._check_init()
        lineageGraphVersionMap = self._read_all_version(sourceKey, LineageGraphVersion.__name__, LineageGraph.__name__)
        lineageGraphVersions = set(list(lineageGraphVersionMap.keys()))
        parentChild = {}
        for evId in lineageGraphVersions:
            ev = lineageGraphVersionMap[evId]
            if ('parentIds' in ev) and (ev['parentIds']):
                assert type(ev['parentIds']) == list
                for parentId in ev['parentIds']:
                    if not parentChild:
                        lineageGraphId = ev['lineageGraphId']
                        parentChild[str(lineageGraphId)] = parentId
                    parentChild[str(parentId)] = ev['id']
        return parentChild

    def getLineageGraphVersion(self, lineageGraphVersionId):
        self._check_init()
        return self._read_version(lineageGraphVersionId, LineageGraphVersion.__name__)