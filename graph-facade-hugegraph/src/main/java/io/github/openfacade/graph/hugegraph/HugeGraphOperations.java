package io.github.openfacade.graph.hugegraph;

import io.github.openfacade.graph.api.DataType;
import io.github.openfacade.graph.api.GraphException;
import io.github.openfacade.graph.api.GraphOperations;
import io.github.openfacade.graph.schema.CreateEdgeRequest;
import io.github.openfacade.graph.schema.CreateEdgeSchemaRequest;
import io.github.openfacade.graph.schema.CreateNodeRequest;
import io.github.openfacade.graph.schema.CreateNodeSchemaRequest;
import io.github.openfacade.graph.schema.DeleteNodeRequest;
import lombok.RequiredArgsConstructor;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Vertex;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.openfacade.graph.hugegraph.ConvertUtil.toHugeGraphDataType;

@RequiredArgsConstructor
public class HugeGraphOperations implements GraphOperations {
    private final HugeClient hugeClient;

    @Override
    public void createNode(CreateNodeRequest createNodeRequest) throws GraphException {
        String nodeId = createNodeRequest.getNodeId();
        String nodeSchema = createNodeRequest.getNodeSchema();
        Map<String, Object> properties = createNodeRequest.getProperties();
        try {
            Vertex vertex = new Vertex(nodeSchema);
            vertex.id(nodeId);

            Set<String> schemaProperties = hugeClient.schema().getVertexLabel(nodeSchema).properties();
            // check properties
            checkNodeProperties(schemaProperties, properties);

            // set properties
            properties.forEach(vertex::property);

            // add vertex to graph
            hugeClient.graph().addVertex(vertex);
        } catch (Exception e) {
            throw new GraphException("Failed to create node: " + nodeId, e);
        }
    }

    private static void checkNodeProperties(Set<String> schemaProperties, Map<String, Object> properties) {
        List<String> invalidProperties = properties.keySet()
                .stream()
                .filter(propertyName -> !schemaProperties.contains(propertyName))
                .toList();

        if (!invalidProperties.isEmpty()) {
            throw new GraphException("invalid properties: [" + String.join(",", invalidProperties) + "]");
        }
    }

    @Override
    public void createNodeSchema(CreateNodeSchemaRequest createNodeSchemaRequest) {
        String name = createNodeSchemaRequest.getName();
        Map<String, DataType> propertyKeys = createNodeSchemaRequest.getPropertyKeys();
        try {
            if (checkVertexLabelExist(name)) {
                return;
            }

            // create vertex label if not exist
            hugeClient.schema().vertexLabel(name)
                    .useCustomizeStringId()
                    .ifNotExist()
                    .create();

            // create property keys if not exist
            propertyKeys.forEach((propertyName, dataType) ->
                    hugeClient.schema().propertyKey(propertyName)
                    .dataType(toHugeGraphDataType(dataType))
                    .ifNotExist()
                    .create());

            // bind properties to node
            if (!propertyKeys.isEmpty()) {
                String[] propertyNames = propertyKeys.keySet().toArray(new String[0]);
                hugeClient.schema().vertexLabel(name)
                        .nullableKeys(propertyNames)
                        .properties(propertyNames)
                        .append();
            }
        } catch (Exception e) {
            throw new GraphException("Failed to create node schema for name: " + name, e);
        }
    }

    @Override
    public void createEdge(@NonNull CreateEdgeRequest createEdgeRequest) throws GraphException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createEdgeSchema(@NonNull CreateEdgeSchemaRequest createEdgeSchemaRequest) throws GraphException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteNode(@NonNull DeleteNodeRequest deleteNodeRequest) throws GraphException {
        String nodeId = deleteNodeRequest.getNodeId();
        String nodeSchema = deleteNodeRequest.getNodeSchema();
        try {
            // Check if the node schema exists
            if (!checkVertexLabelExist(nodeSchema)) {
                throw new GraphException("Node schema does not exist: " + nodeSchema);
            }

            // Check if the node exists before attempting deletion
            Vertex vertex = hugeClient.graph().getVertex(nodeId);
            if (vertex == null) {
                // Considered idempotent - return silently if node doesn't exist
                return;
            }

            // Verify the node belongs to the specified schema
            if (!vertex.label().equals(nodeSchema)) {
                throw new GraphException("Node " + nodeId + " does not belong to schema " + nodeSchema + 
                                        ". Actual schema: " + vertex.label());
            }

            // In HugeGraph, removing a vertex automatically removes all its edges
            // This is the default behavior, but we should make it explicit in the code comment
            hugeClient.graph().removeVertex(nodeId);
        } catch (GraphException e) {
            // Re-throw our custom exceptions
            throw e;
        } catch (Exception e) {
            throw new GraphException("Failed to delete node: " + nodeId, e);
        }
    }

    private boolean checkVertexLabelExist(String name) {
        try {
            hugeClient.schema().getVertexLabel(name);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
