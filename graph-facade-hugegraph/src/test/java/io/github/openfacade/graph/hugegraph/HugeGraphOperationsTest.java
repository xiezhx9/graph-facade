package io.github.openfacade.graph.hugegraph;

import io.github.openfacade.graph.api.DataType;
import io.github.openfacade.graph.api.GraphException;
import io.github.openfacade.graph.schema.CreateNodeRequest;
import io.github.openfacade.graph.schema.CreateNodeSchemaRequest;
import io.github.openfacade.graph.schema.DeleteNodeRequest;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class HugeGraphOperationsTest {

    @Container
    private static final GenericContainer<?> hugeGraphServer = new GenericContainer<>(
            DockerImageName.parse("hugegraph/hugegraph:1.5.0"))
            .withExposedPorts(8080)
            .waitingFor(
                    org.testcontainers.containers.wait.strategy.Wait.forHttp("/versions")
                            .forPort(8080)
                            .withStartupTimeout(java.time.Duration.ofSeconds(60))
            );

    private HugeClient hugeClient;
    private HugeGraphOperations operations;

    @BeforeEach
    void setUp() {
        // The container will automatically wait for the health check before proceeding
        String host = hugeGraphServer.getHost();
        Integer port = hugeGraphServer.getMappedPort(8080);

        // Create HugeGraph client
        String url = "http://" + host + ":" + port;
        hugeClient = HugeClient.builder(url, "hugegraph")
                .configTimeout(20)
                .build();

        operations = new HugeGraphOperations(hugeClient);
    }

    @AfterEach
    void tearDown() {
        if (hugeClient != null) {
            hugeClient.close();
        }
    }

    @Test
    void testCreateNodeSchemaAndNodeSuccess() throws GraphException {
        String nodeSchemaName = "person";
        CreateNodeSchemaRequest req = CreateNodeSchemaRequest.builder()
                .name(nodeSchemaName)
                .propertyKeys(Map.of(
                        "name", DataType.TEXT,
                        "age", DataType.INT))
                .build();
        operations.createNodeSchema(req);


        String nodeId = "alice";
        Map<String, Object> properties = Map.of(
                "name", "Alice",
                "age", 30);

        CreateNodeRequest createNodeRequest = CreateNodeRequest.builder()
                .nodeId(nodeId)
                .nodeSchema(nodeSchemaName)
                .properties(properties)
                .build();

        operations.createNode(createNodeRequest);

        Vertex createdVertex = hugeClient.graph().getVertex(nodeId);
        assertNotNull(createdVertex);
        assertEquals("person", createdVertex.label());
        assertEquals("Alice", createdVertex.property("name"));
        assertEquals(30, createdVertex.property("age"));
    }

    @Test
    void testDeleteNodeSuccess() throws GraphException {
        // 1. Create a node schema
        String nodeSchemaName = "delete_test_person";
        CreateNodeSchemaRequest schemaReq = CreateNodeSchemaRequest.builder()
                .name(nodeSchemaName)
                .propertyKeys(Map.of(
                        "name", DataType.TEXT,
                        "email", DataType.TEXT))
                .build();
        operations.createNodeSchema(schemaReq);

        // 2. Create a node
        String nodeId = "test_user_to_delete";
        Map<String, Object> properties = Map.of(
                "name", "Test User",
                "email", "test@example.com");

        CreateNodeRequest createNodeRequest = CreateNodeRequest.builder()
                .nodeId(nodeId)
                .nodeSchema(nodeSchemaName)
                .properties(properties)
                .build();

        operations.createNode(createNodeRequest);

        // 3. Verify node exists
        Vertex createdVertex = hugeClient.graph().getVertex(nodeId);
        assertNotNull(createdVertex);
        assertEquals(nodeSchemaName, createdVertex.label());

        // 4. Delete the node
        DeleteNodeRequest deleteNodeRequest = DeleteNodeRequest.deleteNodeRequest(nodeId, nodeSchemaName);
        operations.deleteNode(deleteNodeRequest);

        // 5. Verify node is deleted (should return null or throw exception)
        Vertex deletedVertex = hugeClient.graph().getVertex(nodeId);
        assertNull(deletedVertex);
    }

    @Test
    void testDeleteNonExistentNode() throws GraphException {
        // 1. Create a node schema
        String nodeSchemaName = "delete_test_schema";
        CreateNodeSchemaRequest schemaReq = CreateNodeSchemaRequest.builder()
                .name(nodeSchemaName)
                .propertyKeys(Map.of("name", DataType.TEXT))
                .build();
        operations.createNodeSchema(schemaReq);

        // 2. Try to delete a non-existent node
        String nonExistentNodeId = "non_existent_node";
        DeleteNodeRequest deleteNodeRequest = DeleteNodeRequest.deleteNodeRequest(nonExistentNodeId, nodeSchemaName);
        
        // 3. This should either succeed (idempotent delete) or throw an exception
        // HugeGraph's behavior is to return null when getting non-existent vertices
        // and not throw exception when deleting non-existent vertices
        operations.deleteNode(deleteNodeRequest);
        
        // Verify the node is not present
        Vertex vertex = hugeClient.graph().getVertex(nonExistentNodeId);
        assertNull(vertex);
    }

    @Test
    void testDeleteNodeWithNonExistentSchema() {
        // Try to delete a node with a non-existent schema
        String nodeId = "test_node";
        String nonExistentSchema = "non_existent_schema";
        DeleteNodeRequest deleteNodeRequest = DeleteNodeRequest.deleteNodeRequest(nodeId, nonExistentSchema);
        
        // Should throw GraphException with appropriate message
        GraphException exception = assertThrows(GraphException.class, () -> {
            operations.deleteNode(deleteNodeRequest);
        });
        
        assertTrue(exception.getMessage().contains("Node schema does not exist"));
    }
}
