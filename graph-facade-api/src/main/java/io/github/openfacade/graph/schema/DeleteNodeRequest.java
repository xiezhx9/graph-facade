package io.github.openfacade.graph.schema;

import lombok.Getter;
import lombok.Setter;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

@Getter
@Setter
public class DeleteNodeRequest {

    @NonNull
    private String nodeId;

    @NonNull
    private String nodeSchema;

    private DeleteNodeRequest(@NonNull String nodeId, @NonNull String nodeSchema) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.nodeSchema = Objects.requireNonNull(nodeSchema, "nodeSchema must not be null");
    }

    public static DeleteNodeRequest deleteNodeRequest(@NonNull String nodeId, @NonNull String nodeSchema) {
        return new DeleteNodeRequest(nodeId, nodeSchema);
    }
}