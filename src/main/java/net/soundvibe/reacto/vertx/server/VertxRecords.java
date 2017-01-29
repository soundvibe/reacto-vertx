package net.soundvibe.reacto.vertx.server;

import io.vertx.core.json.*;
import io.vertx.servicediscovery.*;
import net.soundvibe.reacto.types.*;

import java.time.*;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author OZY on 2016.08.26.
 */
public interface VertxRecords {

    String LAST_UPDATED = "updated";
    String COMMANDS = "commands";

    static boolean areEquals(Record existingRecord, Record newRecord) {
        return  Objects.equals(newRecord.getName(), existingRecord.getName()) &&
                Objects.equals(newRecord.getLocation(), existingRecord.getLocation()) &&
                Objects.equals(newRecord.getType(), existingRecord.getType());
    }

    static boolean isDown(Record existingRecord, long heartbeatIntervalInMillis) {
        return existingRecord.getStatus() == Status.DOWN || !isUpdatedRecently(existingRecord, heartbeatIntervalInMillis);
    }

    static boolean isUpdatedRecently(Record record, long heartbeatIntervalInMillis) {
        final Instant now = Instant.now();
        return Duration.between(record.getMetadata().getInstant(LAST_UPDATED, now),
                now)
                .toMillis() <= heartbeatIntervalInMillis + 10000L;
    }

    JsonArray emptyJsonArray = new JsonArray();

    static boolean hasCommand(String commandType, Record record) {
        return hasCommand(commandType, "", record);
    }

    static boolean hasCommand(String commandType, String eventType, Record record) {
        return record.getMetadata().getJsonArray(COMMANDS, emptyJsonArray)
                .stream()
                .flatMap(o -> o instanceof JsonObject ? Stream.of((JsonObject)o) : Stream.empty())
                .anyMatch(descriptor -> descriptor.getString(CommandDescriptor.COMMAND, "").equals(commandType) &&
                    descriptor.getString(CommandDescriptor.EVENT, "").equals(eventType));
    }

    static boolean isService(String serviceName, Record record) {
        return serviceName.equals(record.getName());
    }

}
