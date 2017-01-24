package net.soundvibe.reacto.vertx.types;

import net.soundvibe.reacto.mappers.ServiceRegistryMapper;
import net.soundvibe.reacto.types.*;

/**
 * @author OZY on 2017.01.10.
 */
public class DemoServiceRegistryMapper implements ServiceRegistryMapper {

    @Override
    public <C, E> TypedCommand toCommand(C genericCommand, Class<? extends E> eventClass) {
        if (!(genericCommand instanceof MakeDemo)) {
            throw new IllegalArgumentException("Expected MakeDemo class but got: " + genericCommand.getClass());
        }
        final MakeDemo makeDemo = (MakeDemo) genericCommand;
        return TypedCommand.create(MakeDemo.class, eventClass, MetaData.of("name", makeDemo.name));
    }

    @Override
    public <E> E toGenericEvent(Event event, Class<? extends E> eventClass) {
        if (!(eventClass.equals(DemoMade.class))) {
            throw new IllegalArgumentException("Expected DemoMade event but got: " + eventClass);
        }
        return eventClass.cast(new DemoMade(event.get("name")));
    }
}
