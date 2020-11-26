package fr.epita.broke;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class Partition {

    private static Integer _nextID = 0;

    private final Integer _id = _nextID++;
    private final Set<UUID> _subscriptionsUUID = new HashSet<>();

    public Partition() {

    }

    public boolean subscribe(UUID subscriber){
        return  _subscriptionsUUID.add(subscriber);
    }

    public boolean unsubscribe(UUID subscriber){
        return _subscriptionsUUID.remove(subscriber);
    }

    public Integer getId() {
        return _id;
    }
}
