package fr.epita.broke;

import java.util.*;

public class Group {


    private final Integer _partitionNumber;
    private final List<Partition> _partitions = new ArrayList<>();
    private final Map<UUID, Partition> _uuidToPartitions = new HashMap<>();


    public Group(Integer partitionsNumber){
        _partitionNumber = partitionsNumber;

        if (partitionsNumber != -1) {
            for (int i = 0; i < partitionsNumber; i++) {
                _partitions.add(new Partition());
            }
        }
    }

    public UUID subscribe(){
        UUID uuid = UUID.randomUUID();
        int partitionId = uuid.hashCode();
        Partition partition;
        if (_partitionNumber == -1) {
            partition = new Partition();
            _partitions.add(partition);
        }
        else {
            partition = _partitions.get(partitionId % _partitions.size());
        }
        _uuidToPartitions.put(uuid, partition);
        return  uuid;
    }

    public boolean unsubscribe(UUID uuid){
        if (!_uuidToPartitions.containsKey(uuid))
            return false;

        _uuidToPartitions.remove(uuid);

        if (_partitionNumber == -1) {
            Partition partition = _uuidToPartitions.get(uuid);
            _partitions.remove(partition);
        }

        return true;
    }

    public Partition getPartitionFromUUID(final UUID uuid) {
        return _uuidToPartitions.get(uuid);
    }
}
