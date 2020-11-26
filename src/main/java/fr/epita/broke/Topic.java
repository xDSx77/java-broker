package fr.epita.broke;

import java.util.*;
import java.util.stream.Collectors;

public class Topic {

    private final Integer _partitionsNumber;
    private final Map<UUID, Group> _uuidToGroups = new HashMap<>();
    private final Map<String, Group> _groups = new HashMap<>();

    // partitionId
    // int x = magic(partitionId);
    // foreach group -> partitions[x%partitions]
    // group -> partition

    // t1
    //    g1                 g2
    // p1 p2 p3            p4 p5 p6

    // t2
    //    g1                 g2
    // p1 p2 p3 p7         p4 p5 p6 p8

    private final List<Message> _messages = new ArrayList<>(); // +Map<Group,Partition>

    private final Map<Partition, Integer> _partitionToMessageIndex = new HashMap<>();

    public Topic(Integer partition) {
        _partitionsNumber = partition;
    }

    public Integer getPartitionsNumber() {
        return _partitionsNumber;
    }

    private Group createGroupId(final String groupId) {
        if (groupId.isEmpty())
            return null;
        Group group = new Group(_partitionsNumber);
        _groups.put(groupId, group);
        return group;
    }

    private Group getOrCreateGroupId(final String groupId) {
        Group group;
        if (_groups.containsKey(groupId))
            group = _groups.get(groupId);
        else
            group = createGroupId(groupId);
        return group;
    }

    public List<Integer> postMessages(final List<String> messages) {
        List<Integer> messagesId = new ArrayList<>();
        for (String message : messages) {
            _messages.add(new Message(message));
        }
        return messagesId;
    }

    public UUID subscribe(final String groupId) {
        final Group group = getOrCreateGroupId(groupId);
        UUID uuid = group.subscribe();
        _uuidToGroups.put(uuid, group);
        return uuid;
    }

    public boolean unsubscribe(final UUID uuid){
        boolean res = false;
        for (Group group: _groups.values()) {
            if (group.unsubscribe(uuid)) {
                _uuidToGroups.remove(uuid);
                res = true;
            }
        }
        return res;
    }

    public Group getGroupFromUUID(final UUID uuid) {
        return _uuidToGroups.get(uuid);
    }

    public List<Message> fetch(/*Group group ,*/ final Partition partition, int upTo) {
        Integer partitionMessageIndex = _partitionToMessageIndex.getOrDefault(partition, 0);
        List<Message> fetchedMessages = _messages
                // filter our partition only (for our group)
                .subList(partitionMessageIndex, _messages.size())
                .stream()
                .limit(upTo)
                .collect(Collectors.toList());
        _partitionToMessageIndex.put(partition, partitionMessageIndex + fetchedMessages.size());
        return fetchedMessages;
    }
}
