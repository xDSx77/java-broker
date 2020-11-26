package fr.epita.broke;

import java.util.*;


public class BrokeService {

    private static final Map<String, Topic> _topics = new HashMap<>();
    private static final Map<UUID, Topic> _uuidToTopic = new HashMap<>();

    public PostTopicResponse postTopic(final String name, final int partitions) {
        if (name == null || name.isEmpty())
            return new PostTopicResponse(400);

        if (_topics.containsKey(name))
            return new PostTopicResponse(409);

        _topics.put(name, new Topic(partitions));
        return new PostTopicResponse(200);
    }

    public PostMessageResponse postMessages(final List<String> messages, final String topicName) {

        if (topicName == null) {
            return null;
        }

        if (!_topics.containsKey(topicName)) {
            final var response = postTopic(topicName, -1);
            if (response.returnCode != 200)
                return new PostMessageResponse(response.returnCode);
        }

        Topic topic = _topics.get(topicName);
        final List<Integer> messagesId = topic.postMessages(messages);

        return new PostMessageResponse(200, messagesId);
    }

    public SubscribeResponse subscribe(String topicName, String groupId) {

        if (topicName == null || groupId == null) {
            return null;
        }

        if (!_topics.containsKey(topicName)) {
            return new SubscribeResponse(404);
        }

        Topic topic = _topics.get(topicName);
        UUID uuid = topic.subscribe(groupId);

        _uuidToTopic.put(uuid,topic);

        return new SubscribeResponse(200, groupId, uuid);
    }

    public UnsubscribeResponse unsubscribe(UUID subscriptionUUID) {

        if (subscriptionUUID == null)
            return new UnsubscribeResponse(400);

        boolean res = false;

        for (Topic topic: _topics.values()) {
            if (topic.unsubscribe(subscriptionUUID))
                res = true;
        }

        _uuidToTopic.remove(subscriptionUUID);

        if (res)
            return new UnsubscribeResponse(200);
        else
            return  new UnsubscribeResponse(400);
    }


    public FetchResponse fetch(UUID uuid, int upTo) {

        Topic topic = _uuidToTopic.getOrDefault(uuid, null);

        if (topic == null){
            return new FetchResponse(404);
        }

        Group group = topic.getGroupFromUUID(uuid);

        if (group == null){
            return new FetchResponse(404);
        }

        Partition partition = group.getPartitionFromUUID(uuid);

        if (partition == null){
            return new FetchResponse(404);
        }

        List<Message> messages = topic.fetch(partition, upTo);

        return new FetchResponse(200, messages);
    }


    public static class PostMessageResponse {
        public final int returnCode;
        public final List<Integer> messages;

        public PostMessageResponse(final int returnCode, final List<Integer> messages){
            this.returnCode = returnCode;
            this.messages = messages;
        }

        public PostMessageResponse(final int returnCode) {
            this.returnCode = returnCode;
            this.messages = null;
        }

    }

    public static class PostTopicResponse {
        public final int returnCode;

        public PostTopicResponse(final int returnCode) {
            this.returnCode = returnCode;
        }
    }

    public static class SubscribeResponse {
        public final int returnCode;
        public final String groupId;
        public final UUID subscriptionId;

        public SubscribeResponse(final int returnCode) {
            this.returnCode = returnCode;
            this.groupId = null;
            this.subscriptionId = null;
        }

        public SubscribeResponse(final int returnCode, final String groupId, final UUID subscriptionId) {
            this.returnCode = returnCode;
            this.groupId = groupId;
            this.subscriptionId = subscriptionId;
        }
    }

    public static class UnsubscribeResponse {
        public final int returnCode;

        public UnsubscribeResponse(final int returnCode) {
            this.returnCode = returnCode;
        }
    }

    public static class FetchResponse {
        public final int returnCode;
        public final List<Message> messages;

        public FetchResponse(final int returnCode)
        {
            this.returnCode = returnCode;
            this.messages = new ArrayList<>();
        }

        public FetchResponse(final int returnCode, final List<Message> messages) {
            this.returnCode = returnCode;
            this.messages = messages;
        }
    }

}
