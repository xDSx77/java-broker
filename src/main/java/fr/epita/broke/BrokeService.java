package fr.epita.broke;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;



public class BrokeService {
    public PostMessageResponse postTopic(final String name, final int partitions) {
        return null;
    }

    public List<Long> postMessages(final List<String> messages, final String topicName, final String groupId) {
        // TODO: 11/25/2020
        return Collections.emptyList();
    }

    public SubscribeResponse subscribe(String topicName, String groupId) {
        // TODO
        return new SubscribeResponse(0,UUID.randomUUID());
    }

    public Boolean unsubscribe(String subscriptionId) {
        // TODO
        return true;
    }

    public FetchResponse fetch(String subscriptionId, int upTo, int wait) {
        // TODO
        return new FetchResponse(new ArrayList<>());
    }

    public static class PostMessageResponse {
        // TODO: 11/25/2020
    }

    public static class SubscribeResponse {
        public final Integer groupId;
        public final UUID subscriptionId;

        public SubscribeResponse(Integer groupId, UUID subscriptionId) {
            this.groupId = groupId;
            this.subscriptionId = subscriptionId;
        }
    }

    public static class FetchResponse {
        public final List<String> messages;

        public FetchResponse(List<String> messages) {
            this.messages = messages;
        }
    }

}
