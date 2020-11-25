package fr.epita.broke;

import fr.epita.util.Json;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.Data;

import java.util.List;

public class Endopints extends AbstractVerticle {

    private final BrokeService brokeService;

    public Endopints(final BrokeService brokeService) {
        this.brokeService = brokeService;
    }

    @Override
    public void start() throws Exception {
        super.start();


    }

    private void initEndpoints() {
        final Router router = Router.router(vertx);

        router.route(HttpMethod.POST, "/topic")
                .handler(BodyHandler.create())
                .handler(ctx -> {

                    final var json = ctx.getBodyAsString();
                    final var body = Json.decode(PostTopicRequest.class, json);
                    final var result = brokeService.postTopic(body.name, body.partitions);

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.POST, "/:topicName/:groupId")
                .handler(BodyHandler.create())
                .handler(ctx -> {
                    final var json = ctx.getBodyAsString();
                    final var body = Json.decode(PostMessagesRequest.class, json);

                    final var topicName = ctx.request().getParam("topicName");
                    final var groupId = ctx.request().getParam("groupId");

                    final var result = brokeService.postMessages(body.messages, topicName, groupId);

                    final var response = new PostMessagesResponse();
                    response.setItems(result);

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(response));
                });
    }


    @Data private static class PostTopicRequest {
        private String name;
        private int partitions;
    }

    @Data private static class PostMessagesRequest {
        private List<String> messages;
    }

    @Data private static class PostMessagesResponse {
        private List<Long> items;

        public void setItems(final List<Long> result) {
        }
    }
}
