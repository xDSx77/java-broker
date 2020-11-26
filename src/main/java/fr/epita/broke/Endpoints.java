package fr.epita.broke;

import fr.epita.util.Json;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.Data;

import java.util.List;

public class Endpoints extends AbstractVerticle {

    private final BrokeService brokeService;

    public Endpoints(final BrokeService brokeService) {
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

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.POST, "/topic/:topicName/:groupId/subscribe")
                .handler(ctx -> {
                    final var topicName = ctx.request().getParam("topicName");
                    final var groupId = ctx.request().getParam("groupId");

                    final var result = brokeService.subscribe(topicName, groupId);

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.DELETE, "/:subscriptionId")
                .handler(ctx ->{
                    final var subscriptionId = ctx.request().getParam("subscriptionId");

                    final var result = brokeService.unsubscribe(subscriptionId);

                    final var responseCode = result ? HttpResponseStatus.OK.code() : HttpResponseStatus.BAD_REQUEST.code();

                    ctx.response()
                            .setStatusCode(responseCode)
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.GET, "/:subscriptionId/")
                .handler(ctx -> {
                    final var subscriptionId = ctx.request().getParam("subscriptionId");

                    String queryUpTo = ctx.queryParams().get("upTo");
                    final var upTo = (queryUpTo != null) ? Integer.parseInt(queryUpTo) : 100;

                    String queryWait = ctx.queryParams().get("wait");
                    final var wait = (queryWait != null) ? Integer.parseInt(queryWait) : 100;

                    final var result = brokeService.fetch(subscriptionId, upTo, wait);

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });
    }


    @Data private static class PostTopicRequest {
        private String name;
        private int partitions;
    }

    @Data private static class PostMessagesRequest {
        private List<String> messages;
    }
}
