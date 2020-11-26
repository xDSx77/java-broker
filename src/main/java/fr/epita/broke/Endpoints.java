package fr.epita.broke;

import fr.epita.util.Json;
import fr.epita.broke.BrokeService.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Endpoints extends AbstractVerticle {

    private final BrokeService brokeService;
    private static final Vertx vertx = Vertx.vertx();

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

                    if (result.returnCode != HttpResponseStatus.OK.code()) {
                        ctx.response()
                                .setStatusCode(result.returnCode)
                                .end();
                    }

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.POST, "/:topicName")
                .handler(BodyHandler.create())
                .handler(ctx -> {
                    final var json = ctx.getBodyAsString();
                    final var body = Json.decode(PostMessagesRequest.class, json);

                    final var topicName = ctx.request().getParam("topicName");

                    final var result = brokeService.postMessages(body.messages, topicName);

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.POST, "/topic/:topicName/:groupId/subscribe")
                .handler(ctx -> {
                    final var topicName = ctx.request().getParam("topicName");
                    final var groupId = ctx.request().getParam("groupId");

                    final var result = brokeService.subscribe(topicName, groupId);

                    if (result.returnCode != HttpResponseStatus.OK.code()) {
                        ctx.response()
                                .setStatusCode(result.returnCode)
                                .end();
                    }

                    ctx.response()
                            .setStatusCode(HttpResponseStatus.OK.code())
                            .end(Json.encode(result));
                });

        router.route(HttpMethod.DELETE, "/:subscriptionId")
                .handler(ctx -> {
                    UUID subscriptionId;
                    try {
                        subscriptionId = UUID.fromString(ctx.request().getParam("subscriptionId"));
                        final var result = brokeService.unsubscribe(subscriptionId);

                        final var responseCode = result != null ? HttpResponseStatus.OK.code() : HttpResponseStatus.BAD_REQUEST.code();

                        ctx.response()
                                .setStatusCode(responseCode)
                                .end(Json.encode(result));
                    }
                    catch (IllegalArgumentException e) {
                        e.printStackTrace();
                        ctx.response()
                                .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
                                .end();
                    }
                });

        router.route(HttpMethod.GET, "/:subscriptionId/")
                .handler(ctx -> {
                    UUID subscriptionId;
                    try {
                         subscriptionId = UUID.fromString(ctx.request().getParam("subscriptionId"));
                    }
                    catch (IllegalArgumentException e) {
                        ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                        return;
                    }


                    int upTo;
                    try {
                        String queryUpTo = ctx.queryParams().get("upTo");
                        upTo = (queryUpTo != null) ? Integer.parseInt(queryUpTo) : 100;
                    }
                    catch (Exception e) {
                        ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                        return;
                    }

                    int wait;
                    try {
                        String queryWait = ctx.queryParams().get("wait");
                        wait = (queryWait != null) ? Integer.parseInt(queryWait) : 100;
                    }
                    catch (Exception e) {
                        ctx.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
                        return;
                    }

                    vertx.executeBlocking(promise -> {
                        final AtomicBoolean isOver = new AtomicBoolean(false);
                        long timerID = vertx.setTimer(wait, id -> {
                            isOver.set(true);
                        });
                        final var res = new ArrayList<Message>();
                        var returnCode = 200;
                        while (res.size() < upTo && !isOver.get()) {
                            final var response = brokeService.fetch(subscriptionId, upTo - res.size());
                            if (response.returnCode != HttpResponseStatus.OK.code()) {
                                returnCode = response.returnCode;
                                break;
                            }
                            res.addAll(response.messages);
                        }
                        if (isOver.get())
                            vertx.cancelTimer(timerID);
                        promise.complete(new BrokeService.FetchResponse(returnCode, res));
                    }, res -> {
                        FetchResponse response = (FetchResponse) res;
                        ctx.response()
                            .setStatusCode(response.returnCode)
                            .end(Json.encode(response.messages
                                    .stream()
                                    .map(message -> message.message)
                                    .collect(Collectors.toList())));
                    });
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