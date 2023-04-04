package com.posthog.java;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.posthog.api.PosthogApiClient;
import com.posthog.api.core.ObjectMappers;
import com.posthog.api.resources.requests.BatchEventRequest;
import com.posthog.api.resources.types.Event;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class EventQueue {
    private static final int MAX_QUEUE_SIZE = 10000;

    private final String apiKey;
    private final BlockingQueue<Event> queue;
    private final PosthogApiClient posthogApiClient;
    private final ExecutorService flusherExecutor;
    private final long flushIntervalInMillis;
    private final int flushAt;
    private final AtomicBoolean isShutDown;

    public EventQueue(
            String apiKey,
            PosthogApiClient posthogApiClient,
            ThreadFactory threadFactory,
            int flushIntervalInMillis,
            int flushAt) {
        this.apiKey = apiKey;
        this.queue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
        this.flushAt = flushAt;
        this.flushIntervalInMillis = flushIntervalInMillis;
        this.isShutDown = new AtomicBoolean(false);
        this.posthogApiClient = posthogApiClient;
        this.flusherExecutor = Executors.newSingleThreadExecutor(threadFactory);
        this.flusherExecutor.submit(new Flusher());
    }

    public void enqueue(Event event) {
        this.queue.add(event);
    }

    public void shutdown() {
        if (isShutDown.compareAndSet(false, true)) {
            try {
                flusherExecutor.shutdown();
                flusherExecutor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Flusher runs on a background thread and takes messages from the queue. Once it collects enough
     * messages or enough time has elapsed, it triggers a flush.
     */
    class Flusher implements Runnable {

        private static final int MAX_MSG_SIZE = 32 << 10;

        /**
         * Our servers only accept batches less than 500KB. Here limit is set slightly
         * # lower to leave space for extra data that will be added later, eg. "sentAt".
         */
        private static final int BATCH_SIZE_LIMIT_IN_BYTES = 475000;

        @Override
        public void run() {
            while (true) {
                upload();
            }
        }

        private void upload() {
            List<Event> batch = getBatch();
            try {
                posthogApiClient.batchEvents(BatchEventRequest.builder()
                        .apiKey(apiKey)
                        .batch(batch)
                        .build());
            } catch (Exception e) {
                // TODO: Log failure
            }
        }

        private List<Event> getBatch() {
            List<Event> events = new ArrayList<>();
            long start = System.currentTimeMillis();
            int batchSizeInBytes = 0;
            while (events.size() < flushAt) {
                long elapsed = System.currentTimeMillis() - start;
                if (elapsed > flushIntervalInMillis) {
                    break;
                }
                try {
                    Event event = queue.poll(flushIntervalInMillis - elapsed, TimeUnit.MILLISECONDS);
                    int sizeInBytes = getMessageSizeInBytes(event);
                    if (sizeInBytes > MAX_MSG_SIZE) {
                        continue;
                    }
                    events.add(event);
                    batchSizeInBytes += sizeInBytes;
                    if (batchSizeInBytes > BATCH_SIZE_LIMIT_IN_BYTES) {
                        break;
                    }
                } catch (InterruptedException | JsonProcessingException e) {
                    // TODO: Log failure
                    break;
                }
            }
            return events;
        }
    }

    private static int getMessageSizeInBytes(Event event) throws JsonProcessingException {
        String stringifiedMessage = ObjectMappers.JSON_MAPPER.writeValueAsString(event);
        return stringifiedMessage.getBytes(StandardCharsets.UTF_8).length;
    }

}
