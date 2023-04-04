package com.posthog.java;

import com.posthog.api.PosthogApiClient;
import com.posthog.api.core.Environment;
import com.posthog.api.resources.feature.flags.requests.DecideFeatureFlagsRequest;
import com.posthog.api.resources.feature.flags.types.DecideFeatureFlagsResponse;
import com.posthog.api.resources.types.AliasEvent;
import com.posthog.api.resources.types.AliasEventProperties;
import com.posthog.api.resources.types.CaptureEvent;
import com.posthog.api.resources.types.Event;
import com.posthog.api.resources.types.GroupIdentifyEvent;
import com.posthog.api.resources.types.GroupIdentifyEventProperties;
import com.posthog.api.resources.types.IdentifyEvent;
import com.posthog.api.resources.types.PageViewEvent;
import com.posthog.api.resources.types.SetEvent;
import com.posthog.api.resources.types.SetOnceEvent;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import java.util.concurrent.Executors;
import jdk.javadoc.internal.doclint.Env;
import jdk.tools.jlink.internal.Platform;
import org.json.JSONException;
import org.json.JSONObject;

public class PostHog {

    private PosthogApiClient posthogApiClient;
    private EventQueue queue;

    public static class Builder {
        // required
        private final String apiKey;
        private int flushAt = 100;
        private int flushIntervalMillis = 500;

        // optional
        private Environment environment = Environment.CLOUD;

        public Builder(String apiKey) {
            this.apiKey = apiKey;
        }

        public Builder host(String host) {
            this.environment = Environment.custom(host);
            return this;
        }

        public Builder flushAt(int flushAt) {
            this.flushAt = flushAt;
            return this;
        }

        public Builder flushIntervalMillis(int flushIntervalMillis) {
            this.flushIntervalMillis = flushIntervalMillis;
            return this;
        }

        public PostHog build() {
            return new PostHog(this);
        }
    }

    private PostHog(Builder builder) {
        this.posthogApiClient = PosthogApiClient.builder()
                .environment(builder.environment)
                .build();
        this.queue = new EventQueue(
                builder.apiKey,
                posthogApiClient,
                Executors.defaultThreadFactory(),
                builder.flushIntervalMillis,
                builder.flushAt);
    }

    public void shutdown() {
        queue.shutdown();
    }

    /**
     *
     * @param distinctId which uniquely identifies your user in your database. Must
     *                   not be null or empty.
     * @param event      name of the event. Must not be null or empty.
     * @param properties an array with any event properties you'd like to set.
     */
    public void capture(String distinctId, String event, Map<String, Object> properties) {
        CaptureEvent._FinalStage captureEventBuilder = CaptureEvent.builder()
                .event(event)
                .distinctId(distinctId)
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now().toString());
        if (properties != null) {
            captureEventBuilder.properties(properties);
        }
        enqueue(Event.of(captureEventBuilder.build()));
    }

    /**
     *
     * @param distinctId which uniquely identifies your user in your database. Must
     *                   not be null or empty.
     * @param event      name of the event. Must not be null or empty.
     */
    public void capture(String distinctId, String event) {
        capture(distinctId, event, null);
    }

    /**
     *
     * @param distinctId        which uniquely identifies your user in your
     *                          database. Must not be null or empty.
     * @param properties        an array with any person properties you'd like to
     */
    public void identify(String distinctId, Map<String, Object> properties) {
        IdentifyEvent._FinalStage identifyEventBuilder = IdentifyEvent.builder()
                .event("$identify")
                .distinctId(distinctId)
                .timestamp(Instant.now().toString());;
        if (properties != null) {
            identifyEventBuilder.properties(properties);
        }
        enqueue(Event.of(identifyEventBuilder.build()));
    }

    /**
     *
     * @param distinctId distinct ID to merge. Must not be null or empty. Note: If
     *                   there is a conflict, the properties of this person will
     *                   take precedence.
     * @param alias      distinct ID to merge. Must not be null or empty. Note: If
     *                   there is a conflict, the properties of this person will be
     *                   overriden.
     */
    public void alias(String distinctId, String alias) {
        AliasEvent aliasEvent = AliasEvent.builder()
                .event("$create_alias")
                .distinctId(distinctId)
                .properties(AliasEventProperties.builder()
                        .distinctId(distinctId)
                        .alias(alias)
                        .build())
                .timestamp(Instant.now().toString())
                .build();
        enqueue(Event.of(aliasEvent));
    }

    /**
     *
     * @param distinctId which uniquely identifies your user in your database. Must
     *                   not be null or empty.
     * @param properties a map with any person properties you'd like to set.
     */
    public void set(String distinctId, Map<String, Object> properties) {
        SetEvent._FinalStage setEventBuilder = SetEvent.builder()
                .event("$set")
                .distinctId(distinctId)
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now().toString());
        if (properties != null) {
            setEventBuilder.properties(properties);
        }
        enqueue(Event.of(setEventBuilder.build()));
    }

    /**
     *
     * @param distinctId which uniquely identifies your user in your database. Must
     *                   not be null or empty.
     * @param properties a map with any person properties you'd like to set.
     *                   Previous values will not be overwritten.
     */
    public void setOnce(String distinctId, Map<String, Object> properties) {
        SetOnceEvent._FinalStage setOnceBuilder = SetOnceEvent.builder()
                .event("$set_once")
                .distinctId(distinctId)
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now().toString());
        if (properties != null) {
            setOnceBuilder.properties(properties);
        }
        enqueue(Event.of(setOnceBuilder.build()));
    }

    /**
     *
     * @param groupType
     * @param groupKey
     * @param properties
     */
    public void groupIdentify(
            String groupType,
            String groupKey,
            Map<String, Object> properties) {
        GroupIdentifyEventProperties._FinalStage groupProperties = GroupIdentifyEventProperties.builder()
                .groupType(groupType)
                .groupKey(groupKey);
        if (properties != null) {
            groupProperties.properties(properties);
        }
        GroupIdentifyEvent groupIdentifyEvent = GroupIdentifyEvent.builder()
                .event("$groupidentify")
                .distinctId(String.format("%s_%s", groupType, groupKey))
                .properties(groupProperties.build())
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now().toString())
                .build();
        enqueue(Event.of(groupIdentifyEvent));
    }

    /**
     *
     * @param distinctId which uniquely identifies your user in your database. Must
     *                   not be null or empty.
     * @param url
     * @param properties
     */
    public void page(
            String distinctId,
            String url,
            Map<String, Object> properties) {
        Map<String, Object> propertyCopy = new HashMap<>();
        if (properties != null) {
            propertyCopy.putAll(properties);
        }
        propertyCopy.put("$current_url", url);
        PageViewEvent pageViewEvent = PageViewEvent.builder()
                .event("$pageview")
                .distinctId(distinctId)
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now().toString())
                .properties(propertyCopy)
                .build();
        enqueue(Event.of(pageViewEvent));
    }

    private void enqueue(Event event) {
        queue.enqueue(event);
    }

    private DecideFeatureFlagsResponse getDecide(
            String distinctId,
            Map<String, String> groups,
            Map<String, Object> groupProperties,
            Map<String, Object> personProperties) {
        DecideFeatureFlagsRequest decideFeatureFlagsRequest = DecideFeatureFlagsRequest.builder()
                .v("3")
                .distinctId(distinctId)
                .groups(groups)
                .groupProperties(Optional.ofNullable(groupProperties))
                .personProperties(Optional.ofNullable(personProperties))
                .build();
        return posthogApiClient.featureFlags().decide(decideFeatureFlagsRequest);
    }
}
