package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.Event.ClientBoundEvent;

public record TimestampedClientBoundEvent(long timestamp, ClientBoundEvent event) {}
