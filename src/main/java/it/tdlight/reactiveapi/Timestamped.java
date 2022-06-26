package it.tdlight.reactiveapi;

public record Timestamped<T>(long timestamp, T data) {}
