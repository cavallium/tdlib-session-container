package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;

/**
 * {@link #sessionUuid} changes every time a session is restarted
 */
public record ReactiveApiUpdate(long sessionUuid, TdApi.Object update) {}
