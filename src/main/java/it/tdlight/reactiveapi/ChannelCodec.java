package it.tdlight.reactiveapi;

import java.lang.reflect.InvocationTargetException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ChannelCodec {
	public static final ChannelCodec CLIENT_BOUND_EVENT = new ChannelCodec(ClientBoundEventSerializer.class, ClientBoundEventDeserializer.class);
	public static final ChannelCodec TDLIB_REQUEST = new ChannelCodec(TdlibRequestSerializer.class, TdlibRequestDeserializer.class);
	public static final ChannelCodec TDLIB_RESPONSE = new ChannelCodec(TdlibResponseSerializer.class, TdlibResponseDeserializer.class);
	public static final ChannelCodec UTF8_TEST = new ChannelCodec(UtfCodec.class, UtfCodec.class);

	private final Class<?> serializerClass;
	private final Class<?> deserializerClass;

	public ChannelCodec(Class<?> serializerClass,
			Class<?> deserializerClass) {
		this.serializerClass = serializerClass;
		this.deserializerClass = deserializerClass;
	}

	public Class<?> getSerializerClass() {
		return serializerClass;
	}

	public Class<?> getDeserializerClass() {
		return deserializerClass;
	}

	public <K> Deserializer<K> getNewDeserializer() {
		try {
			//noinspection unchecked
			return (Deserializer<K>) deserializerClass.getDeclaredConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
						 ClassCastException e) {
			throw new IllegalStateException("Can't instantiate the codec deserializer", e);
		}
	}

	public <K> Serializer<K> getNewSerializer() {
		try {
			//noinspection unchecked
			return (Serializer<K>) serializerClass.getDeclaredConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
						 ClassCastException e) {
			throw new IllegalStateException("Can't instantiate the codec serializer", e);
		}
	}
}
