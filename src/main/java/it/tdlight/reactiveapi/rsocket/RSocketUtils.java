package it.tdlight.reactiveapi.rsocket;

import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import reactor.core.publisher.Flux;

public class RSocketUtils {

	public static <T> Flux<T> deserialize(Flux<Payload> payloadFlux, Deserializer<T> deserializer) {
		return payloadFlux.map(payload -> {
			var slice = payload.sliceData();
			byte[] bytes = new byte[slice.readableBytes()];
			slice.readBytes(bytes, 0, bytes.length);
			return deserializer.deserialize(null, bytes);
		});
	}

	public static <T> Flux<Payload> serialize(Flux<T> flux, Serializer<T> serializer) {
		return flux.map(element -> DefaultPayload.create(serializer.serialize(null, element)));
	}
}
