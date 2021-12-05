package it.tdlight.reactiveapi;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.primitive.serialization.SerializationService;
import it.tdlight.common.ConstructorDetector;
import it.tdlight.jni.TdApi;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.stream.Stream;
import org.apache.commons.lang3.SerializationException;

public class TdSerializer extends Serializer<TdApi.Object> {

	private TdSerializer() {

	}

	public static void register(SerializationService serializationService) {
		var serializerBuilder = serializationService.newBuilder("TdApi");
		var tdApiClasses = TdApi.class.getDeclaredClasses();
		// Add types
		Class<?>[] classes = Stream
				.of(tdApiClasses)
				.filter(clazz -> clazz.isAssignableFrom(TdApi.Object.class))
				.toArray(Class<?>[]::new);

		serializerBuilder.addSerializer(new TdSerializer(), classes);
	}

	@Override
	public void write(Kryo kryo, Output output, TdApi.Object object) {
		try {
			object.serialize(new KryoDataOutput(output));
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public TdApi.Object read(Kryo kryo, Input input, Class<TdApi.Object> type) {
		try {
			return TdApi.Deserializer.deserialize(new KryoDataInput(input));
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	public static TdApi.Object deserializeBytes(byte[] bytes) {
		var din = new DataInputStream(new ByteArrayInputStream(bytes));
		try {
			return TdApi.Deserializer.deserialize(din);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}
}
