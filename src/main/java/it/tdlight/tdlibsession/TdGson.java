package it.tdlight.tdlibsession;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import it.tdlight.jni.TdApi;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;

public class TdGson {

	private static final TdApiGenericSerializer<?> tdApiGenericSerializerInstance = new TdApiGenericSerializer<>();
	private static final ArrayList<Class<?>> abstractClassesSerializers = new ArrayList<>();

	static {
		for (Class<?> declaredClass : TdApi.class.getDeclaredClasses()) {
			var modifiers = declaredClass.getModifiers();
			if (Modifier.isAbstract(modifiers) && Modifier.isPublic(modifiers) && Modifier
					.isStatic(modifiers)) {
				abstractClassesSerializers.add(declaredClass);
			}
		}
	}

	public static GsonBuilder registerAdapters(GsonBuilder gsonBuilder) {
		for (Class<?> abstractClassesSerializer : abstractClassesSerializers) {
			gsonBuilder.registerTypeAdapter(abstractClassesSerializer, tdApiGenericSerializerInstance);
		}
		return gsonBuilder;
	}

	public static class TdApiGenericSerializer<T> implements JsonSerializer<T>, JsonDeserializer<T> {

		@Override
		public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject result = new JsonObject();
			result.add("type", new JsonPrimitive(src.getClass().getSimpleName()));
			result.add("properties", context.serialize(src, src.getClass()));

			return result;
		}

		@Override
		public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			JsonObject jsonObject = json.getAsJsonObject();
			String type = jsonObject.get("type").getAsString().replaceAll("[^a-zA-Z0-9]", "");
			JsonElement element = jsonObject.get("properties");

			try {
				return context
						.deserialize(element, Class.forName(TdApi.class.getCanonicalName() + "$" + type));
			} catch (ClassNotFoundException cnfe) {
				throw new JsonParseException("Unknown element type: " + type, cnfe);
			}
		}
	}
}