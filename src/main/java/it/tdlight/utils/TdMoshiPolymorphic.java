package it.tdlight.utils;

import com.squareup.moshi.JsonAdapter;
import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Object;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import org.warp.commonutils.moshi.MoshiPolymorphic;

public class TdMoshiPolymorphic extends MoshiPolymorphic<Object> {


	private final Set<Class<TdApi.Object>> abstractClasses = new HashSet<>();
	private final Set<Class<TdApi.Object>> concreteClasses = new HashSet<>();

	public TdMoshiPolymorphic() {
		super();
		var declaredClasses = TdApi.class.getDeclaredClasses();
		for (Class<?> declaredClass : declaredClasses) {
			var modifiers = declaredClass.getModifiers();
			if (Modifier.isPublic(modifiers) && Modifier
					.isStatic(modifiers)) {
				if (Modifier.isAbstract(modifiers)) {
					//noinspection unchecked
					this.abstractClasses.add((Class<TdApi.Object>) declaredClass);
				} else {
					//noinspection unchecked
					this.concreteClasses.add((Class<TdApi.Object>) declaredClass);
				}
			}
		}
	}

	@Override
	public Set<Class<TdApi.Object>> getAbstractClasses() {
		return abstractClasses;
	}

	@Override
	public Set<Class<TdApi.Object>> getConcreteClasses() {
		return concreteClasses;
	}

	@Override
	protected boolean shouldIgnoreField(String fieldName) {
		return fieldName.equals("CONSTRUCTOR");
	}
}
