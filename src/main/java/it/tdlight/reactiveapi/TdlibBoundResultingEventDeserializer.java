package it.tdlight.reactiveapi;

import it.tdlight.jni.TdApi;
import it.tdlight.jni.TdApi.Function;
import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import java.io.DataInput;
import java.io.IOException;

public class TdlibBoundResultingEventDeserializer implements Deserializer<TDLibBoundResultingEvent<?>> {

	@Override
	public TDLibBoundResultingEvent<?> deserialize(int length, DataInput dataInput) throws IOException {
		Function<?> action = (Function<?>) TdApi.Deserializer.deserialize(dataInput);
		return new TDLibBoundResultingEvent<>(action, dataInput.readBoolean());
	}
}
