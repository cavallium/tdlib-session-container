package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.ResultingEvent.TDLibBoundResultingEvent;
import java.io.DataOutput;
import java.io.IOException;

public class TdlibBoundResultingEventSerializer implements Serializer<TDLibBoundResultingEvent<?>> {

	@Override
	public void serialize(TDLibBoundResultingEvent<?> data, DataOutput output) throws IOException {
		data.action().serialize(output);
		output.writeBoolean(data.ignoreFailure());
	}
}
