package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.ResultingEvent.ClusterBoundResultingEvent;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterBoundResultingEventSerializer implements Serializer<ClusterBoundResultingEvent> {

	@Override
	public void serialize(ClusterBoundResultingEvent data, DataOutput output) throws IOException {
		if (data instanceof ResultingEvent.ResultingEventPublisherClosed) {
			output.writeByte(0x0);
		} else {
			throw new UnsupportedOperationException("Unsupported event: " + data);
		}
	}
}
