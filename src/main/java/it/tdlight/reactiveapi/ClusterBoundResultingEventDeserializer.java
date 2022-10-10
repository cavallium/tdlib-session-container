package it.tdlight.reactiveapi;

import it.tdlight.reactiveapi.ResultingEvent.ClusterBoundResultingEvent;
import it.tdlight.reactiveapi.ResultingEvent.ResultingEventPublisherClosed;
import java.io.DataInput;
import java.io.IOException;

public class ClusterBoundResultingEventDeserializer implements Deserializer<ClusterBoundResultingEvent> {

	@Override
	public ClusterBoundResultingEvent deserialize(int length, DataInput dataInput) throws IOException {
		var type = dataInput.readByte();
		return switch (type) {
			case 0 -> new ResultingEventPublisherClosed();
			default -> throw new UnsupportedOperationException("Unsupported type: " + type);
		};
	}
}
