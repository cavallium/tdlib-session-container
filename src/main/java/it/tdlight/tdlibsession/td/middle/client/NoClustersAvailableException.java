package it.tdlight.tdlibsession.td.middle.client;

public class NoClustersAvailableException extends Throwable {

	public NoClustersAvailableException(String error) {
		super(error);
	}

	@Override
	public String toString() {
		return "No clusters are available. " + this.getMessage();
	}
}
