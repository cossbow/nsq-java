package com.cossbow.nsq.exceptions;

public class BadTopicException extends NSQException {
	private static final long serialVersionUID = -1081661174848274583L;

	public BadTopicException(String message) {
		super(message);
	}
}
