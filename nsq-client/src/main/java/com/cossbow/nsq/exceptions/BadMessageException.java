package com.cossbow.nsq.exceptions;

public class BadMessageException extends NSQException {
	private static final long serialVersionUID = 2668504738938186790L;

	public BadMessageException(String message) {
		super(message);
	}
}
