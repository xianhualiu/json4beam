package com.google.json4beam.exceptions;

/** Exception class used in transforms service module */
public class TransformServiceException extends RuntimeException {

  public TransformServiceException() {}

  public TransformServiceException(String message) {
    super(message);
  }

  public TransformServiceException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransformServiceException(Throwable cause) {
    super(cause);
  }

  public TransformServiceException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
