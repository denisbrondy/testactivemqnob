/**
 *-------------------------------------------------------------------------
 * Copyright 2018 (C) by Thales Alenia Space France - all rights reserved
 *-------------------------------------------------------------------------
 */
package com.test;

/**
 * @author T0128980
 */
public class MockServiceException extends Exception
{

  /**
   * @param pMessage
   * @param pCause
   */
  public MockServiceException(String pMessage, Throwable pCause)
  {
    super(pMessage, pCause);
  }

}
