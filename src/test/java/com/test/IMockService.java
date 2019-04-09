/**
 *-------------------------------------------------------------------------
 * Copyright 2018 (C) by Thales Alenia Space France - all rights reserved
 *-------------------------------------------------------------------------
 */
package com.test;

/**
 * @author T0128980
 */
public interface IMockService
{
  void call() throws MockServiceException;
  
  void call(String message) throws MockServiceException;
  
}
