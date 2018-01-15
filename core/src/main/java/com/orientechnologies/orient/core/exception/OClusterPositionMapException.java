package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.impl.local.paginated.cluster.OClusterPositionMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OClusterPositionMapException extends ODurableComponentException {
  public OClusterPositionMapException(OClusterPositionMapException exception) {
    super(exception);
  }

  public OClusterPositionMapException(String message, OClusterPositionMap component) {
    super(message, component);
  }
}
