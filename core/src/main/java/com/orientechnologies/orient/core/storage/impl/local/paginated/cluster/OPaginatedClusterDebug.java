package com.orientechnologies.orient.core.storage.impl.local.paginated.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPageDebug;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class OPaginatedClusterDebug {
  public long                    clusterPosition;
  public List<OClusterPageDebug> pages;
  public boolean                 empty;
  public int                     contentSize;
  public long                    fileId;

}
