package org.apache.hive.spark.client;

import java.io.Serializable;
import java.util.List;

public class SendResults implements Serializable {

  String[] res;
  boolean result;

  SendResults() {
    this(null, false);
  }

  public SendResults(String[] res, boolean result) {
    this.res = res;
    this.result = result;
  }
}
