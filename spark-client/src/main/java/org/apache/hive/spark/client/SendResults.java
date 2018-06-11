package org.apache.hive.spark.client;

import java.io.Serializable;
import java.util.List;

public class SendResults implements Serializable {

  String[] res;

  SendResults() {
    this(null);
  }

  public SendResults(String[] res) {
    this.res = res;
  }
}
