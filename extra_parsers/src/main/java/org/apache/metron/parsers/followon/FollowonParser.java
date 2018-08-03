package org.apache.metron.parsers.followon;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.metron.parsers.BasicParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FollowonParser extends BasicParser {

  @Override
  public void init() {

  }

  @Override
  public List<JSONObject> parse(byte[] bytes) {
    String input = new String(bytes);
    JSONObject ret = new JSONObject();
    try {
      ret = (JSONObject) new JSONParser().parse(input);
      ret.put("follow-on", UUID.randomUUID().toString());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return Collections.singletonList(ret);
  }

  @Override
  public void configure(Map<String, Object> map) {

  }
}