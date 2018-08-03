package org.apache.metron.parsers.standalone;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.metron.parsers.BasicParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class StandaloneParser extends BasicParser {
  @Override
  public void init() {

  }

  @Override
  public List<JSONObject> parse(byte[] bytes) {
    String input = new String(bytes);
    JSONObject ret = null;
    try {
      ret = (JSONObject) new JSONParser().parse(input);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    ret.put("original_string", input);
    ret.put("timestamp", System.currentTimeMillis());
    return ImmutableList.of(ret);
  }

  @Override
  public void configure(Map<String, Object> map) {

  }
}