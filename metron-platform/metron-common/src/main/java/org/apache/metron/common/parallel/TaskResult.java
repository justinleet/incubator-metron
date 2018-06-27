package org.apache.metron.common.parallel;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.json.simple.JSONObject;

/**
 * The result of a task, e.g. enrichment or parsing
 */
public class TaskResult {

  private JSONObject result;
  private List<Entry<Object, Throwable>> taskErrors;

  public TaskResult(JSONObject result, List<Map.Entry<Object, Throwable>> taskErrors) {
    this.result = result;
    this.taskErrors = taskErrors;
  }

  /**
   * The unified task result.
   * @return
   */
  public JSONObject getResult() {
    return result;
  }

  /**
   * The errors that happened in the course of the task.
   * @return
   */
  public List<Map.Entry<Object, Throwable>> getTaskErrors() {
    return taskErrors;
  }
}
