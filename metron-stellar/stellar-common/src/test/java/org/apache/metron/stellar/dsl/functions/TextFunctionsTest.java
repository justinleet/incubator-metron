/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.metron.stellar.dsl.functions;

import static org.apache.metron.stellar.common.utils.StellarProcessorUtils.run;
import static org.apache.metron.stellar.common.utils.StellarProcessorUtils.runPredicate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.metron.stellar.dsl.DefaultVariableResolver;
import org.apache.metron.stellar.dsl.ParseException;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class TextFunctionsTest {

  static final Map<String, String> variableMap = new HashMap<String, String>() {{
    put("metron", "metron");
    put("sentence", "metron is great");
    put("empty", "");
    put("english", "en");
    put("klingon", "Kling");
    put("asf", "Apache Software Foundation");
  }};

  @Test
  @SuppressWarnings("unchecked")
  public void testGetAvailableLanguageTags() {
    Object ret = run("FUZZY_LANGS()", new HashMap<>());
    Assert.assertNotNull(ret);
    Assert.assertTrue(ret instanceof List);
    List<String> tags = (List<String>) ret;
    Assert.assertTrue(tags.size() > 0);
    Assert.assertTrue(tags.contains("en"));
    Assert.assertTrue(tags.contains("fr"));
  }

  @Test()
  public void testNoMatchStrings() throws Exception {
    Assert.assertTrue(runPredicate("0 == FUZZY_SCORE(metron,'z',english)",
        new DefaultVariableResolver(v -> variableMap.get(v),
            v -> variableMap.containsKey(v))));
  }

  @Test(expected = ParseException.class)
  public void testMissingLanguage() throws Exception {
    runPredicate("0 == FUZZY_SCORE(metron,'z',klingon)",
        new DefaultVariableResolver(v -> variableMap.get(v),
            v -> variableMap.containsKey(v)));
  }

  @Test()
  public void testEmptyFirstArg() throws Exception {
    Assert.assertTrue(runPredicate("0 == FUZZY_SCORE(empty,'z',english)",
        new DefaultVariableResolver(v -> variableMap.get(v), v -> variableMap.containsKey(v))));
  }

  @Test()
  public void testEmptyFirstTwoArgs() throws Exception {
    Assert.assertTrue(runPredicate("0 == FUZZY_SCORE(empty,empty,english)",
        new DefaultVariableResolver(v -> variableMap.get(v),
            v -> variableMap.containsKey(v))));
  }

  @Test(expected = ParseException.class)
  public void testEmptyArgs() throws Exception {
    runPredicate("0 == FUZZY_SCORE(empty,empty,empty)",
        new DefaultVariableResolver(v -> variableMap.get(v), v -> variableMap.containsKey(v)));
  }

  @Test(expected = ParseException.class)
  public void testNoArgs() throws Exception {
    runPredicate("0 == FUZZY_SCORE()",
        new DefaultVariableResolver(v -> variableMap.get(v), v -> variableMap.containsKey(v)));
  }

  @Test
  public void testHappyStringFunctions() throws Exception {
    Assert
        .assertTrue(runPredicate("1 == FUZZY_SCORE(metron,'m',english)",
            new DefaultVariableResolver(v -> variableMap.get(v), v -> variableMap.containsKey(v))));
    Assert.assertTrue(
        runPredicate("16 == FUZZY_SCORE(metron,'metron',english)",
            new DefaultVariableResolver(v -> variableMap.get(v), v -> variableMap.containsKey(v))));
    Assert.assertTrue(runPredicate("3 == FUZZY_SCORE(asf,'asf',english)",
        new DefaultVariableResolver(v -> variableMap.get(v), v -> variableMap.containsKey(v))));
  }
}
