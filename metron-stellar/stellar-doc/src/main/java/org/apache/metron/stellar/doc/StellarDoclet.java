/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.metron.stellar.doc;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationDesc.ElementValuePair;
import com.sun.javadoc.AnnotationTypeElementDoc;
import com.sun.javadoc.AnnotationValue;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class StellarDoclet extends Standard {

  public static boolean start(RootDoc root) {
    writeContents(root.classes());
    return true;
  }

  protected static void writeContents(ClassDoc[] classes) {
    String currentPath = Paths.get("").toAbsolutePath().toString();
    // Immediately return if we're running over test Stellar functions
    if (currentPath.contains("testapidocs")) {
      return;
    }

    // Want to move up a few directories, to drop this somewhere outside of /target
    String baseDirectory = new File(currentPath).getParentFile().getParentFile().getParent();

    File file = new File(baseDirectory + "/STELLARDOC_README.md");
    Charset charset = StandardCharsets.UTF_8;
    try {
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      // Do nothing
    }

    try (BufferedWriter writer = Files.newBufferedWriter(file.toPath(), charset)) {
      for (ClassDoc classDoc : classes) {
        AnnotationDesc[] annotations = classDoc.annotations();
        for (AnnotationDesc desc : annotations) {
          if ("org.apache.metron.stellar.dsl.Stellar".equals(desc.annotationType().toString())) {
            ElementValuePair[] elementValuePairs = desc.elementValues();
            Map<String, Object> elementMap = elementMap(elementValuePairs);
            writer.append("\n### `");
            Object namespace = elementMap.get("namespace");
            if (namespace != null) {
              writer.append(namespace.toString().replace("\"", "")).append('_');
            }
            writer.append(elementMap.get("name").toString().replace("\"", "")).append('`');
            writer.append("\n  * Description: ").append(elementMap.get("description").toString());
            Object params = elementMap.get("params");
            if (params instanceof AnnotationValue) {
              if (!"{}".equals(params.toString())) {
                writer.append("\n  * Input:");
                writer.append("\n    * ").append(params.toString());
              }
            }

            Object returnElement = elementMap.get("return");
            if (returnElement != null) {
              writer.append("\n  * Returns: ").append(returnElement.toString());
            }
            writer.append("\n");
          }
        }
      }
    } catch (IOException x) {
      System.err.format("IOException: %s%n", x);
    }
  }

  private static Map<String, Object> elementMap(ElementValuePair[] elementValuePairs) {
    Map<String, Object> elementMap = new HashMap<>();
    for (ElementValuePair elementValuePair : elementValuePairs) {
      String elementStr = formatElement(elementValuePair.element());
      elementMap.put(elementStr, elementValuePair.value());
    }
    return elementMap;
  }

  private static String formatElement(AnnotationTypeElementDoc element) {
    String[] splits = element.toString().split("\\.");
    String elementName = splits[splits.length - 1];
    // Drop the last two characters, which will be '()'
    return elementName.substring(0, elementName.length() - 2);
  }

  public static LanguageVersion languageVersion() {
    return LanguageVersion.JAVA_1_5;
  }
}
