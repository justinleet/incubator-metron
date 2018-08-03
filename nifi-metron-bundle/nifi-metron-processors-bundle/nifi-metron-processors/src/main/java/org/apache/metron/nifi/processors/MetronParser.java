/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.nifi.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.curator.framework.CuratorFramework;
import org.apache.metron.common.configuration.ParserConfigurations;
import org.apache.metron.common.configuration.SensorParserConfig;
import org.apache.metron.nifi.zookeeper.MetronZkControllerService;
import org.apache.metron.parsers.interfaces.MessageParser;
import org.apache.metron.zookeeper.ZKCache;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

@Tags({"metron", "parser", "cybersecurity"})
@CapabilityDescription("Provides a Record based interface to allow use of Metron parsers within NiFi. This allows parsers to be run against FlowFile sources instead of Kafka sources. It should primarily be used for routing messages at the edge, while full Metron topologies should be used for high volume sources.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class MetronParser extends AbstractProcessor {

  public static final PropertyDescriptor PARSER_CLASS_NAME = new PropertyDescriptor.Builder()
      .name("PARSER_CLASS_NAME").displayName("Parser Class Name")
      .description("The fully qualified name of the parser class in Metron")
      .required(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  public static final PropertyDescriptor PARSER_CONFIG_NAME = new PropertyDescriptor.Builder()
      .name("PARSER_CONFIG_NAME").displayName("Parser Config Name")
      .description("The name of the parser config in Metron")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  public static final PropertyDescriptor PARSER_CONFIG = new PropertyDescriptor.Builder()
      .name("PARSER_CONFIG")
      .displayName("Parser Config")
      .description(
          "The full parser config for the Metron Parser (overrides version loaded via Parser Config Name)")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  public static final PropertyDescriptor PARSERS = new PropertyDescriptor.Builder()
      .name("metron-custom-parsers")
      .displayName("Custom Parsers Directory")
      .description(
          "Comma-separated list of paths to files and/or directories which contain modules containing custom parsers (that are not included on NiFi's classpath).")
      .required(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  public static final PropertyDescriptor ZK_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
      .name("ZK Controller Service")
      .description("Specified the ZK Controller Service used to manage Metron configs")
      .required(false)
      .identifiesControllerService(MetronZkControllerService.class)
      .build();

  public static final Relationship SUCCESS = new Relationship.Builder().name("Success")
      .description("Successfully parsed records").build();
  public static final Relationship FAILED = new Relationship.Builder().name("Failed")
      .description("Records Failed to Parse").build();
  public static final Relationship INVALID = new Relationship.Builder().name("Invalid")
      .description("Records Parsed, but did not pass validation").build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private volatile ClassLoader customClassLoader;
  private volatile MessageParser<JSONObject> parser;

  private volatile String zookeeperUrl;

  protected CuratorFramework client;
  protected ZKCache cache;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
//    descriptors.add(ZOOKEEPER);
    descriptors.add(PARSER_CLASS_NAME);
    descriptors.add(PARSER_CONFIG_NAME);
    descriptors.add(PARSER_CONFIG);
    descriptors.add(PARSERS);
    descriptors.add(ZK_CONTROLLER_SERVICE);

    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    relationships.add(FAILED);
    relationships.add(INVALID);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  /**
   * Custom validation since we must have either a full JSON config in the
   * processor, or a Metron connection string and a parse name to pull and listen
   * to the config from zookeeper.
   */
  protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
    final Collection<ValidationResult> results = new ArrayList<>();
//    if (validationContext.getProperty(PARSER_CONFIG_NAME).isSet()) {
//    } else {
//       validate the parser config JSON is valid Metron Parser configuration
//      validationContext.getProperty(PARSER_CONFIG).getValue();
//    }

    final String modulePath = validationContext.getProperty(PARSERS).getValue();
    getLogger().error("Module Path: " + modulePath);
    final ClassLoader customClassLoader;
    try {
      customClassLoader = ClassLoaderUtils
          .getCustomClassLoader(modulePath, this.getClass().getClassLoader(),
              getJarFilenameFilter());
      getLogger().error("Using custom class loader from: " + modulePath);
      String parserName = validationContext.getProperty(PARSER_CLASS_NAME).getValue();
      customClassLoader.loadClass(parserName);
    } catch (final Exception e) {
      getLogger().info("Processor is not valid - " + e.toString());
      String message = "Specification not valid for the selected parser.";
      results.add(new ValidationResult.Builder().valid(false)
          .explanation(message)
          .build());
    }

    return results;
  }

  protected FilenameFilter getJarFilenameFilter() {
    return (dir, name) -> (name != null && name.endsWith(".jar"));
  }


  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    try {
      if (context.getProperty(PARSERS).isSet()) {
        customClassLoader = ClassLoaderUtils.getCustomClassLoader(
            context.getProperty(PARSERS).getValue(),
            this.getClass().getClassLoader(),
            getJarFilenameFilter()
        );
      } else {
        customClassLoader = this.getClass().getClassLoader();
      }
    } catch (final Exception ex) {
      getLogger().error("Unable to setup processor", ex);
    }

    try {
      String className = context.getProperty(PARSER_CLASS_NAME).getValue();
      getLogger().error("className is: " + className);
      Map<String, Object> config = new HashMap<>();
      if (context.getProperty(PARSER_CONFIG).isSet()) {
        config = (Map<String, Object>) new JSONParser()
            .parse(context.getProperty(PARSER_CONFIG).getValue());
      } else if (context.getProperty(ZK_CONTROLLER_SERVICE).isSet()) {
        final MetronZkControllerService zkControllerService = context
            .getProperty(ZK_CONTROLLER_SERVICE)
            .asControllerService(MetronZkControllerService.class);
        String configName = context.getProperty(PARSER_CONFIG_NAME).getValue();
        ParserConfigurations configurations = zkControllerService
            .getConfigurations();
        if (configurations == null) {
          getLogger().error("NULL CONFIGURATIONS FROM ZK");
        } else {
          getLogger().error("Hey, we got configurations!");
        }

        SensorParserConfig sensorParserConfig = configurations
            .getSensorParserConfig(configName);
        if (sensorParserConfig == null) {
          getLogger().error("NULL SENSOR PARSER CONFIGURATIONS FROM ZK for: " + configName);
        } else {
          getLogger().error("Hey, we got sensor parser configs!");
        }
        config = sensorParserConfig.getParserConfig();
        if (sensorParserConfig == null) {
          getLogger().error("NULL PARSER CONFIGURATIONS FROM ZK");
        } else {
          getLogger().error("Hey, we got parser configs!");
        }
      }

      Class<?> parserClass = customClassLoader.loadClass(className);
      Object parserRawInstance = parserClass.newInstance();
      if (parserRawInstance instanceof MessageParser<?>) {
        parser = (MessageParser<JSONObject>) parserRawInstance;
        parser.configure(config); // Actually get config here
        parser.init();
      } else {
        // Do some useful logging or error handling or nifi magic
        getLogger()
            .error("Provided class is not a Metron MessageParser: " + parserRawInstance.getClass());
      }
    } catch (Exception e) {
      getLogger().error("onScheduled Error", e);
    }
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {
    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();

    session.write(flowFile, (in, out) -> {
      try (InputStreamReader inReader = new InputStreamReader(in);
          BufferedReader reader = new BufferedReader(inReader);
          OutputStreamWriter outWriter = new OutputStreamWriter(out);
          BufferedWriter writer = new BufferedWriter(outWriter)) {

        String line = reader.readLine();
        while (line != null) {
          try {
            // Swap our classloader for parsing.
            Thread.currentThread().setContextClassLoader(customClassLoader);
            getLogger().error("Passing into parser: " + line);
            getLogger().error("Parser: " + parser);
            Optional<List<JSONObject>> messagesOptional = parser
                .parseOptional(line.getBytes());
            List<JSONObject> messages = messagesOptional.orElse(new ArrayList<>());
            getLogger().error("Have messages: " + messages);
            for (int i = 0; i < messages.size(); i++) {
              JSONObject message = messages.get(i);
              getLogger().error("Operating on message: " + message);
              writer.write(message.toJSONString());
              if (i == messages.size() - 1 || reader.ready()) {
                // Always write a newline if there are more messages or if there are more lines
                // If we're at the last message in the group and there's nothing else in the buffer, don't write an empty line
                writer.newLine();
              }
            }
            line = reader.readLine();
          } catch (Exception ex) {
            getLogger().error("Trigger Error", ex);
            getLogger().error(Arrays.toString(ex.getStackTrace()));
          } finally {
            // Make sure to use the original classloader for writing.
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
          }
        }
      } catch (Exception ex) {
        session.transfer(flowFile, FAILED);
      }
    });

    session.transfer(flowFile, SUCCESS);
  }
}
