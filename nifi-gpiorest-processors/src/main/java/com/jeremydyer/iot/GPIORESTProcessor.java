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
package com.jeremydyer.iot;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"gpio"})
@CapabilityDescription("Raspberry PI GPIO REST actuator. Provides the ability to turn individual GPIO pins on or off.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class GPIORESTProcessor extends AbstractProcessor {


    public static final PropertyDescriptor PROP_SWITCH_DELAY_THRESHOLD = new PropertyDescriptor
            .Builder().name("Switch Delay Threshold")
            .description("Second delay between switching from on state to the other. This prevents the outlet from 'flickering' on/off with minor face detections")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();


    public static final Relationship REL_TURN_ON = new Relationship.Builder()
            .name("turn on")
            .description("Turn on GPIO Outlet")
            .build();

    public static final Relationship REL_TURN_OFF = new Relationship.Builder()
            .name("turn off")
            .description("Turn off GPIO Outlet")
            .build();

    public static final Relationship REL_NO_CHANGE = new Relationship.Builder()
            .name("No Change")
            .description("No need to change the outlet state")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to contact the RPI device")
            .build();

    final AtomicReference<Boolean> CURRENTLY_ON = new AtomicReference<>();
    final AtomicReference<Long> LAST_SWITCH_TS = new AtomicReference<>();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        CURRENTLY_ON.set(false);
        LAST_SWITCH_TS.set(System.currentTimeMillis());
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_SWITCH_DELAY_THRESHOLD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_TURN_ON);
        relationships.add(REL_TURN_OFF);
        relationships.add(REL_NO_CHANGE);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ProcessorLog log = this.getLogger();
        final AtomicReference<Integer> value = new AtomicReference<>();

        final FlowFile flowfile = session.get();
        final Integer delayThreshold = context.getProperty(PROP_SWITCH_DELAY_THRESHOLD).asInteger();

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    String json = IOUtils.toString(in);
                    int numFaces = JsonPath.read(json, "$.faces");
                    value.set(numFaces);
                }catch(Exception ex){
                    getLogger().error("Failed to process face detection context", ex);
                    session.transfer(flowfile, REL_FAILURE);
                }
            }
        });

        // Write the results to an attribute
        int numFaces = value.get();

        if (CURRENTLY_ON.get()) {
            //Outlet is on. If no faces detected turn off. Else do nothing
            if (numFaces == 0 && (((System.currentTimeMillis() - LAST_SWITCH_TS.get()) / 1000) > delayThreshold)) {
                CURRENTLY_ON.set(false);
                LAST_SWITCH_TS.set(System.currentTimeMillis());
                session.transfer(flowfile, REL_TURN_OFF);
            } else {
                session.transfer(flowfile, REL_NO_CHANGE);
            }
        } else {
            //Outlet is currently turned off
            if (numFaces > 0 && (((System.currentTimeMillis() - LAST_SWITCH_TS.get()) / 1000) > delayThreshold)) {
                CURRENTLY_ON.set(true);
                LAST_SWITCH_TS.set(System.currentTimeMillis());
                session.transfer(flowfile, REL_TURN_ON);
            } else {
                session.transfer(flowfile, REL_NO_CHANGE);
            }
        }

    }

}