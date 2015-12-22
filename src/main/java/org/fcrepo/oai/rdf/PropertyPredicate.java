/*
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.oai.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import java.util.function.Predicate;

/**
 * The type Property predicate.
 *
 * @author Frank Asseg
 */
public class PropertyPredicate implements Predicate<Triple> {

    private final String property;

    /**
     * Instantiates a new Property predicate.
     *
     * @param property the property
     */
    public PropertyPredicate(final String property) {
        this.property = property;
    }

    @Override
    public boolean test(final Triple triple) {
        final Node node = triple.getPredicate();
        return node.getURI().equals(property);
    }
}
