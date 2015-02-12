/**
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
package org.fcrepo.oai.jersey;

import java.io.IOException;
import java.io.Writer;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2_0.oai_dc.OaiDcType;

import com.sun.xml.bind.marshaller.CharacterEscapeHandler;

/**
 * The type Oai jaxb provider.
 *
 * @author Frank Asseg
 */
@Provider
public class OaiJaxbProvider implements ContextResolver<Marshaller> {

    private final Marshaller marshaller;

    /**
     * Instantiates a new Oai jaxb provider.
     *
     * @throws JAXBException the jAXB exception
     */
    public OaiJaxbProvider() throws JAXBException {
        this.marshaller = JAXBContext.newInstance(OaiDcType.class, OAIPMHtype.class).createMarshaller();
        this.marshaller.setProperty("com.sun.xml.bind.marshaller.CharacterEscapeHandler", new CharacterEscapeHandler() {
            @Override
            public void escape(final char[] chars, final int start, final int len, final boolean isAttr,
                    final Writer writer) throws IOException {
                final StringBuilder data = new StringBuilder(len);
                for (int i = start; i < len + start; i++) {
                    if (chars[i] == '&') {
                        data.append("&amp;");
                    } else {
                        data.append(chars[i]);
                    }
                }
                writer.write(data.toString());
            }
        });
    }

    @Override
    public Marshaller getContext(final Class<?> aClass) {
        if (aClass == OAIPMHtype.class) {
            return this.marshaller;
        }
        return null;
    }
}
