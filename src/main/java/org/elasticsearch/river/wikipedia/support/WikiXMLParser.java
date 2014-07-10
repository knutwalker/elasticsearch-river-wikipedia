/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.wikipedia.support;

import org.elasticsearch.river.wikipedia.bzip2.CBZip2InputStream;
import org.xml.sax.InputSource;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * @author Delip Rao
 * @author Jason Smith
 */
public abstract class WikiXMLParser {

    private final URL wikiXMLFile;
    private WikiPage currentPage;
    private final Map<String, Pattern> parsingRegxDefinitions;

    protected WikiXMLParser(final URL fileName, final Map<String, Pattern> parsingDefinitions) {
      wikiXMLFile = fileName;
      parsingRegxDefinitions = Collections.unmodifiableMap(parsingDefinitions);
    }

    /**
     * Set a callback handler. The callback is executed every time a
     * page instance is detected in the stream. Custom handlers are
     * implementations of {@link PageCallbackHandler}
     *
     * @param handler
     * @throws Exception
     */
    public abstract void setPageCallback(PageCallbackHandler handler) throws Exception;

    /**
     * The main parse method.
     *
     * @throws Exception
     */
    public abstract void parse() throws Exception;

    /**
     * @return an iterator to the list of pages
     * @throws Exception
     */
    public abstract WikiPageIterator getIterator() throws Exception;

    /**
     * @return An InputSource created from wikiXMLFile
     * @throws Exception
     */
    protected InputSource getInputSource() throws Exception {
        final BufferedReader br;

        if (wikiXMLFile.toExternalForm().endsWith(".gz")) {
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(wikiXMLFile.openStream()), "UTF-8"));
        } else if (wikiXMLFile.toExternalForm().endsWith(".bz2")) {
            final InputStream fis = wikiXMLFile.openStream();
            final byte[] ignoreBytes = new byte[2];
            fis.read(ignoreBytes); //"B", "Z" bytes from commandline tools
            br = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis), "UTF-8"));
        } else {
            br = new BufferedReader(new InputStreamReader(wikiXMLFile.openStream(), "UTF-8"));
        }

        return new InputSource(br);
    }

    protected final void notifyPage(final WikiPage page) {
      currentPage = page;
    }

  public final Map<String, Pattern> getParsingRegxDefinitions() {
    return Collections.unmodifiableMap(parsingRegxDefinitions);
  }
}
