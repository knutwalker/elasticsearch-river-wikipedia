/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *//*
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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.river.wikipedia.WikipediaRiver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For internal use only -- Used by the {@link WikiPage} class.
 * Can also be used as a stand alone class to parse wiki formatted text.
 *
 * @author Delip Rao
 */
public final class WikiTextParser {

    private final String wikiText;
    private List<String> pageCats;
    private List<String> pageLinks;
    private boolean redirect;
    private String redirectString;
    private boolean stub;
    private boolean disambiguation;
    private InfoBox infoBox;

    private final Pattern categoryPattern;
    private final Pattern linkPattern;

    public WikiTextParser(final String wtext, final Map<String, Pattern> regexPatterns) {
        wikiText = wtext;
        // set the parsing pattern
        categoryPattern = regexPatterns.get(WikipediaRiver.CATEGORY_REGEX_KEY);
        linkPattern = regexPatterns.get(WikipediaRiver.LINK_REGEX_KEY);

        final Pattern redirectPattern = regexPatterns.get(WikipediaRiver.REDIRECT_REGEX_KEY);
        final Pattern disambCatPattern = regexPatterns.get(WikipediaRiver.DISAMBIGUATION_REGEX_KEY);
        final Pattern stubPattern = regexPatterns.get(WikipediaRiver.STUB_REGEX_KEY);

        final Matcher matcher = redirectPattern.matcher(wikiText);
        if (matcher.find()) {
            redirect = true;
            if (matcher.groupCount() == 1){
              redirectString = matcher.group(1);
            }
        }
        final Matcher stubMatcher = stubPattern.matcher(wikiText);
        stub = stubMatcher.find();
        final Matcher disambiMatcher = disambCatPattern.matcher(wikiText);
        disambiguation = disambiMatcher.find();
    }

    public boolean isRedirect() {
        return redirect;
    }

    public boolean isStub() {
        return stub;
    }

    public String getRedirectText() {
        return redirectString;
    }

    public String getText() {
        return wikiText;
    }

    public List<String> getCategories() {
        if (pageCats == null) {
          parseCategories();
        }
        return Collections.unmodifiableList(pageCats);
    }

    public List<String> getLinks() {
        if (pageLinks == null) {
          parseLinks();
        }
        return Collections.unmodifiableList(pageLinks);
    }

    private void parseCategories() {
        pageCats = new ArrayList<String>();
        final Matcher matcher = categoryPattern.matcher(wikiText);
        while (matcher.find()) {
            final String[] temp = matcher.group(1).split("\\|");
            pageCats.add(temp[0]);
        }
    }

    private void parseLinks() {
        pageLinks = new ArrayList<String>();
        final Matcher matcher = linkPattern.matcher(wikiText);
        while (matcher.find()) {
            final String[] temp = matcher.group(1).split("\\|");
            if (temp == null || temp.length == 0) {
              continue;
            }
            final String link = temp[0];
            if (!link.contains(":")) {
                pageLinks.add(link);
            }
        }
    }

    public String getPlainText() {
        String text = wikiText.replaceAll("&gt;", ">");
        text = text.replaceAll("&lt;", "<");
        text = text.replaceAll("<ref>.*?</ref>", " ");
        text = text.replaceAll("</?.*?>", " ");
        text = text.replaceAll("\\{\\{.*?\\}\\}", " ");
        text = text.replaceAll("\\[\\[.*?:.*?\\]\\]", " ");
        text = text.replaceAll("\\[\\[(.*?)\\]\\]", "$1");
        text = text.replaceAll("\\s(.*?)\\|(\\w+\\s)", " $2");
        text = text.replaceAll("\\[.*?\\]", " ");
        text = text.replaceAll("'+", "");
        text = text.replaceAll("===(.*?)===", "$1");
        text = text.replaceAll("==(.*?)==", "$1");
        text = text.replaceAll("\\*", "");
        return text;
    }

    public InfoBox getInfoBox() {
        //parseInfoBox is expensive. Doing it only once like other parse* methods
        if (infoBox == null) {
          infoBox = parseInfoBox();
        }
        return infoBox;
    }

    @Nullable
    private InfoBox parseInfoBox() {
        final String INFOBOX_CONST_STR = "{{Infobox";
        final int startPos = wikiText.indexOf(INFOBOX_CONST_STR);
        if (startPos < 0) {
          return null;
        }
        int bracketCount = 2;
        int endPos = startPos + INFOBOX_CONST_STR.length();
        for (; endPos < wikiText.length(); endPos++) {
            switch (wikiText.charAt(endPos)) {
                case '}':
                    bracketCount--;
                    break;
                case '{':
                    bracketCount++;
                    break;
                default:
            }
            if (bracketCount == 0) {
              break;
            }
        }
        String infoBoxText = wikiText.substring(startPos, endPos + 1);
        infoBoxText = stripCite(infoBoxText); // strip clumsy {{cite}} tags
        // strip any html formatting
        infoBoxText = infoBoxText.replaceAll("&gt;", ">");
        infoBoxText = infoBoxText.replaceAll("&lt;", "<");
        infoBoxText = infoBoxText.replaceAll("<ref.*?>.*?</ref>", " ");
        infoBoxText = infoBoxText.replaceAll("</?.*?>", " ");
        return new InfoBox(infoBoxText);
    }

    private String stripCite(final String text) {
        final String CITE_CONST_STR = "{{cite";
        final int startPos = text.indexOf(CITE_CONST_STR);
        if (startPos < 0) {
            return text;
        }
        int bracketCount = 2;
        int endPos = startPos + CITE_CONST_STR.length();
        for (; endPos < text.length(); endPos++) {
            switch (text.charAt(endPos)) {
                case '}':
                    bracketCount--;
                    break;
                case '{':
                    bracketCount++;
                    break;
                default:
            }
            if (bracketCount == 0) {
                break;
            }
        }
        final String newText = text.substring(0, startPos - 1) + text.substring(endPos);
        return stripCite(newText);
    }

    public boolean isDisambiguationPage() {
        return disambiguation;
    }

    public String getTranslatedTitle(final String languageCode) {
        final Pattern pattern = Pattern.compile("^\\[\\[" + languageCode + ":(.*?)\\]\\]$", Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(wikiText);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}
