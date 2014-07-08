package org.elasticsearch.river.wikipedia.support;

import org.elasticsearch.river.wikipedia.WikipediaRiver;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


public final class WikiTextParserTest {

  @Test
  public void shouldStripText() {

    final String input = "{{Infobox Gemeinde in Deutschland\n" +
        "|Wappen            = kein\n" +
        "|Breitengrad       = 54/21/14/N\n" +
        "|Längengrad        = 08/49/34/E\n" +
        "|Lageplan          = Tetenbuell in NF.PNG\n" +
        "|Bundesland        = Schleswig-Holstein\n" +
        "|Kreis             = Nordfriesland\n" +
        "|Amt               = Eiderstedt\n" +
        "|Höhe              = 2\n" +
        "|Fläche            = 36.48\n" +
        "|PLZ               = 25882\n" +
        "|Vorwahl           = 04862, 04864, 04865\n" +
        "|Kfz               = NF\n" +
        "|Gemeindeschlüssel = 01054135\n" +
        "|NUTS              = DEF07\n" +
        "|Adresse-Verband   = Welter Str. 1<br />25836 Garding \n" +
        "|Website           = [http://www.amt-eiderstedt.de/index.phtml?sNavID=1840.34 www.amt-eiderstedt.de]\n" +
        "|Bürgermeister     = Thomas Lorenzen\n" +
        "|Partei            = CDU\n" +
        "}}\n" +
        "\n" +
        "'''Tetenbüll''' (dänisch: Tetenbøl) ist eine Gemeinde im [[Amt Eiderstedt]] des [[Kreis Nordfriesland|Kreises Nordfriesland]] in [[Schleswig-Holstein]].\n" +
        "\n" +
        "== Geografie und Verkehr ==\n" +
        "Tetenbüll liegt etwa 8 km nordwestlich von [[Tönning]] und 12 km nordöstlich von [[St. Peter-Ording]] auf der Halbinsel [[Eiderstedt]]. Südlich verlaufen die [[Bundesstraße 5]] und die Bahnlinie von Tönning nach St. Peter-Ording. Kaltenhörn, Warmhörn und Wasserkoog liegen im Gemeindegebiet. Nordöstlich von Tetenbüll liegt der [[Adenbüller Koog]]. Zur Gemeinde gehören auch Teile des [[Norderheverkoog|Norderheverkooges]].\n" +
        "\n" +
        "== Kirche ==\n" +
        "Eine erste Kapelle wurde um 1113 erbaut; die heutige Kirche ''St.&nbsp;Anna'' entstand um 1400 nach der [[Eindeichung]] des Tetenbüller Kirchenkooges. Die Holzbalkendecke schmückt eine aus dem 18.&nbsp;Jahrhundert stammende Malerei, die den Weg Christi zeigt. An der Nordempore von 1612 sind dreißig Szenen aus dem [[Altes Testament|Alten Testament]] in Bildern dargestellt.<ref>Hans-Walter Wulf: ''Kirchen in Eiderstedt.'' Lühr & Dircks, St. Peter-Ording 1981, S. 60f.</ref>\n" +
        "\n" +
        "Tetenbüll bildet mit Katharinenheerd eine Kirchengemeinde.  \n" +
        "<gallery>\n" +
        "Datei:Tetenbüll St. Anna.JPG |Tetenbüll, St. Anna\n" +
        "Datei:St. Anna, Altar.jpg|St. Anna, holzgeschnitztes Altarbild\n" +
        "</gallery>\n" +
        "\n" +
        "== Politik ==\n" +
        "=== Gemeindevertretung ===\n" +
        "* Von den neun Sitzen in der Gemeindevertretung hatte die [[CDU]] seit der Kommunalwahl 2008 vier Sitze, die [[Wählergemeinschaft]] KWT hatte drei und die [[Bündnis 90/Die Grünen|Grünen]] kamen auf zwei Sitze.\n" +
        "* Bei den Kommunalwahlen am 26. Mai 2013 erhielt die CDU 43,6 % der abgegebenen Stimmen und erhielt vier Sitze. Die Grünen kamen auf 29,2 % und auf drei Sitze. Auf die Kommunale [[Wählergemeinschaft]] Tetenbüll (KWT) entfielen 27,3 % und zwei Sitze. Die Wahlbeteiligung betrug 63,27 Prozent.<ref>[wahlen.amt-eiderstedt.de/2013/kommunal/tetenbüll] abgerufen am 27. Mai 2013</ref>\n" +
        "=== Bürgermeister ===\n" +
        "Für die Wahlperiode 2013–2018 wurde Thomas Lorenzen (CDU) zum neuen Bürgermeister gewählt. Er folgte auf Henning Möller (CDU) der das Amt 23 Jahre innehatte.<ref Name=\"shz\"> [http://www.shz.de/nachrichten/lokales/husumer-nachrichten/artikeldetails/artikel/im-zweiten-anlauf-klappte-es.html Im zweiten Anlauf klappte es] Husumer Nachrichten vom 5. Juli 2013, abgerufen am 6. Juli 2013</ref>\n" +
        "\n" +
        "== Wirtschaft ==\n" +
        "Das Gemeindegebiet ist überwiegend landwirtschaftlich strukturiert, es gibt jedoch auch einige Gewerbeunternehmen und einen Hafen. Auch der Tourismus ist eine wichtige Einnahmequelle für die Gemeinde.\n" +
        "\n" +
        "== Sport ==\n" +
        "* Der Turn- und Sportverein (TuS) Tetenbüll e.V. von 1951 bietet die Sparten Volleyball, Turnen, Fußball, Badminton, Gymnastik, Tanzen und Floorball an.\n" +
        "* Der [[Ringreiten|Ringreiterverein]] bietet mit dem Reiten eine besondere Pferdesportart an. Mit einer Lanze ist im Galopp ein Ring unterschiedlicher Größe aufzuspießen.\n" +
        "* Das [[Boßeln]] wird von zwei Vereinen betrieben. Für die Mannslüüd (Männer) ist das der Boßelverein von 1894 e. V., für die Frauen gibt es den Fruunsboßelverein.\n" +
        "\n" +
        "== Öffentliche Einrichtungen ==\n" +
        "* In der Gemeinde sind ein Kindergarten und eine Grundschule vorhanden.\n" +
        "* Für den Brandschutz ist die 1934 gegründete Freiwillige Feuerwehr vorhanden. Sie ist gleichzeitig für das Gebiet der Gemeinde Katharinenheerd zuständig.\n" +
        "\n" +
        "== Söhne und Töchter der Gemeinde ==\n" +
        "* [[Johannes Nikolaus Tetens]] (1736–1807), deutscher Philosoph, Mathematiker und Naturforscher der [[Zeitalter der Aufklärung in der westlichen Staatenwelt|Aufklärung]]\n" +
        "* [[Wilhelm Hamkens]] (1896–1955), Führer der [[Landvolkbewegung (Schleswig-Holstein)|schleswig-holsteinischen Landvolkbewegung]]\n" +
        "\n" +
        "== Literatur ==\n" +
        "* Hans-Walter Wulf: ''Kirchen in Eiderstedt.'' Lühr & Dircks, St. Peter-Ording 1981, S. 60−64, ISBN 3-921416-13-2\n" +
        "\n" +
        "== Weblinks ==\n" +
        "{{Commonscat|Tetenbüll}}\n" +
        "* [http://www.amt-eiderstedt.de/index.phtml?sNavID=1840.34 Gemeinde Tetenbüll beim Amt Eiderstedt]\n" +
        "* [http://www.tetenbuell.de/ Ortskulturring Tetenbüll]\n" +
        "\n" +
        "== Einzelnachweise ==\n" +
        "<references />\n" +
        "\n" +
        "{{Navigationsleiste Städte und Gemeinden im Kreis Nordfriesland}}\n" +
        "{{Normdaten|TYP=g|GND=4678562-0|VIAF=234788893}}\n" +
        "\n" +
        "{{SORTIERUNG:Tetenbull}}\n" +
        "\n" +
        "[[Kategorie:Eiderstedt]]\n" +
        "[[Kategorie:Ort im Kreis Nordfriesland]]\n" +
        "[[Kategorie:Tetenbüll| ]]\n";

    final Pattern redirectPattern = Pattern.compile("#REDIRECT\\s+\\[\\[(.*?)\\]\\]", Pattern.CASE_INSENSITIVE);
    final Pattern disambCatPattern = Pattern.compile("\\{\\{[Dd]isambig(uation)?\\}\\}");
    final Pattern stubPattern = Pattern.compile("\\-stub\\}\\}");
    final Pattern categoryPattern = Pattern.compile("\\[\\[[Cc]ategory:(.*?)\\]\\]", Pattern.MULTILINE);
    final Pattern linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE);
    final Map<String, Pattern> languagePattern = new HashMap<String, Pattern>(5);
    languagePattern.put(WikipediaRiver.REDIRECT_REGEX_KEY, redirectPattern);
    languagePattern.put(WikipediaRiver.DISAMBIGUATION_REGEX_KEY, disambCatPattern);
    languagePattern.put(WikipediaRiver.STUB_REGEX_KEY, stubPattern);
    languagePattern.put(WikipediaRiver.CATEGORY_REGEX_KEY, categoryPattern);
    languagePattern.put(WikipediaRiver.LINK_REGEX_KEY, linkPattern);

    final WikiTextParser wikiTextParser = new WikiTextParser(input, languagePattern);
    final String plainText = wikiTextParser.getPlainText();
    assertThat(plainText.indexOf("== "), is(equalTo(-1)));
    assertThat(plainText.indexOf("* "), is(equalTo(-1)));
    assertThat(plainText.indexOf("[["), is(equalTo(-1)));
    assertThat(plainText.indexOf("]]"), is(equalTo(-1)));
    assertThat(plainText.indexOf("'''"), is(equalTo(-1)));
    assertThat(plainText.indexOf("'''"), is(equalTo(-1)));
  }
}