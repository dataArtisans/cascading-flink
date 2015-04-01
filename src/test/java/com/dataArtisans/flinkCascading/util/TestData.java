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

package com.dataArtisans.flinkCascading.util;

public class TestData {

	public static String getTextData() {
		return "To be, or not to be,--that is the question:--\n" +
				"Whether 'tis nobler in the mind to suffer\n" +
				"The slings and arrows of outrageous fortune\n" +
				"Or to take arms against a sea of troubles,\n" +
				"And by opposing end them?--To die,--to sleep,--\n" +
				"No more; and by a sleep to say we end\n" +
				"The heartache, and the thousand natural shocks\n" +
				"That flesh is heir to,--'tis a consummation\n" +
				"Devoutly to be wish'd. To die,--to sleep;--\n" +
				"To sleep! perchance to dream:--ay, there's the rub;\n" +
				"For in that sleep of death what dreams may come,\n" +
				"When we have shuffled off this mortal coil,\n" +
				"Must give us pause: there's the respect\n" +
				"That makes calamity of so long life;\n" +
				"For who would bear the whips and scorns of time,\n" +
				"The oppressor's wrong, the proud man's contumely,\n" +
				"The pangs of despis'd love, the law's delay,\n";
	}

	public static String getTextData2() {
		return "The insolence of office, and the spurns\n" +
				"That patient merit of the unworthy takes,\n" +
				"When he himself might his quietus make\n" +
				"With a bare bodkin? who would these fardels bear,\n" +
				"To grunt and sweat under a weary life,\n" +
				"But that the dread of something after death,--\n" +
				"The undiscover'd country, from whose bourn\n" +
				"No traveller returns,--puzzles the will,\n";
	}

	public static String getTextData3() {
		return "And makes us rather bear those ills we have\n" +
				"Than fly to others that we know not of?\n" +
				"Thus conscience does make cowards of us all;\n" +
				"And thus the native hue of resolution\n" +
				"Is sicklied o'er with the pale cast of thought;\n" +
				"And enterprises of great pith and moment,\n" +
				"With this regard, their currents turn awry,\n" +
				"And lose the name of action.--Soft you now!\n" +
				"The fair Ophelia!--Nymph, in thy orisons\n" +
				"Be all my sins remember'd.";
	}

	public static String getApacheLogData() {
		return "75.185.76.245 - - [01/Sep/2007:00:01:03 +0000] \"POST /mt-tb.cgi/235 HTTP/1.1\" 403 174 \"-\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"68.46.103.112 - - [01/Sep/2007:00:01:17 +0000] \"POST /mt-tb.cgi/92 HTTP/1.1\" 403 174 \"-\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"76.197.151.0 - - [01/Sep/2007:00:02:24 +0000] \"POST /mt-tb.cgi/274 HTTP/1.1\" 403 174 \"-\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"63.123.238.8 - - [01/Sep/2007:00:02:49 +0000] \"GET /robots.txt HTTP/1.1\" 200 0 \"-\" \"Mozilla/2.0 (compatible; Ask Jeeves/Teoma)\" \"-\"\n" +
				"63.123.238.8 - - [01/Sep/2007:00:02:49 +0000] \"GET / HTTP/1.1\" 200 34293 \"-\" \"Mozilla/2.0 (compatible; Ask Jeeves/Teoma)\" \"-\"\n" +
				"12.215.138.88 - - [01/Sep/2007:00:03:31 +0000] \"POST /archives/000180.html HTTP/1.1\" 405 315 \"http://www.example.org/mt-comments.cgi\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"78.84.186.161 - - [01/Sep/2007:00:04:00 +0000] \"POST /mt-tb.cgi/247 HTTP/1.1\" 403 174 \"-\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"68.102.166.126 - - [01/Sep/2007:00:07:51 +0000] \"POST /archives/000229.html HTTP/1.1\" 405 315 \"http://www.example.org/mt-comments.cgi\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"68.102.166.126 - - [01/Sep/2007:00:07:51 +0000] \"POST /archives/000228.html HTTP/1.1\" 405 315 \"http://www.example.org/mt-comments.cgi\" \"Opera/9.10 (Windows NT 5.1; U; ru)\" \"-\"\n" +
				"122.152.128.48 - - [01/Sep/2007:00:10:14 +0000] \"GET / HTTP/1.1\" 200 34293 \"-\" \"Baiduspider+(+http://www.baidu.com/search/spider_jp.html)\" \"-\"Ï\n";
	}

}
