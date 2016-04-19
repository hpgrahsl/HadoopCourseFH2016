package edu.campus02.iwi.hadoop.twitter;

import java.util.ArrayList;

public class Tweet {
	public class Entities {
		public class HashTag {
			public String text;
		}
		public class Url {
			public String url;
		}
		public ArrayList<HashTag> hashtags;
		public ArrayList<Url> urls;
	}
	public Long id;
	public String text;
	public Entities entities;
	public String lang;
	public String source;
}
