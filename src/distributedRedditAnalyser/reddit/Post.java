package distributedRedditAnalyser.reddit;

import java.io.Serializable;

/**
 * Implementation of a reddit post
 * @author Luke Barnett 1109967
 *
 */
public class Post implements Serializable {
	
	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -1176274406450812313L;
	private String subReddit;
	private String title;
	
	/**
	 * Creates a new Post object
	 * @param title The title of the post
	 * @param subReddit The sub-reddit the post came from
	 */
	public Post(String title, String subReddit){
		this.title = title;
		this.subReddit = subReddit;
	}
	
	/**
	 * Creates an empty Post object
	 */
	public Post(){}
	
	/**
	 * Gets the sub-reddit the post came from
	 * @return The sub-reddit of the post
	 */
	public String getSubReddit(){
		return this.subReddit;
	}

	/**
	 * Gets the title of the post
	 * @return The title of the post
	 */
	public String getTitle(){
		return this.title;
	}
	
	/**
	 * Sets the sub-reddit the post came from
	 * @param subReddit The sub-reddit to set the post from
	 */
	public void setSubReddit(String subReddit){
		this.subReddit = subReddit;
	}
	
	/**
	 * Sets the title of the post
	 * @param title The title of the post to set
	 */
	public void setTitle(String title){
		this.title = title;
	}
	
	@Override
	public String toString(){
		return String.format("%s [%s]", this.title, this.subReddit);
	}

}
