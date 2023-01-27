package datacreator;

import dataoutput.DataOutput;

/**
 * Interface for creating randomly UserInfo and ApacheLog objects
 */
public interface DataCreator {
	/** Initialize the DataCreator */
	public void init();

	/**
	 * Sets the DataOutput object to write to
	 * 
	 * @param dataOutput The DataOutput object to write to
	 */
	public void setDataOutput(DataOutput dataOutput);

	/**
	 * Sets the number of iterations to run for creating random data
	 * 
	 * @param number The number of iterations of random data generation
	 */
	public void setIterations(long number);

	/** Starts the data creation and writing out to the data output */
	public void start();
}
