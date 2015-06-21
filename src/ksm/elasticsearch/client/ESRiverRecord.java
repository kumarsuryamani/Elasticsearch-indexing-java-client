/**
 * 
 */
package ksm.elasticsearch.client;

/**
 * @author suryamani
 *
 */
public class ESRiverRecord {
	private String index;
	private String type;
	private String id;
	private String originalSource;
	private String updatedSource;
	private boolean isBayesianAnalysisNeeded;
	
	/**
	 * @param index
	 * @param type
	 * @param id
	 */
	public ESRiverRecord(String index, String type, String id) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
	}
	/**
	 * @return the index
	 */
	public String getIndex() {
		return index;
	}
	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @return the originalSource
	 */
	public String getOriginalSource() {
		return originalSource;
	}
	/**
	 * @return the updatedSource
	 */
	public String getUpdatedSource() {
		return updatedSource;
	}
	/**
	 * @param index the index to set
	 */
	public void setIndex(String index) {
		this.index = index;
	}
	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @param originalSource the originalSource to set
	 */
	public void setOriginalSource(String originalSource) {
		this.originalSource = originalSource;
	}
	/**
	 * @param updatedSource the updatedSource to set
	 */
	public void setUpdatedSource(String updatedSource) {
		this.updatedSource = updatedSource;
	}
	/**
	 * @return the isBayesianAnalysisNeeded
	 */
	public boolean isBayesianAnalysisNeeded() {
		return isBayesianAnalysisNeeded;
	}
	/**
	 * @param isBayesianAnalysisNeeded the isBayesianAnalysisNeeded to set
	 */
	public void setBayesianAnalysisNeeded(boolean isBayesianAnalysisNeeded) {
		this.isBayesianAnalysisNeeded = isBayesianAnalysisNeeded;
	}
	
	@Override
	public String toString() {
		return "Index: "+this.index+" , Type: "+this.type+" , Id: "+this.id;
	}
	

}
