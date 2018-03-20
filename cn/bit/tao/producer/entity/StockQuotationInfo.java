package cn.bit.tao.producer.entity;

import java.io.Serializable;

/**
 *@author  tao wenjun
 *@date 2018年3月20日
 *股票信息类
 */

public class StockQuotationInfo implements Serializable {

	/**  版本信息  */
	private static final long serialVersionUID = 1L;

	/** 股票代码  */
	private String stockCode;
	
	/** 股票名称 */
	private String stockName;
	
	/** 交易时间 */
	private long tradeTime;
	
	/** 昨日收盘时间 */
	private float preClosePrice;
	
	/** 开盘价 */
	private float openPrice;
	
	/** 当前价，收盘时即为当日收盘价 */
	private float currentPrice;
	
	/** 今日最高价 */
	private float highPrice;
	
	/** 今日最低价 */
	private float lowPrice;

	/**
	 * @return the stockCode
	 */
	public String getStockCode() {
		return stockCode;
	}

	/**
	 * @param stockCode the stockCode to set
	 */
	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	/**
	 * @return the stockName
	 */
	public String getStockName() {
		return stockName;
	}

	/**
	 * @param stockName the stockName to set
	 */
	public void setStockName(String stockName) {
		this.stockName = stockName;
	}

	/**
	 * @return the tradeTime
	 */
	public long getTradeTime() {
		return tradeTime;
	}

	/**
	 * @param tradeTime the tradeTime to set
	 */
	public void setTradeTime(long tradeTime) {
		this.tradeTime = tradeTime;
	}

	/**
	 * @return the preClosePrice
	 */
	public float getPreClosePrice() {
		return preClosePrice;
	}

	/**
	 * @param preClosePrice the preClosePrice to set
	 */
	public void setPreClosePrice(float preClosePrice) {
		this.preClosePrice = preClosePrice;
	}

	/**
	 * @return the openPrice
	 */
	public float getOpenPrice() {
		return openPrice;
	}

	/**
	 * @param openPrice the openPrice to set
	 */
	public void setOpenPrice(float openPrice) {
		this.openPrice = openPrice;
	}

	/**
	 * @return the currentPrice
	 */
	public float getCurrentPrice() {
		return currentPrice;
	}

	/**
	 * @param currentPrice the currentPrice to set
	 */
	public void setCurrentPrice(float currentPrice) {
		this.currentPrice = currentPrice;
	}

	/**
	 * @return the highPrice
	 */
	public float getHighPrice() {
		return highPrice;
	}

	/**
	 * @param highPrice the highPrice to set
	 */
	public void setHighPrice(float highPrice) {
		this.highPrice = highPrice;
	}

	/**
	 * @return the lowPrice
	 */
	public float getLowPrice() {
		return lowPrice;
	}

	/**
	 * @param lowPrice the lowPrice to set
	 */
	public void setLowPrice(float lowPrice) {
		this.lowPrice = lowPrice;
	}
	
	@Override
	public String toString() {
		return this.stockCode+"|"+this.stockName+"|"+tradeTime+"|"+preClosePrice+
				"|"+openPrice+"|"+currentPrice+"|"+highPrice+"|"+lowPrice;
	}
}
