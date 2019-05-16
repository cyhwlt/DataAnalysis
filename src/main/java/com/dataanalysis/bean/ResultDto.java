package com.dataanalysis.bean;

/**
 * 返回结果实体信息
 * @author soyuan
 *
 */
public class ResultDto {
	private int code; // 返回状态码
	private String message; // 返回错误信息
	private Object data; // 返回数据
	private long pageCount; // 分页查询总条数

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public long getPageCount() {
		return pageCount;
	}

	public void setPageCount(long pageCount) {
		this.pageCount = pageCount;
	}
}
