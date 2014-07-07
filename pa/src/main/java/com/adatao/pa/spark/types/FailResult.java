/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark.types;

public class FailResult extends ExecutorResult {
	public String message;
	
	public FailResult() {
		success = false;
	}
	
	public FailResult(String message) {
		success = false;
		this.setMessage(message);
	}
	
	public FailResult setMessage(String message) {
		this.message = message;
		return this;
	}
	
	@Override
	public Boolean isSuccess() {
		return success;
	}
}
