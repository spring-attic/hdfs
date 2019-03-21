/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.app.hdfs.hadoop.store.event;

/**
 * Interface for publishing store based application events.
 *
 * @author Janne Valkealahti
 *
 */
public interface StoreEventPublisher {

	/**
	 * Publish a general application event of type {@link AbstractStoreEvent}.
	 *
	 * @param event the event
	 */
	void publishEvent(AbstractStoreEvent event);

}
