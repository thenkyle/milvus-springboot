package com.core.milvus.config;

import lombok.NoArgsConstructor;

/**
 * @author kylelin
 * Collection 可理解為Table
 * Partition 為分區，也就是將Collection切割成多個
 * Fields 可理解為Table Schema
 * Vectors 為向量
 */
@NoArgsConstructor
public final class MilvusCollectionConfig {

    /**
     * Collection name
     */
    public static final String COLLECTION_NAME = "case";

    /**
     * Partition name
     */
    public static final String PARTITION_NAME = "novel";

    /**
     * Feature dimension
     */
    public static final Integer FEATURE_DIM = 256;

    public static final String DESCRIPTION = "Test search";

    public static class Fields{
        /**
         * Primary key
         */
        public static final String CASE_ID = "case_id";

        /**
         * Word count fields
         */
        public static final String ACTION = "action";
        public static final String DEPARTURE_STATION = "departure_station";
        public static final String ARRIVAL_STATION = "arrival_station";
        public static final String PROFILE = "profile";

        /**
         * Vector
         */
        public static final String CASE_VECTOR = "case_vector";
    }


}
