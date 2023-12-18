package com.core.milvus.services;

import com.core.milvus.config.MilvusCollectionConfig;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CheckHealthResponse;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetVersionResponse;
import io.milvus.grpc.SearchResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.CreatePartitionParam;
import io.milvus.response.SearchResultsWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;


/**
 * @author kylelin
 */
@Service
public class MilvusService {

    /**
     * DI MilvusServiceClient
     */
    private final MilvusServiceClient milvusServiceClient;
    Logger logger = LogManager.getLogger(getClass());

    static final IndexType INDEX_TYPE = IndexType.IVF_FLAT;
    static final String INDEX_PARAM = "{\"nlist\":1024}";
    private static final String[] ACTION = {"createPNR", "modifyPNR", "payment"};
    private static final String[] STATIONS = {"NAK", "TPE", "BAC", "TAY", "HSC", "MIL", "TAC", "CHA", "YUL", "CHY", "TNN", "ZUY"};
    private static final String[] PROFILES = {"F", "H", "E", "W", "P", "T", "S", "M"};

    @Autowired
    public MilvusService(MilvusServiceClient milvusServiceClient) {
        this.milvusServiceClient = milvusServiceClient;
    }

    /**
     * Initialize Milvus
     */
    public void initializeMilvus() {
        CreateCollectionParam createCollectionReq = this.createCollectionReq();
        CreatePartitionParam createPartitionReq = this.createPartitionReq();
        R<Boolean> respHasCollection = this.milvusServiceClient.hasCollection(
                HasCollectionParam.newBuilder()
                        .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                        .build()
        );
        if (respHasCollection.getData().equals(Boolean.TRUE)) {
            System.out.println("Collection exists.");
        } else {
            this.createCollection(createCollectionReq);
            this.createPartition(createPartitionReq);
        }

    }

    public void insertData() {
        insertData((MilvusServiceClient) milvusServiceClient);
    }

    @Async
    public void buildIndex() {
        buildIndex((MilvusServiceClient) milvusServiceClient);
    }

    @Async
    public void loadCollection() {
        loadCollection((MilvusServiceClient) milvusServiceClient);
    }

    public List<Long> search() {
        final Integer SEARCH_K = 3;
        final String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields = Arrays.asList(MilvusCollectionConfig.Fields.CASE_ID);
        List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(0.1f, 0.2f, 0.3f, 0.4f));

        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                .withMetricType(MetricType.L2)
                .withOutFields(search_output_fields)
                .withTopK(SEARCH_K)
                .withVectors(search_vectors)
                .withVectorFieldName(MilvusCollectionConfig.Fields.CASE_VECTOR)
                .withParams(SEARCH_PARAM)
                .build();

        R<SearchResults> respSearch = milvusServiceClient.search(searchParam);
        SearchResultsWrapper wrapperSearch = new SearchResultsWrapper(respSearch.getData().getResults());
        System.out.println("wrapperSearch.getIDScore" + wrapperSearch.getIDScore(0));
        System.out.println("wrapperSearch.getFieldData" + wrapperSearch.getFieldData(MilvusCollectionConfig.Fields.CASE_ID, 0));
        // 釋放Collection
//        milvusServiceClient.releaseCollection(
//                ReleaseCollectionParam.newBuilder()
//                        .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
//                        .build());
        return (List<Long>) wrapperSearch.getFieldData(MilvusCollectionConfig.Fields.CASE_ID, 0);
    }

    /**
     * 建立Collection所需參數
     */
    private CreateCollectionParam createCollectionReq() {
        FieldType fieldType1 = FieldType.newBuilder()
                .withName(MilvusCollectionConfig.Fields.CASE_ID)
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();
        FieldType fieldType2 = FieldType.newBuilder()
                .withName(MilvusCollectionConfig.Fields.ACTION)
                .withDataType(DataType.VarChar)
                .withDataType(DataType.VarChar).withMaxLength(30)

                .build();
        FieldType fieldType3 = FieldType.newBuilder()
                .withName(MilvusCollectionConfig.Fields.DEPARTURE_STATION)
                .withDataType(DataType.VarChar).withMaxLength(21)
                .build();
        FieldType fieldType4 = FieldType.newBuilder()
                .withName(MilvusCollectionConfig.Fields.ARRIVAL_STATION)
                .withDataType(DataType.VarChar).withMaxLength(21)
                .build();
        FieldType fieldType5 = FieldType.newBuilder()
                .withName(MilvusCollectionConfig.Fields.PROFILE)
                .withDataType(DataType.VarChar).withMaxLength(21)
                .build();
        FieldType fieldType6 = FieldType.newBuilder()
                .withName(MilvusCollectionConfig.Fields.CASE_VECTOR)
                .withDataType(DataType.FloatVector)
                .withDimension(4)
                .build();

        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                .withDescription(MilvusCollectionConfig.DESCRIPTION)
                .withShardsNum(2)
                .addFieldType(fieldType1)
                .addFieldType(fieldType2)
                .addFieldType(fieldType3)
                .addFieldType(fieldType4)
                .addFieldType(fieldType5)
                .addFieldType(fieldType6)
                .build();

        return createCollectionReq;
    }

    /**
     * 建立Partition所需參數
     */
    private CreatePartitionParam createPartitionReq() {
        CreatePartitionParam createPartitionReq = CreatePartitionParam.newBuilder()
                .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                .withPartitionName(MilvusCollectionConfig.PARTITION_NAME)
                .build();
        return createPartitionReq;
    }

    /**
     * 產生一個Collection
     */
    private void createCollection(CreateCollectionParam createCollectionReq) {
        milvusServiceClient.createCollection(createCollectionReq);
        System.out.println("Create Collection Complete.");
    }

    /**
     * 產生一個Partition
     */
    private void createPartition(CreatePartitionParam createPartitionParamReq) {
        milvusServiceClient.createPartition(createPartitionParamReq);
        System.out.println("Create Partition Complete.");
    }

    /**
     * 建立要新增的資料
     */
    private void insertData(MilvusServiceClient milvusServiceClient) {
        Random ran = new Random();
        List<Long> case_id_array = new ArrayList<>();
        List<String> action_array = new ArrayList<>();
        List<String> departure_array = new ArrayList<>();
        List<String> arrival_array = new ArrayList<>();
        List<String> profile_array = new ArrayList<>();
        List<List<Float>> case_vector_array = new ArrayList<>();
        //List<float[]> combined_vector_array = new ArrayList<>();

        // 生成所有可能的 departure_arrive 組合
        Map<String, List<String>> combinations = generateCombinations(STATIONS);
        List<String> profileCombinations = generateRandomProfiles(PROFILES);

        int index = 0;
        for (long i = 0L; i < 10; ++i) {
            case_id_array.add(i);
            // 隨機選擇一個 action
            String action = ACTION[ran.nextInt(ACTION.length)];
            action_array.add(action);
            // 隨機選擇一個 departure、arrive 組合
            String departureStation = combinations.get("departure").get(index);
            departure_array.add(departureStation);
            String arrivalStation = combinations.get("arrival").get(index);
            arrival_array.add(arrivalStation);
            // 隨機選擇一個 profile 組合
            String profiles = profileCombinations.get(ran.nextInt(profileCombinations.size()));
            profile_array.add(profiles);

            System.out.print("該筆資料：" + action + ",");
            System.out.print(departureStation + ",");
            System.out.print(arrivalStation + ",");
            System.out.println(profiles);

            // 使用 VectorizationService 計算向量
            // float[] actionVector = vectorizationSvc.getFieldVector(action);
            // float[] departureVector = vectorizationSvc.getFieldVector(departureStation);
            // float[] arrivalVector = vectorizationSvc.getFieldVector(arrivalStation);
            // float[] profileVector = vectorizationSvc.getFieldVector(profiles);


            // 將4個向量合併為一個
            // float[] combinedVector = combineVectors(actionVector, departureVector, arrivalVector, profileVector);
            // 將合併的向量轉換為 List<Float> 並添加到 case_vector_array
            // case_vector_array.add(convertArrayToList(combinedVector));
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < 4; ++k) {
                vector.add(ran.nextFloat());
            }
            // System.out.println("vector:"+vector);
            case_vector_array.add(vector);
            index++;
        }

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field(MilvusCollectionConfig.Fields.CASE_ID, case_id_array));
        fields.add(new InsertParam.Field(MilvusCollectionConfig.Fields.ACTION, action_array));
        fields.add(new InsertParam.Field(MilvusCollectionConfig.Fields.DEPARTURE_STATION, departure_array));
        fields.add(new InsertParam.Field(MilvusCollectionConfig.Fields.ARRIVAL_STATION, arrival_array));
        fields.add(new InsertParam.Field(MilvusCollectionConfig.Fields.PROFILE, profile_array));
        fields.add(new InsertParam.Field(MilvusCollectionConfig.Fields.CASE_VECTOR, case_vector_array));

        InsertParam insertParam = InsertParam.newBuilder()
                .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                .withPartitionName(MilvusCollectionConfig.PARTITION_NAME)
                .withFields(fields)
                .build();
        logger.info(insertParam);
        milvusServiceClient.insert(insertParam);
        System.out.println("Insert Data Complete.");

    }

    /**
     * 隨機產生departure_arrive站組合
     */
    private Map<String, List<String>> generateCombinations(String[] stations) {
        Map<String, List<String>> stationMap = new HashMap<>();
        for (int i = 0; i < stations.length - 1; i++) {
            for (int j = i + 1; j < stations.length; j++) {
                String departureStation = stations[i];
                String arrivalStation = stations[j];
                // 將 departureStation 加到對應的 List 中，如果不存在則創建一個新的 List
                stationMap.computeIfAbsent("departure", k -> new ArrayList<>()).add(departureStation);

                // 將 arrivalStation 加到對應的 List 中，如果不存在則創建一個新的 List
                stationMap.computeIfAbsent("arrival", k -> new ArrayList<>()).add(arrivalStation);
            }
        }
        return stationMap;
    }

    /**
     * 隨機生成profile組合
     */
    private static List<String> generateRandomProfiles(String[] count) {
        Random random = new Random();
        List<String> profiles = new ArrayList<>();

        for (int i = 0; i < count.length; ++i) {
            int countDigit1 = random.nextInt(3) + 1;  // 1到3之間的數字
            String letter1 = PROFILES[random.nextInt(PROFILES.length)];
            int countDigit2 = random.nextInt(3) + 1;  // 1到3之間的數字
            String letter2 = PROFILES[random.nextInt(PROFILES.length)];

            String profile = countDigit1 + letter1 + countDigit2 + letter2;
            profiles.add(profile);
        }
        return profiles;
    }

    private void buildIndex(MilvusServiceClient milvusServiceClient) {
        milvusServiceClient.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                        .withFieldName(MilvusCollectionConfig.Fields.CASE_VECTOR)
                        .withIndexType(INDEX_TYPE)
                        .withMetricType(MetricType.L2)
                        .withExtraParam(INDEX_PARAM)
                        .withSyncMode(Boolean.FALSE)
                        .build()
        );
        System.out.println("Bilid Complete.");

    }

    /**
     * 載入Collection
     */
    private void loadCollection(MilvusServiceClient milvusServiceClient) {
        milvusServiceClient.loadCollection(
                LoadCollectionParam.newBuilder()
                        .withCollectionName(MilvusCollectionConfig.COLLECTION_NAME)
                        .build()
        );
        System.out.println("Load Collection Complete.");

    }

    /**
     * 將 float[] 轉換為 List<Float>
     */
    private List<Float> convertArrayToList(float[] array) {
        List<Float> list = new ArrayList<>();
        for (float value : array) {
            list.add(value);
        }
        return list;
    }

    private float[] combineVectors(float[] vector1, float[] vector2, float[] vector3, float[] vector4) {
        int length = vector1.length + vector2.length + vector3.length + vector4.length;
        float[] combinedVector = new float[length];

        // 將每個向量的值複製到合併的向量中
        System.arraycopy(vector1, 0, combinedVector, 0, vector1.length);
        System.arraycopy(vector2, 0, combinedVector, vector1.length, vector2.length);
        System.arraycopy(vector3, 0, combinedVector, vector1.length + vector2.length, vector3.length);
        System.arraycopy(vector3, 0, combinedVector, vector1.length + vector3.length, vector4.length);

        return combinedVector;
    }

    /**測試連線正常*/
    public String milvusCheck() {
        R<CheckHealthResponse> checkHealthResponseR = this.milvusServiceClient.checkHealth();
        R<GetVersionResponse> getVersionResponseR = this.milvusServiceClient.getVersion();
        return checkHealthResponseR.toString() + " , " + getVersionResponseR.toString();
    }

}
