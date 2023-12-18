package com.core.milvus;

import com.core.milvus.services.MilvusService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * @author kylelin
 */
@Component
public class MilvusCommandLineRunner implements CommandLineRunner {

    private final MilvusService milvusService;

    public MilvusCommandLineRunner(MilvusService milvusService) {
        this.milvusService = milvusService;
    }

    @Override
    public void run(String... args) throws Exception {
        // Application 執行時啟動
        System.out.println("CommandLineRunner executed");

        // 所要執行 milvusService
        milvusService.initializeMilvus();
        milvusService.insertData();
        milvusService.buildIndex();
        milvusService.loadCollection();
        milvusService.search();
    }
}
