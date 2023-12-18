package com.core.milvus.controllers;

import com.core.milvus.services.MilvusService;
import io.milvus.client.MilvusServiceClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author kylelin
 */
@RestController
@RequestMapping("/api")
public class MilvusController {

    //DI 自己建立的MilvusService
    private MilvusService milvusService;

    @Autowired
    public MilvusController(MilvusService milvusService) {
        this.milvusService = milvusService;
    }

    @GetMapping("/check")
    public String milvusCheck() {
        return this.milvusService.milvusCheck();
    }

    @GetMapping("/search")
    public List<Long> search() {
        List<Long> searchResultsWrapper = milvusService.search();
        return searchResultsWrapper;
    }
}
