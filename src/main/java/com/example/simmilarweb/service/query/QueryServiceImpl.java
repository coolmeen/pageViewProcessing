package com.example.simmilarweb.service.query;

import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.pojo.WebStat;
import com.example.simmilarweb.repository.UserStatRepo;
import com.example.simmilarweb.repository.WebStatRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class QueryServiceImpl implements QueryService {

    UserStatRepo userStatsUserStatRepo;
    WebStatRepo webStatRepo;

    @Autowired
    public QueryServiceImpl(UserStatRepo userStatsUserStatRepo, WebStatRepo webStatRepo) {
        this.userStatsUserStatRepo = userStatsUserStatRepo;
        this.webStatRepo = webStatRepo;
    }

    @Override
    public UserStats getUserStat(String userId) {
        return userStatsUserStatRepo.findById(userId).orElseThrow(() -> new IllegalArgumentException("userId wasn't found"));
    }

    @Override
    public WebStat getWebStatBySiteUrl(String siteUrl) {
        return webStatRepo.findById(siteUrl).orElseThrow(() -> new IllegalArgumentException("siteUrl wasn't found"));
    }

    @Override
    public List<UserStats> getUserStatAll() {
        return userStatsUserStatRepo.findAll();
    }

    @Override
    public List<WebStat> getWebStatBySiteUrlAll() {
        return webStatRepo.findAll();
    }


}
