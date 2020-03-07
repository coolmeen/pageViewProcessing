package com.example.simmilarweb.service.query;

import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.pojo.WebStat;

import java.util.List;

public interface QueryService {
    UserStats getUserStat(String userId);

    WebStat getWebStatBySiteUrl(String siteUrl);

    List<UserStats> getUserStatAll();

    List<WebStat> getWebStatBySiteUrlAll();

}
