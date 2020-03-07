package com.example.simmilarweb.controller;


import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.service.query.QueryService;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController("/")
public class Controller {

    @Autowired
    QueryService queryService;

    @RequestMapping(value = "/UniqueVisited/{id}",method = RequestMethod.GET)
    public Long numUniqueVisitedSites(@NonNull @PathVariable("id") String visitorId){
        return queryService.getUserStat(visitorId)
                .getUniquesitescount();
    }

    @RequestMapping(value = "/numSessions/{id}",method = RequestMethod.GET)
    public Long numSessions(@NonNull @PathVariable("id") String SiteUrl){
        return queryService.getWebStatBySiteUrl(SiteUrl)
                .getCount();
    }
    @RequestMapping(value = "/medianSession/{id}",method = RequestMethod.GET)
    public Double medianSessionLength(@NonNull @PathVariable("id") String SiteUrl){
        return queryService.getWebStatBySiteUrl(SiteUrl)
                .getMedianSessionLength();
    }


    @RequestMapping(value = "/UniqueVisitedAll",method = RequestMethod.GET)
    public String numUniqueVisitedSitedAl(){
        return queryService.getUserStatAll().toString();
    }

    @RequestMapping(value = "/allWebSessionStats",method = RequestMethod.GET)
    public String getAllWebStats(){
        return queryService.getWebStatBySiteUrlAll().toString();
    }

}
