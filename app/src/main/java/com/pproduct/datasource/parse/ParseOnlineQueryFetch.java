package com.pproduct.datasource.parse;

import com.parse.FindCallback;
import com.parse.ParseException;
import com.parse.ParseObject;
import com.parse.ParseQuery;

import java.util.List;

import com.pproduct.datasource.core.ListDataSource;
import com.pproduct.datasource.core.LogUtils;
import com.pproduct.datasource.core.fetch_result.BaseFetchResult;
import com.pproduct.datasource.core.listeners.DataCallback;
import com.pproduct.datasource.core.listeners.Fetch;

/**
 * Created by Developer on 2/12/2016.
 */
public class ParseOnlineQueryFetch implements Fetch {
    ParseQuery query = null;
    public ParseOnlineQueryFetch(ParseQuery query) {
        this.query = query;
    }
    public void setQuery(ParseQuery query) {
        this.query = query;
    }
    @Override
    public void fetchOnline(ListDataSource.Paging paging, final DataCallback callback) {
        LogUtils.logi("perform fetch query");
        if(paging!=null) {
            LogUtils.logi("paging: s/l"+paging.getSkip()+"/"+paging.getLimit());
            query.setSkip(paging.getSkip());
            query.setLimit(paging.getLimit());
        }
        query.findInBackground(new FindCallback<ParseObject>() {
            @Override
            public void done(List objects, ParseException e) {
                if(e==null)
                    callback.onSuccess(objects);
                else
                    callback.onError(e);
            }
        });
    }

    @Override
    public void fetchOffline(DataCallback callback) {

    }

    @Override
    public void storeItems(BaseFetchResult result) {

    }
}
