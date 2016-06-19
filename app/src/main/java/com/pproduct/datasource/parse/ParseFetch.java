package com.pproduct.datasource.parse;

import com.parse.DeleteCallback;
import com.parse.FindCallback;
import com.parse.ParseConfig;
import com.parse.ParseException;
import com.parse.ParseObject;
import com.parse.ParseQuery;

import java.util.List;

import com.parse.SaveCallback;
import com.pproduct.datasource.core.ListDataSource;
import com.pproduct.datasource.core.LogUtils;
import com.pproduct.datasource.core.fetch_result.BaseFetchResult;
import com.pproduct.datasource.core.listeners.DataCallback;
import com.pproduct.datasource.core.listeners.Fetch;

/**
 * Created by Developer on 2/12/2016.
 */
public class ParseFetch implements Fetch {
    ParseQuery query = null;
    private String mPinName = ParseObject.DEFAULT_PIN+"x";
    public ParseFetch(ParseQuery query) {
        this.query = query;
    }
    public void setQuery(ParseQuery query) {
        this.query = query;
    }
    @Override
    public void fetchOnline(ListDataSource.Paging paging, final DataCallback callback) {
        if(paging!=null) {
            query.setSkip(paging.getSkip());
            query.setLimit(paging.getLimit());
        }
        query.findInBackground(new FindCallback<ParseObject>() {
            @Override
            public void done(List objects, ParseException e) {
                if(e==null) {
                    callback.onSuccess(objects);
                    pinObjects(objects);
                } else
                    callback.onError(e);
            }
        });
    }

    @Override
    public void fetchOffline(final DataCallback callback) {
        ParseQuery offlineQuery=new ParseQuery(query);
        offlineQuery.fromPin(mPinName);
        offlineQuery.setSkip(0);
        offlineQuery.setLimit(100);
        offlineQuery.findInBackground(new FindCallback<ParseObject>() {
            @Override
            public void done(List<ParseObject> objects, ParseException e) {
                if(e==null)
                    callback.onSuccess(objects);
                else
                    callback.onError(e);
            }
        });

    }

    @Override
    public void storeItems(BaseFetchResult result) {


    }
    private void pinObjects(final List objects){
        ParseObject.unpinAllInBackground(mPinName, new DeleteCallback() {
            @Override
            public void done(ParseException e) {
                if(e!=null) {
                    e.printStackTrace();
                }
                ParseObject.pinAllInBackground(mPinName, objects, new SaveCallback() {
                    @Override
                    public void done(ParseException e) {
                        if(e!=null)
                            e.printStackTrace();
                    }
                });
            }
        });
    }
}
