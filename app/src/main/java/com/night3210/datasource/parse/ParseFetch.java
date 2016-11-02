package com.night3210.datasource.parse;

import android.os.AsyncTask;
import android.util.Pair;

import com.parse.DeleteCallback;
import com.parse.FindCallback;
import com.parse.FunctionCallback;
import com.parse.ParseCloud;
import com.parse.ParseException;
import com.parse.ParseObject;
import com.parse.ParseQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.parse.SaveCallback;
import com.night3210.datasource.core.ListDataSource;
import com.night3210.datasource.core.fetch_result.BaseFetchResult;
import com.night3210.datasource.core.listeners.BoolCallback;
import com.night3210.datasource.core.listeners.DataCallback;
import com.night3210.datasource.core.listeners.DataObject;
import com.night3210.datasource.core.listeners.Fetch;

/**
 * Created by Developer on 2/12/2016.
 */
public class ParseFetch implements Fetch {

    public interface CloudParamsProvider {
        public Map<String, ?> getCloudParams(ListDataSource.Paging paging);
    }

    public interface OnlineQueryProvider {
        public ParseQuery<? extends DataObject> getQuery();
    }

    public interface OfflineQueryProvider {
        public List<ParseQuery<? extends DataObject>> getOfflineQueries();
    }

    // Call Query section
    protected OnlineQueryProvider onlineQueryProvider;

    // Call Cloud section
    protected String cloudFuncName;
    protected CloudParamsProvider cloudParamsProvider;


    // Offline section
    // Should be Parse.isLocalDatastoreEnabled() by default, but thanks to Parse developers - this method is hidden
    protected boolean offlineFetchAvailable = false;
    protected boolean offlineStoreAvailable = false;
    protected String pinName = ParseObject.DEFAULT_PIN;
    protected OfflineQueryProvider offlineQueryProvider;

    // Implementing Fetch section
    @Override
    public void fetchOnline(ListDataSource.Paging paging, DataCallback dataCallback) {
        if (cloudFuncName != null) {
            fetchCloud(paging, dataCallback);
        } else {
            fetchQuery(paging, dataCallback);
        }
    }

    @Override
    public void fetchOffline(final DataCallback dataCallback) {
        if (!offlineFetchAvailable) {
            return;
        }

        if (offlineQueryProvider == null) {
            throw new IllegalStateException("You need to set offlineQueryProvider if offlineFetchAvailable is true");
        }
        final List<ParseQuery<? extends DataObject>> queries = offlineQueryProvider.getOfflineQueries();
        new AsyncTask<Void, Void, Pair<List<Object>, Throwable>>() {

            @Override
            protected Pair<List<Object>, Throwable> doInBackground(Void... voids) {
                List<Object> array = new ArrayList<>();
                Throwable th = null;
                for (ParseQuery<?> query : queries) {
                    try {
                        List result = query.find();
                        if (result != null) {
                            array.addAll(result);
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                        th = e;
                    }
                }
                return new Pair<>(array, th);
            }

            @Override
            protected void onPostExecute(Pair<List<Object>, Throwable> listThrowablePair) {
                super.onPostExecute(listThrowablePair);
                if (listThrowablePair.second != null) {
                    dataCallback.onError(listThrowablePair.second);
                } else {
                    dataCallback.onSuccess(listThrowablePair.first);
                }
            }
        }.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    @Override
    public void storeItems(BaseFetchResult<? extends DataObject> baseFetchResult, final BoolCallback boolCallback) {
        if (!offlineStoreAvailable) {
            return;
        }
        final List<ParseObject> itemsToSave = new ArrayList<>();
        for (List<? extends DataObject> section : baseFetchResult.getSections()) {
            itemsToSave.addAll((List)section);
        }
        if (!offlineFetchAvailable) {
            storeItemsInternal(itemsToSave, boolCallback);
            return;
        }
        fetchOffline(new DataCallback() {
            @Override
            public void onSuccess(Object o) {
                if (o instanceof List) {
                    List<ParseObject> objects = (List<ParseObject>)o;
                    ParseObject.unpinAllInBackground(pinName, objects, new DeleteCallback() {

                        @Override
                        public void done(ParseException e) {
                            storeItemsInternal(itemsToSave, boolCallback);
                        }
                    });
                    return;
                }
                storeItemsInternal(itemsToSave, boolCallback);
            }

            @Override
            public void onError(Throwable throwable) {
                storeItemsInternal(itemsToSave, boolCallback);
            }
        });
    }

    protected void storeItemsInternal(List<ParseObject> itemsToSave, final BoolCallback boolCallback) {
        ParseObject.pinAllInBackground(pinName, itemsToSave, new SaveCallback() {
            @Override
            public void done(ParseException e) {
                if (e != null) {
                    boolCallback.onError(e);
                    return;
                }
                boolCallback.onSuccess();
            }
        });
    }

    // Fetch online
    protected void fetchCloud(ListDataSource.Paging paging, final DataCallback dataCallback) {
        if (dataCallback == null) {
            throw new IllegalArgumentException("dataCallback cannot be null");
        }
        Map<String, ?> params = cloudParamsProvider != null
                ? cloudParamsProvider.getCloudParams(paging)
                : new HashMap<String, Object>();
        ParseCloud.callFunctionInBackground(cloudFuncName, params, new FunctionCallback<Object>() {
            @Override
            public void done(Object object, ParseException e) {
                if (e != null) {
                    dataCallback.onError(e);
                    return;
                }
                dataCallback.onSuccess(object);
            }
        });
    }

    protected void fetchQuery(ListDataSource.Paging paging, final DataCallback dataCallback) {
        if (dataCallback == null) {
            throw new IllegalArgumentException("dataCallback cannot be null");
        }
        if (onlineQueryProvider == null) {
            throw new IllegalStateException("Should be set either onlineQueryProvider or cloudFuncName");
        }
        ParseQuery query = onlineQueryProvider.getQuery();
        if (query == null) {
            throw new IllegalStateException("Should return query in getQuery()");
        }
        if (paging != null) {
            query.setSkip(paging.getSkip());
            query.setLimit(paging.getLimit());
        }
        query.findInBackground(new FindCallback<ParseObject>() {
            @Override
            public void done(List objects, ParseException e) {
                if (e != null) {
                    dataCallback.onError(e);
                    return;
                }
                dataCallback.onSuccess(objects);
            }
        });
    }

    // Properties
    public OnlineQueryProvider getOnlineQueryProvider() {
        return onlineQueryProvider;
    }

    public void setOnlineQueryProvider(OnlineQueryProvider onlineQueryProvider) {
        this.onlineQueryProvider = onlineQueryProvider;
    }

    public String getCloudFuncName() {
        return cloudFuncName;
    }

    public void setCloudFuncName(String cloudFuncName) {
        this.cloudFuncName = cloudFuncName;
    }

    public CloudParamsProvider getCloudParamsProvider() {
        return cloudParamsProvider;
    }

    public void setCloudParamsProvider(CloudParamsProvider cloudParamsProvider) {
        this.cloudParamsProvider = cloudParamsProvider;
    }

    public boolean isOfflineFetchAvailable() {
        return offlineFetchAvailable;
    }

    public void setOfflineFetchAvailable(boolean offlineFetchAvailable) {
        this.offlineFetchAvailable = offlineFetchAvailable;
    }

    public boolean isOfflineStoreAvailable() {
        return offlineStoreAvailable;
    }

    public void setOfflineStoreAvailable(boolean offlineStoreAvailable) {
        this.offlineStoreAvailable = offlineStoreAvailable;
    }

    public String getPinName() {
        return pinName;
    }

    public void setPinName(String pinName) {
        this.pinName = pinName;
    }

    public OfflineQueryProvider getOfflineQueryProvider() {
        return offlineQueryProvider;
    }

    public void setOfflineQueryProvider(OfflineQueryProvider offlineQueryProvider) {
        this.offlineQueryProvider = offlineQueryProvider;
    }
}
