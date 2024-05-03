package site.ycsb.db;

import com.google.api.client.util.Maps;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIOCatalog;
import site.ycsb.DBException;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

public class FileIOCatalogClient extends CatalogClient<FileIOCatalog> {
  @Override
  public void init() throws DBException {
    try (FileInputStream credentials = new FileInputStream(credentials())) {
        storage = RemoteStorageHelper.create("lst-consistency", credentials).getOptions().getService();
      } catch (Exception e){
        throw new DBException("Failed to load credentials");
      }
      try {
        GCSFileIO io = new GCSFileIO(() -> storage, new GCPProperties());
        final Map<String, String> properties = Maps.newHashMap();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        catalog = new FileIOCatalog("test", gs_location, null, io, Maps.newHashMap());
        initLock.lock();
        try {Thread.sleep(1000);} catch (Exception ignored){};
        catalog.initialize("YCSB-Bench", properties);
        if(!catalogInited) {
          try {Thread.sleep(1000);} catch (Exception ignored){};
          initTables();
          catalogInited = true;
        }
        initLock.unlock();
      } catch (Exception e){
        System.out.println(e.getLocalizedMessage());
        throw new DBException("Failed to load remote / init storage or catalog");
      }

  }
}
