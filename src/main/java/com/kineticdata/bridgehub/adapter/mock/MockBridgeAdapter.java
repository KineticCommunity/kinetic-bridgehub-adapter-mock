package com.kineticdata.bridgehub.adapter.mock;

import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeUtils;

import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MockBridgeAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name. */
    public static final String NAME = "Mock Bridge";
    
    /** Defines the logger */
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MockBridgeAdapter.class);
    
    /** Defines the collection of property names for the adapter. */
    public static class Properties {}
    
    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    
    /** 
     * Specifies the configurable properties for the adapter.  These are populated as part of object
     * construction so that the collection of properties (default values, menu options, etc) are 
     * available before the adapter is configured.  These initial properties can be used to 
     * dynamically generate the list of configurable properties, such as when the Kinetic Filehub
     * application prepares the new Filestore display.
     */
    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap();
    
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
       return  "1.0.0";
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }
    
    @Override
    public void initialize() throws BridgeError {
        
    }
    
    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/
    
    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));
        // Log the access
        logger.trace("Counting the records");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Query: " + request.getQuery());
        
        // If the request is to simulate an error
        if (request.getParameter("error") != null) {
            throw new BridgeError(request.getParameter("error"));
        }
        
        // Build the default result values
        Long count = new Long(1);
        // Override the default result values if they were manually specified
        if (request.getParameter("count") != null) {
            count = Long.valueOf(request.getParameter("count"));
        }
        Map<String,String> metadata = request.getMetadata();
        
        // Create count object
        return new Count(count, metadata);
    }
    
    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));
        // Log the access
        logger.trace("Retrieving a record");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Fields: " + request.getFieldString());
        logger.trace("  Query: " + request.getQuery());
        
        // If the request is to simulate an error
        if (request.getParameter("error") != null) {
            throw new BridgeError(request.getParameter("error"));
        }

        // Build up the metadata
        Map<String,String> metadata  = request.getMetadata();
        // Build the result record
        Map<String,Object> record = buildRecord(request);

        // Return the response
        return new Record(record, metadata);
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        request.setQuery(substituteQueryParameters(request));
        // Log the access
        logger.trace("Searching the records");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Fields: " + request.getFieldString());
        logger.trace("  Query: " + request.getQuery());
        
        // If the request is to simulate an error
        if (request.getParameter("error") != null) {
            throw new BridgeError(request.getParameter("error"));
        }

        // Build if a default size or count has not been passed with the mock
        // bridge, default it to count=10 and size=5
        Integer count;
        if (request.getMetadata("count") != null && !"".equals(request.getMetadata("count"))) {
            count = Integer.valueOf(request.getMetadata("count"));
        } else {
            count = 10;
        }

        // Build up the metadata
        Map<String,String> metadata = request.getMetadata();
        // Validate and normalize the pagination metadata
        metadata = BridgeUtils.normalizePaginationMetadata(metadata);

        // Build up the results
        List<Record> records = new ArrayList<Record>();
        int offset = Integer.valueOf(metadata.get("offset"));
        int maxRecord;
        if (!metadata.get("pageSize").equals("0")) {
            maxRecord = (offset + Integer.valueOf(metadata.get("pageSize"))) > count ? count : offset + Integer.valueOf(metadata.get("pageSize"));
        } else {
            maxRecord = count;
        }
        for (int i=offset; i<maxRecord; i++) {
            Record record = new Record(buildRecords(request,i));
            records.add(record);
        }
        
        metadata.put("size",String.valueOf(records.size()));
        metadata.put("count",count.toString());

        // Return the response
        return new RecordList(request.getFields(), records, metadata);
    }
    
    /*---------------------------------------------------------------------------------------------
     * HELPER METHODS
     *-------------------------------------------------------------------------------------------*/
    
    private Map<String,Object> buildRecord(BridgeRequest request) {
        return buildRecords(request, 1);
    }

    private Map<String,Object> buildRecords(BridgeRequest request, int index) {
        Map<String,Object> records = new LinkedHashMap();
        String recordsString = request.getParameter("records");
        if (recordsString == null && request.getFields() != null) {
            List<String> recordList = new ArrayList();
            for(String fieldName : request.getFields()) {
                recordList.add(fieldName+":"+fieldName+" $");
            }
            recordsString = StringUtils.join(recordList, ",");
        }
        if (recordsString != null && !"".equals(recordsString)) {
            for (String attribute : recordsString.split("(?<!\\\\),")) {
                String[] tuple = attribute.split(":", 2);
                    if (tuple.length != 2) {
                        throw new IllegalArgumentException("Attributes must be "+
                            "specified using comma separated NAME:VALUE pairs, "+
                            "the attribute '"+attribute+"' is not valid.");
                    }
                records.put(tuple[0], tuple[1].replace("$", String.valueOf(index)));
            }
        }
        return records;
    }
    
    private String substituteQueryParameters(BridgeRequest request) throws BridgeError {
        MockQualificationParser parser = new MockQualificationParser();
        return parser.parse(request.getQuery(),request.getParameters());
    }
}
