package com.kineticdata.bridgehub.adapter.mock;

import com.kineticdata.bridgehub.adapter.QualificationParser;

/**
 *
 */
public class MockQualificationParser extends QualificationParser {
    @Override
    public String encodeParameter(String name, String value) {
        return value;
    }
}