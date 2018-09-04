package com.alberto.playground.splitter;

import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class KeyVal implements Serializable {
    @SerializedName("key")
    private String key;
    @SerializedName("value")
    private Integer value;

    public KeyVal(final String key, final Integer value) {
        this.key = key;
        this.value = value;
    }

    public KeyVal() {
        this.key = null;
        this.value = null;
    }

    public String key() {
        return key;
    }

    public int value() {
        return value;
    }
}
