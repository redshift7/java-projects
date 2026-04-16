package com.example.flink;

import com.example.flink.streaming.adhocinv.DocumentProcessor;

public class MainClass {
    public static String objPicker;
    public static void main(String[] args) throws Exception
    {
        DocumentProcessor.execute();
    }
}


