package com.kadal.response;

public enum ResponseEnum {
    SUCCESSFUL_MESSAGE("İşlem Başarılı"),
    ERROR_MESSAGE("İşlem Başarısız"),
    STATUS_TRUE("True"),
    STATUS_FALSE("False");

    private String response;

    ResponseEnum(String response) {
        this.response=response;
    }

    public String getResponse() {
        return response;
    }
}

