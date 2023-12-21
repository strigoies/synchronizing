package com.yisa.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ArangoDBMark{

    private String op;
    private long group;
    private String afterPersonnelIdNumber = "";
    private String afterPersonnelIdType = "";
    private String beforePersonnelIdNumber = "";
    private String beforePersonnelIdType = "";
    private Integer[] afterLabels = new Integer[0];
    private Integer[] beforeLabels = new Integer[0];

    private FaceProfile faceProfile;
}
