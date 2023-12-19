package com.yisa.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ArangoDBMark extends FaceProfile{

    private String op;
    private long group;
    private String personnelIdNumber;
    private String personnelIdType;
    private Integer[] afterLabels = new Integer[0];
    private Integer[] beforeLabels = new Integer[0];
}
