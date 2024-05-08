package io.oferto.pocminios3select.model;

import com.opencsv.bean.CsvBindByPosition;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Expression {
	@CsvBindByPosition(position = 0,required = true)
    private String sampleId;
	@CsvBindByPosition(position = 1,required = true)
    private float expression;
}
