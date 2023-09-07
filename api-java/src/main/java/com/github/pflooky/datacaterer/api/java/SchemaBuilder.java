package com.github.pflooky.datacaterer.api.java;

import com.github.pflooky.datacaterer.api.java.model.FieldBuilder;
import com.github.pflooky.datacaterer.api.model.DataType;
import com.github.pflooky.datacaterer.api.model.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public final class SchemaBuilder {
    private final com.github.pflooky.datacaterer.api.SchemaBuilder scalaDef;

    public SchemaBuilder(com.github.pflooky.datacaterer.api.SchemaBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public SchemaBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.SchemaBuilder(new Schema(toScalaOption(Optional.empty())));
    }

    public com.github.pflooky.datacaterer.api.model.Schema schema() {
        return scalaDef.schema();
    }

    public SchemaBuilder addField(String name, DataType type) {
        return new SchemaBuilder(scalaDef.addField(name, type));
    }

    public SchemaBuilder addField(FieldBuilder fieldBuilder) {
        return new SchemaBuilder(scalaDef.addFieldsJava(toScalaSeq(List.of(fieldBuilder.field()))));
    }

    public SchemaBuilder addFields(FieldBuilder... fieldBuilders) {
        return new SchemaBuilder(scalaDef.addFieldsJava(
                toScalaSeq(Arrays.stream(fieldBuilders).map(FieldBuilder::field).collect(Collectors.toList()))
        ));
    }
}
