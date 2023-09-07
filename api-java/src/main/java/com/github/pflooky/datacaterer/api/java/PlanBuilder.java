package com.github.pflooky.datacaterer.api.java;

import com.github.pflooky.datacaterer.api.java.model.SinkOptionsBuilder;
import com.github.pflooky.datacaterer.api.java.model.TaskSummaryBuilder;
import com.github.pflooky.datacaterer.api.model.ForeignKeyRelation;
import com.github.pflooky.datacaterer.api.model.Plan;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaList;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaOption;
import static com.github.pflooky.datacaterer.api.converter.Converters.toScalaSeq;

public final class PlanBuilder {
    private final com.github.pflooky.datacaterer.api.PlanBuilder scalaDef;

    public PlanBuilder(com.github.pflooky.datacaterer.api.PlanBuilder scalaDef) {
        this.scalaDef = scalaDef;
    }

    public PlanBuilder() {
        this.scalaDef = new com.github.pflooky.datacaterer.api.PlanBuilder(
                new Plan(
                        "Default plan",
                        "Data generation plan",
                        toScalaList(Collections.emptyList()),
                        toScalaOption(Optional.empty()),
                        toScalaList(Collections.emptyList())
                ), toScalaList(Collections.emptyList())
        );
    }

    public com.github.pflooky.datacaterer.api.PlanBuilder plan() {
        return scalaDef;
    }

    public PlanBuilder name(String name) {
        return new PlanBuilder(scalaDef.name(name));
    }

    public PlanBuilder description(String description) {
        return new PlanBuilder(scalaDef.description(description));
    }

    public PlanBuilder taskSummaries(TaskSummaryBuilder... taskSummaryBuilders) {
        return new PlanBuilder(scalaDef.taskSummaries(
                toScalaSeq(Arrays.stream(taskSummaryBuilders).map(TaskSummaryBuilder::taskSummary).collect(Collectors.toList()))
        ));
    }

    public PlanBuilder sinkOptions(SinkOptionsBuilder sinkOptionsBuilder) {
        return new PlanBuilder(scalaDef.sinkOptions(sinkOptionsBuilder.sinkOptions()));
    }

    public PlanBuilder seed(long seed) {
        return new PlanBuilder(scalaDef.seed(seed));
    }

    public PlanBuilder locale(String locale) {
        return new PlanBuilder(scalaDef.locale(locale));
    }

    public PlanBuilder addForeignKeyRelationship(ForeignKeyRelation foreignKey, ForeignKeyRelation relation, ForeignKeyRelation... relations) {
        return new PlanBuilder(scalaDef.addForeignKeyRelationship(foreignKey, relation, toScalaSeq(Arrays.asList(relations))));
    }

    public PlanBuilder addForeignKeyRelationship(ForeignKeyRelation foreignKey, List<ForeignKeyRelation> relations) {
        return new PlanBuilder(scalaDef.addForeignKeyRelationship(foreignKey, toScalaList(relations)));
    }
}
