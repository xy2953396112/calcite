/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.List;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class LogicalCorrelateMaterializationTest extends SqlToRelTestBase {

  private static final RelBuilder builder = RelBuilder.create(config().build());

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mv0", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("empno", SqlTypeName.INTEGER)
            .add("num", SqlTypeName.INTEGER)
            .build();
      }
    });
    rootSchema.add("mv1", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("empno", SqlTypeName.INTEGER)
            .add("ename", SqlTypeName.VARCHAR)
            .add("num", SqlTypeName.INTEGER)
            .build();
      }
    });
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  void testLateraltableFilter() {
    RelNode mv = tester.convertSqlToRel(
        "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)").rel;

    RelNode replacement = builder.scan("mv0").build();

    RelNode query = tester.convertSqlToRel(""
        + "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num) where empno > 0").rel;

    RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv0"));
    List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations.useMaterializedViews(
        query, ImmutableList.of(relOptMaterialization));

    String mvOptimized = ""
        + "LogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "  LogicalTableScan(table=[[mv0]])\n";
    assertThat(mvOptimized, equalTo(RelOptUtil.toString(relOptimized.get(0).left)));
  }

  @Test
  void testLateraltableProject() {
    RelNode mv = tester.convertSqlToRel(
        "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)").rel;

    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode replacement = builder.scan("mv1").build();

    RelNode query = tester.convertSqlToRel(
        "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)").rel;

    RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));

    List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    System.out.println(RelOptUtil.toString(relOptimized.get(0).left));
  }

  @Test
  void testLateraltableProjectFilter() {
    RelNode mv = tester.convertSqlToRel(
        "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)").rel;

    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode replacement = builder.scan("mv1").build();

    RelNode query = tester.convertSqlToRel(
        "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num) where empno > 0").rel;

    RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));

    List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    System.out.println(RelOptUtil.toString(relOptimized.get(0).left));
  }

  @Test
  void testLateraltableProjectFilterJoin() {
    RelNode mv = tester.convertSqlToRel(
        "select empno, ename, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) on "
            + "emp.deptno = num").rel;

    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode replacement = builder.scan("mv1").build();

    RelNode query = tester.convertSqlToRel(
        "select empno, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) on emp"
            + ".deptno = num where empno > 0 ").rel;

    RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));

    List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    System.out.println(RelOptUtil.toString(relOptimized.get(0).left));
  }

  @Test
  void testLateraltableProjectFilterRename() {
    RelNode mv = tester.convertSqlToRel(
        "select empno, ename, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) on "
            + "emp.deptno = num").rel;

    final RelBuilder builder = RelBuilder.create(config().build());
    RelNode replacement = builder.scan("mv1").build();

    RelNode query = tester.convertSqlToRel(
        "select empno, r.deptno1 from emp join lateral table(ramp(emp.deptno)) as r(deptno1) on emp"
            + ".deptno = r.deptno1 where empno > 0 ").rel;

    RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));

    List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    System.out.println(RelOptUtil.toString(relOptimized.get(0).left));
  }
}
