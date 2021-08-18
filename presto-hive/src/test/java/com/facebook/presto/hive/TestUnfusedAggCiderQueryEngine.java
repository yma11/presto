/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.cider.CiderAggregationNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

@Test
public class TestUnfusedAggCiderQueryEngine
        extends AbstractTestQueryFramework
{
    private String aggJson = "{\"rels\":[" +
            "{\"id\":\"0\"," +
            "\"relOp\":\"LogicalTableScan\"," +
            "\"fieldNames\":[\"orderkey\",\"totalprice\"]," +
            "\"table\":[\"tpch\",\"orders\"]," +
            "\"inputs\":[]}," +
            "{\"id\":\"1\"," +
            "\"relOp\":\"LogicalAggregate\"," +
            "\"fields\":[\"EXPR$0\",\"EXPR$1\"]," +
            "\"group\":[]," +
            "\"aggs\":[{\"agg\":\"COUNT\",\"type\":{\"type\":\"BIGINT\",\"nullable\":false},\"distinct\":false,\"operands\":[0]}," +
            "{\"agg\":\"SUM\",\"type\":{\"type\":\"DOUBLE\",\"nullable\":true},\"distinct\":false,\"operands\":[1]}]}]}";
    private String finalAggJson = "{\"rels\":[" +
            "{\"id\":\"0\"," +
            "\"relOp\":\"LogicalTableScan\"," +
            "\"fieldNames\":[\"EXPR$0\",\"EXPR$1\"]," +
            "\"table\":[\"tpch\",\"orders\"]," +
            "\"inputs\":[]}," +
            "{\"id\":\"1\"," +
            "\"relOp\":\"LogicalAggregate\"," +
            "\"fields\":[\"EXPR$0\",\"EXPR$1\"]," +
            "\"group\":[]," +
            "\"aggs\":[{\"agg\":\"COUNT\",\"type\":{\"type\":\"BIGINT\",\"nullable\":false},\"distinct\":false,\"operands\":[0]}," +
            "{\"agg\":\"SUM\",\"type\":{\"type\":\"DOUBLE\",\"nullable\":true},\"distinct\":false,\"operands\":[1]}]}]}";
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @Test
    public void testUnfusedAggCider()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
        SubPlan subPlan = subplan("SELECT sum(totalprice), count(orderkey) FROM orders", session);
        List<PlanFragment> planAllFragments = subPlan.getAllFragments();
        for (PlanFragment planFragment : planAllFragments) {
            PlanNode planNode = planFragment.getRoot();
            if (planNode instanceof AggregationNode) {
                AggregationNode aggregationNode = (AggregationNode) planNode;
                if (aggregationNode.getStep() == AggregationNode.Step.PARTIAL) {
                    CiderAggregationNode ciderAggregationNode = new CiderAggregationNode(aggregationNode, false);
                    String raJson = ciderAggregationNode.toRAJsonStr("partial");
                    assertEquals(aggJson, raJson);
                }
            }
        }
    }

    // FilterNode should not affect agg node in unfused scenario
    @Test
    public void testUnfusedAggWithFilterCider()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
        SubPlan subPlan = subplan("SELECT sum(totalprice), count(orderkey) FROM orders where totalprice > 123.4567", session);
        List<PlanFragment> planAllFragments = subPlan.getAllFragments();
        for (PlanFragment planFragment : planAllFragments) {
            PlanNode planNode = planFragment.getRoot();
            if (planNode instanceof AggregationNode) {
                AggregationNode aggregationNode = (AggregationNode) planNode;
                if (aggregationNode.getStep() == AggregationNode.Step.PARTIAL) {
                    CiderAggregationNode ciderAggregationNode = new CiderAggregationNode(aggregationNode, false);
                    String raJson = ciderAggregationNode.toRAJsonStr("partial");
                    assertEquals(aggJson, raJson);
                }
            }
        }
    }

    @Test
    public void testFinalUnfusedAggWithFilterCider()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
        SubPlan subPlan = subplan("SELECT count(orderkey), sum(totalprice) FROM orders where totalprice > 123.4567", session);
        List<PlanFragment> planAllFragments = subPlan.getAllFragments();
        for (PlanFragment planFragment : planAllFragments) {
            PlanNode planNode = planFragment.getRoot();
            if (planNode instanceof AggregationNode) {
                AggregationNode aggregationNode = (AggregationNode) planNode;
                if (aggregationNode.getStep() == AggregationNode.Step.PARTIAL) {
                    CiderAggregationNode ciderAggregationNode = new CiderAggregationNode(aggregationNode, false);
                    String raJson = ciderAggregationNode.toRAJsonStr("final");
                    assertEquals(finalAggJson, raJson);
                }
            }
        }
    }
}
