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
import com.facebook.presto.spi.cider.CiderPlanBuilder;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static io.airlift.tpch.TpchTable.getTables;

public class TestCiderPlanNodes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @Test
    public void testFilterNodeToCiderFilterNode()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "false")
                .build();
        // , orderkey, totalprice
        SubPlan subPlan = subplan("SELECT totalprice, orderkey%2, custkey%2 FROM orders where totalprice/2 > 123.4567", session);
        List<PlanFragment> planAllFragments = subPlan.getAllFragments();
        for (PlanFragment planFragment : planAllFragments) {
            PlanNode planNode = planFragment.getRoot();
            if (planNode instanceof ProjectNode) {
                String json = CiderPlanBuilder.toCiderPlanNode(planNode).toRAJson();
            }
        }
    }
}
