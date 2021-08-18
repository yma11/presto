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
package com.facebook.presto.sql.planner.cider;

import com.facebook.presto.spi.plan.AggregationNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CiderAggregationNode
{
    private AggregationNode aggregationNode;
    private boolean fuseEnabled;
    public CiderAggregationNode(AggregationNode aggregationNode, boolean fuseEnabled)
    {
        this.aggregationNode = aggregationNode;
        this.fuseEnabled = fuseEnabled;
    }
    public String toRAJsonStr(String step)
    {
        if (!fuseEnabled) {
            // if cider only takes agg op without previous ops such as project/filter, we
            // need to make a fake TableScanNode to pass to Cider
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode rootNode = objectMapper.createObjectNode();
            ArrayNode relsNode = objectMapper.createArrayNode();
            // compose fake TableScanNode
            ObjectNode tableScanNode = objectMapper.createObjectNode();
            tableScanNode.put("id", "0");
            tableScanNode.put("relOp", "LogicalTableScan");
            tableScanNode.set("fieldNames",
                    CiderPlanBuilderHelper.getFakeScanFieldNodes(objectMapper, aggregationNode, step));
            tableScanNode.set("table", CiderPlanBuilderHelper.getTableInfoNodes(objectMapper, aggregationNode));
            tableScanNode.set("inputs", objectMapper.createArrayNode());
            // compose AggregationNode
            ObjectNode aggregationJSONNode = objectMapper.createObjectNode();
            // group info
            ArrayNode groupNodes = CiderPlanBuilderHelper.getGroupNodes(objectMapper, aggregationNode);
            // convert aggregations to Cider format
            ArrayNode aggNodes = CiderPlanBuilderHelper.getAggNodes(objectMapper, aggregationNode);
            // feilds info
            ArrayNode fieldNodes = CiderPlanBuilderHelper.getAggFeildNodes(objectMapper, aggregationNode);
            aggregationJSONNode.put("id", "1");
            // repOp node
            aggregationJSONNode.put("relOp", "LogicalAggregate");
            aggregationJSONNode.set("fields", fieldNodes);
            aggregationJSONNode.set("group", groupNodes);
            aggregationJSONNode.set("aggs", aggNodes);
            relsNode.add(tableScanNode);
            relsNode.add(aggregationJSONNode);
            rootNode.set("rels", relsNode);
            return rootNode.toString();
        }
        return "";
    }
}
