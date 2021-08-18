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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CiderPlanBuilderHelper
{
    private CiderPlanBuilderHelper() {}

    public static String getCiderAggOp(String name)
    {
        switch (name) {
            case "count":
                return "COUNT";
            case "sum":
                return "SUM";
        }
        return "";
    }

    public static Boolean getCiderAggOpNullable(String name)
    {
        switch (name) {
            case "count":
                return false;
            case "sum":
                return true;
        }
        return false;
    }
    /**
     * get column property from json string of TableScanNode
     * @param planNode Current visiting Node
     * @param columnName  name of column
     * @return column index
     */
    public static int getColumnIndex(ObjectMapper objectMapper, PlanNode planNode, String columnName)
    {
        // find tableScanNode to get column info
        while (!(planNode instanceof TableScanNode)) {
            planNode = planNode.getSources().get(0);
        }
        TableScanNode tableScanNode = (TableScanNode) planNode;
        List<VariableReferenceExpression> tableOutputColumns = tableScanNode.getOutputVariables();
        for (VariableReferenceExpression expression : tableOutputColumns) {
            if (expression.getName().equals(columnName)) {
                return tableOutputColumns.indexOf(expression);
            }
        }
        return -1;
    }

    public static ArrayNode getGroupNodes(ObjectMapper objectMapper, AggregationNode planNode)
    {
        ArrayNode groupKeyNodes = objectMapper.createArrayNode();
        List<VariableReferenceExpression> groupKeys = planNode.getGroupingKeys();
        // no group key
        if (groupKeys.size() > 0) {
            // group keys: column name
            for (VariableReferenceExpression expression : groupKeys) {
                Integer columnIndex = getColumnIndex(objectMapper, planNode, expression.getName());
                groupKeyNodes.add(columnIndex);
            }
        }
        return groupKeyNodes;
    }
    public static ArrayNode getAggNodes(ObjectMapper objectMapper, AggregationNode planNode)
    {
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = planNode.getAggregations();
        ArrayNode aggs = objectMapper.createArrayNode();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> agg : aggregations.entrySet()) {
            ObjectNode aggNode = objectMapper.createObjectNode();
            aggNode.put("agg", getCiderAggOp(agg.getValue().getCall().getDisplayName()));
            ObjectNode typeNode = objectMapper.createObjectNode();
            typeNode.put("type", agg.getKey().getType().getTypeSignature().toString().toUpperCase());
            typeNode.put("nullable", getCiderAggOpNullable(agg.getValue().getCall().getDisplayName()));
            aggNode.set("type", typeNode);
            aggNode.put("distinct", agg.getValue().isDistinct());
            // "operands": [
            //   0  --> (arguments -> columnName -> columnIndex)
            // ]
            ArrayNode operandsNode = objectMapper.createArrayNode();
            if (agg.getValue().getArguments().size() != 0) {
                for (RowExpression expression : agg.getValue().getArguments()) {
                    if (expression instanceof VariableReferenceExpression) {
                        String columnName = ((VariableReferenceExpression) expression).getName();
                        int columnIndex = Integer.valueOf(
                                getColumnIndex(objectMapper, planNode, columnName));
                        operandsNode.add(columnIndex);
                    }
                }
            }
            aggNode.set("operands", operandsNode);
            aggs.add(aggNode);
        }
        return aggs;
    }

    public static ArrayNode getAggFeildNodes(ObjectMapper objectMapper, AggregationNode planNode)
    {
        ArrayNode feildsNode = objectMapper.createArrayNode();
        for (int i = 0; i < planNode.getAggregations().size(); i++) {
            feildsNode.add("EXPR$" + i);
        }
        return feildsNode;
    }

    // get relNode id
    public static int getNodeId(PlanNode planNode)
    {
        int i = 0;
        while (!(planNode instanceof TableScanNode)) {
            planNode = planNode.getSources().get(0);
            i++;
        }
        return i;
    }

    // get table schemas, such as
    public static ArrayNode getTableInfoNodes(ObjectMapper objectMapper, PlanNode planNode)
    {
        ArrayNode tableInfoNodes = objectMapper.createArrayNode();
        // find TableScanNode where stores table info
        while (!(planNode instanceof TableScanNode)) {
            planNode = planNode.getSources().get(0);
        }
        TableScanNode tableScanNode = (TableScanNode) planNode;
        try {
            String json = objectMapper.writeValueAsString(tableScanNode);
            // for HiveTableHandle only
            Map map = objectMapper.readValue(json, Map.class);
            LinkedHashMap table = (LinkedHashMap) map.get("table");
            LinkedHashMap connectorHandle = (LinkedHashMap) table.get("connectorHandle");
            tableInfoNodes.add(String.valueOf(connectorHandle.get("schemaName")));
            tableInfoNodes.add(String.valueOf(connectorHandle.get("tableName")));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return tableInfoNodes;
    }
    // get output variables of parent node(FilterNode/TableScanNode), which are input of current agg node
    public static ArrayNode getFakeScanFieldNodes(
            ObjectMapper objectMapper, AggregationNode planNode, String step)
    {
        ArrayNode variableNodes = objectMapper.createArrayNode();
        List<VariableReferenceExpression> inputVariables = planNode.getSource().getOutputVariables();
        for (VariableReferenceExpression inputVariable : inputVariables) {
            switch (step) {
                case "partial":
                    variableNodes.add(inputVariable.getName());
                    break;
                case "final":
                    variableNodes.add("EXPR$" + inputVariables.indexOf(inputVariable));
                    break;
            }
        }
        return variableNodes;
    }
}
