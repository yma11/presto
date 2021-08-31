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
package com.facebook.presto.spi.cider;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapd.plan.CiderCallExpression;
import com.mapd.plan.CiderConstantExpression;
import com.mapd.plan.CiderExpression;
import com.mapd.plan.CiderFilterNode;
import com.mapd.plan.CiderOperatorNode;
import com.mapd.plan.CiderProjectNode;
import com.mapd.plan.CiderSpecialFormCondition;
import com.mapd.plan.CiderTableScanNode;
import com.mapd.plan.CiderVariableReferenceExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A builder utils to bridge Presto Plan and Cider Plan (e.g., Json RA)
 */
public class CiderPlanBuilder
{
    private CiderPlanBuilder() {}

    private static CiderProjectNode toCiderPlanNode(ProjectNode node)
    {
        HashMap<String, Integer> columnInfo = getColumnInfoFromSourceNode(node);
        Map<VariableReferenceExpression, RowExpression> assignments = node.getAssignments().getMap();
        HashMap<CiderVariableReferenceExpression, CiderExpression> ciderAssignments = new HashMap<>();
        for (VariableReferenceExpression field: assignments.keySet()) {
            // will include projects like expr_1 -> MOD(custkey,2), for them, columnIndex will not used and set it to -1
            int columnIndex = -1;
            if (columnInfo.containsKey(field.getName())) {
                columnIndex = columnInfo.get(field.getName());
            }
            CiderVariableReferenceExpression ciderField = new CiderVariableReferenceExpression(field.getName(),
                    field.getType().toString(), columnIndex);
            CiderExpression ciderExpression = toCiderExpression(assignments.get(field), columnInfo);
            ciderAssignments.put(ciderField, ciderExpression);
        }
        CiderProjectNode ciderProjectNode = new CiderProjectNode(ciderAssignments);
        ciderProjectNode.registerChildren(toCiderPlanNode(node.getSource()));
        return ciderProjectNode;
    }

    private static HashMap<String, Integer> getColumnInfoFromSourceNode(PlanNode node)
    {
        if (node instanceof ProjectNode || node instanceof FilterNode) {
            List<VariableReferenceExpression> outputVariables = node.getSources().get(0).getOutputVariables();
            HashMap<String, Integer> columnInfo = new HashMap<>();
            int i = 0;
            for (VariableReferenceExpression outputVariable : outputVariables) {
                columnInfo.put(outputVariable.getName(), i++);
            }
            return columnInfo;
        }
        else {
            throw new UnsupportedOperationException("Currently only get ColumnInfo from Project/FilterNode");
        }
    }

    private static CiderFilterNode toCiderPlanNode(FilterNode node)
    {
        RowExpression expression = node.getPredicate();
        // get columnInfo
        CiderFilterNode ciderFilterNode =
                new CiderFilterNode(toCiderExpression(expression, getColumnInfoFromSourceNode(node)));
        ciderFilterNode.registerChildren(toCiderPlanNode(node.getSource()));
        return ciderFilterNode;
    }

    private static CiderExpression toCiderExpression(RowExpression expression, HashMap<String, Integer> columnInfo)
    {
        if (expression instanceof SpecialFormExpression) {
            SpecialFormExpression specialFormExpression = (SpecialFormExpression) expression;
            String form = specialFormExpression.getForm().toString();
            String type = specialFormExpression.getType().toString();
            List<RowExpression> rowExpressionList = specialFormExpression.getArguments();
            List<CiderExpression> ciderConditionList = new ArrayList<>();
            for (RowExpression rowExpression : rowExpressionList) {
                CiderExpression ciderCondition = toCiderExpression(rowExpression, columnInfo);
                ciderConditionList.add(ciderCondition);
            }
            CiderSpecialFormCondition specialFormCondition =
                    new CiderSpecialFormCondition(CiderSpecialFormCondition.Form.valueOf(form), type, ciderConditionList);
            return specialFormCondition;
        }
        else if (expression instanceof CallExpression) {
            String op = getCiderOp(((CallExpression) expression).getDisplayName());
            String expressionType = expression.getType().toString();
            List<RowExpression> arguments = ((CallExpression) expression).getArguments();
            List<CiderExpression> ciderArguments = new ArrayList<>();
            for (RowExpression argument : arguments) {
                if (argument instanceof ConstantExpression) {
                    String type = argument.getType().toString();
                    String value = ((ConstantExpression) argument).getValue().toString();
                    CiderConstantExpression ciderConstantExpression = new CiderConstantExpression(type, value);
                    ciderArguments.add(ciderConstantExpression);
                }
                else if (argument instanceof VariableReferenceExpression) {
                    String type = argument.getType().toString();
                    String name = ((VariableReferenceExpression) argument).getName();
                    CiderVariableReferenceExpression ciderVariableReferenceExpression =
                            new CiderVariableReferenceExpression(name, type, columnInfo.get(name));
                    ciderArguments.add(ciderVariableReferenceExpression);
                }
                else if (argument instanceof CallExpression) {
                    // actually this is not a filterCondition but has same structure, like DIVIDE(totalprice, 2.0)
                    CiderCallExpression ciderCallExpression = (CiderCallExpression) toCiderExpression(argument, columnInfo);
                    ciderArguments.add(ciderCallExpression);
                }
                else {
                    throw new UnsupportedOperationException("unsupported expression in condition");
                }
            }
            CiderCallExpression ciderFilterCondition = new CiderCallExpression(op, expressionType, ciderArguments);
            return ciderFilterCondition;
        }
        else if (expression instanceof VariableReferenceExpression) {
            VariableReferenceExpression vExpression = (VariableReferenceExpression) expression;
            CiderVariableReferenceExpression ciderVariableReferenceExpression =
                    new CiderVariableReferenceExpression(vExpression.getName(), vExpression.getType().toString(), columnInfo.get(vExpression.getName()));
            return ciderVariableReferenceExpression;
        }
        else {
            throw new UnsupportedOperationException("Unsupported condition transformation form presto to Omnisci");
        }
    }

    private static CiderTableScanNode toCiderPlanNode(TableScanNode node)
    {
        try {
            List<String> columns = new ArrayList<>();
            for (VariableReferenceExpression column : node.getOutputVariables()) {
                columns.add(column.getName());
            }
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(node);
            // for HiveTableHandle only
            Map map = objectMapper.readValue(json, Map.class);
            LinkedHashMap connectorHandle = (LinkedHashMap) ((LinkedHashMap) map.get("table")).get("connectorHandle");
            return new CiderTableScanNode(String.valueOf(connectorHandle.get("schemaName")),
                            String.valueOf(connectorHandle.get("tableName")), columns);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static CiderOperatorNode toCiderPlanNode(PlanNode planNode)
    {
        if (planNode instanceof ProjectNode) {
            return toCiderPlanNode((ProjectNode) planNode);
        }
        else if (planNode instanceof FilterNode) {
            return toCiderPlanNode((FilterNode) planNode);
        }
        else if (planNode instanceof TableScanNode) {
            return toCiderPlanNode((TableScanNode) planNode);
        }
        //FIXME
        throw new RuntimeException("Unsupported operator");
    }

    public static Map<VariableReferenceExpression, ColumnHandle> getColumnHandle(
            PlanNode node)
    {
        return getSourceNode(node).getAssignments();
    }

    public static ConnectorTableHandle getTableHandle(PlanNode node)
    {
        return getSourceNode(node).getTable().getConnectorHandle();
    }

    private static TableScanNode getSourceNode(
            PlanNode node)
    {
        List<PlanNode> p = node.getSources();
        // Proceed to source node
        while (p.size() == 1) {
            node = node.getSources().get(0);
            p = node.getSources();
        }
        if (p.size() != 0 || !(node instanceof TableScanNode)) {
            //FIXME
            throw new RuntimeException("Unable to resolve variable since multi-source found!");
        }
        return ((TableScanNode) node);
    }

    private static String getCiderOp(String displayName)
    {
        switch (displayName) {
            case "MODULUS":
                return "MOD";
            case "DIVIDE":
                return "\\/";
        }
        return "";
    }
}
