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
package com.facebook.presto.cider;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestTpch
        extends AbstractTestQueryFramework
{
    private final String prefix = "q";
    private final String resultSuffix = ".result";
    private final String querySuffix = ".sql";

    private String loadTestQueryStatement(int queryId)
    {
        String filePath = "/" + prefix + String.format("%02d", queryId) + querySuffix;
        List<String> lines = getResourceFileContent(filePath);
        StringBuilder strBuilder = new StringBuilder();
        for (String line : lines) {
            strBuilder.append(line + " ");
        }
        return strBuilder.toString();
    }

    private List<String> loadTestQueryResult(int queryId)
    {
        String filePath = "/" + prefix + String.format("%02d", queryId) + resultSuffix;
        return getResourceFileContent(filePath);
    }

    private List<String> getResourceFileContent(String filePath)
    {
        List<String> lines = null;
        try {
            Path path = Paths.get(getClass().getResource(filePath).toURI());
            lines = Files.readAllLines(path);
            // remove first line which is comment
            lines.remove(0);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return lines;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return TpchQueryRunner.createQueryRunner();
    }

    private void testTpchQuery(int queryId)
    {
        String query = loadTestQueryStatement(queryId);

        MaterializedResult actual = computeActual(query);
        List<String> expected = loadTestQueryResult(queryId);
        // We'd better to compare the real content instead of only row number
        assertEquals(actual.getRowCount(), expected.size());
    }

    @Test
    public void testTpchQuery_01()
    {
        testTpchQuery(1);
    }

    @Test
    public void testTpchQuery_02()
    {
        testTpchQuery(2);
    }

    @Test
    public void testTpchQuery_03()
    {
        testTpchQuery(3);
    }

    @Test
    public void testTpchQuery_04()
    {
        testTpchQuery(4);
    }

    @Test
    public void testTpchQuery_05()
    {
        testTpchQuery(5);
    }

    @Test
    public void testTpchQuery_06()
    {
        testTpchQuery(6);
    }

    @Test
    public void testTpchQuery_07()
    {
        testTpchQuery(7);
    }

    @Test
    public void testTpchQuery_08()
    {
        testTpchQuery(8);
    }

    @Test
    public void testTpchQuery_09()
    {
        testTpchQuery(9);
    }

    @Test
    public void testTpchQuery_10()
    {
        testTpchQuery(10);
    }

    @Test
    public void testTpchQuery_11()
    {
        testTpchQuery(11);
    }

    @Test
    public void testTpchQuery_12()
    {
        testTpchQuery(12);
    }

    @Test
    public void testTpchQuery_13()
    {
        testTpchQuery(13);
    }

    @Test
    public void testTpchQuery_14()
    {
        testTpchQuery(14);
    }

    @Test
    public void testTpchQuery_15()
    {
        // Skip q15 due to connector does not support create view
        // We need modify q15 to pass the test
    }

    @Test
    public void testTpchQuery_16()
    {
        testTpchQuery(16);
    }

    @Test
    public void testTpchQuery_17()
    {
        testTpchQuery(17);
    }

    @Test
    public void testTpchQuery_18()
    {
        testTpchQuery(18);
    }

    @Test
    public void testTpchQuery_19()
    {
        testTpchQuery(19);
    }

    @Test
    public void testTpchQuery_20()
    {
        testTpchQuery(20);
    }

    @Test
    public void testTpchQuery_21()
    {
        testTpchQuery(21);
    }

    @Test
    public void testTpchQuery_22()
    {
        testTpchQuery(22);
    }
}
