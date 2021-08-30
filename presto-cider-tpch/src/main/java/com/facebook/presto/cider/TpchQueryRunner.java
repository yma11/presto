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

import com.facebook.presto.Session;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TpchQueryRunner
{
    private TpchQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return createQueryRunner(extraProperties, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        Session session = TestingSession.testSessionBuilder()
                .setSource("test")
                .setCatalog("tpch")
                .setSchema("sf1")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch",
                    ImmutableMap.of(TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name().toUpperCase()));
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        createQueryRunner(ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
    }
}
