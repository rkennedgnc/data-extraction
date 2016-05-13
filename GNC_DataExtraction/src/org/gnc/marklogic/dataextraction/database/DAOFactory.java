/*
 * Copyright (c)2016 General Networks Corporation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 * The use of the Apache License does not indicate that this project is 
 * affiliated with the Apache Software Foundation. 
 */

package org.gnc.marklogic.dataextraction.database;

import java.sql.SQLException;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 */
public class DAOFactory {

    private DAOFactory() {
    }

    public static DAOFactory getInstance() {
        return DAOFactoryHolder.INSTANCE;
    }

    private static class DAOFactoryHolder {

        private static final DAOFactory INSTANCE = new DAOFactory();
    }

    public DAO getDAO(String db) throws SQLException {
        switch (db) {
            case "ORACLE":
                return new ORACLE_DAO();
            case "MSSQL":
                return new MSSQL_DAO();
            default:
                throw new SQLException("DB type not supported");
        }
    }
}
