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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 */
public class ORACLE_DAO extends DAO {

    Connection conn;

    @Override
    public Connection getConnection() {
        return conn;
    }

    @Override
    public void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(ORACLE_DAO.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public void openConnection(String url, String username, String password) {
        try {
            if (conn == null) {
                Class.forName("oracle.jdbc.OracleDriver").newInstance();
                conn = DriverManager.getConnection(url, username, password);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            Logger.getLogger(ORACLE_DAO.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    // Not Implemented - Needed for MS SQL windows authentication
    public void openConnection(String url) {

    }

}
